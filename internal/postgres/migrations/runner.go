package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Runner struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

func NewRunner(pool *pgxpool.Pool, logger *zap.Logger) *Runner {
	return &Runner{pool: pool, logger: logger}
}

func (r *Runner) ensureTable(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	return err
}

func (r *Runner) applied(ctx context.Context) (map[int64]bool, error) {
	rows, err := r.pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := map[int64]bool{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		res[v] = true
	}

	return res, nil
}

func (r *Runner) Up(ctx context.Context, migrations []Migration) error {
	if err := acquireLock(ctx, r.pool); err != nil {
		return err
	}
	defer releaseLock(ctx, r.pool)

	if err := r.ensureTable(ctx); err != nil {
		return err
	}

	applied, err := r.applied(ctx)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}

		if r.logger != nil {
			r.logger.Info("applying migration",
				zap.Int64("version", m.Version),
				zap.String("name", m.Name))
		}

		if err := r.apply(ctx, m); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) Down(ctx context.Context, migrations []Migration, steps int) error {
	if err := acquireLock(ctx, r.pool); err != nil {
		return err
	}
	defer releaseLock(ctx, r.pool)

	rows, err := r.pool.Query(ctx,
		`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT $1`, steps)
	if err != nil {
		return err
	}
	defer rows.Close()

	var versions []int64
	for rows.Next() {
		var v int64
		err := rows.Scan(&v)
		if err != nil {
			return err
		}
		versions = append(versions, v)
	}

	mMap := map[int64]Migration{}
	for _, m := range migrations {
		mMap[m.Version] = m
	}

	for _, v := range versions {
		m := mMap[v]

		if m.DownSQL == "" {
			return fmt.Errorf("no down migration for %d", v)
		}

		if r.logger != nil {
			r.logger.Info("rolling back", zap.Int64("version", m.Version), zap.String("name", m.Name))
		}

		if err := r.rollback(ctx, m); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) apply(ctx context.Context, m Migration) error {
	sql := m.UpSQL

	// Detect CONCURRENTLY → must run outside tx
	if strings.Contains(strings.ToUpper(sql), "CONCURRENTLY") {
		if _, err := r.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("exec (non-tx): %w", err)
		}
	} else {
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, sql); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
			m.Version, m.Name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}

		return tx.Commit(ctx)
	}

	// record separately if non-tx
	_, err := r.pool.Exec(ctx,
		`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
		m.Version, m.Name,
	)
	return err
}

func (r *Runner) rollback(ctx context.Context, m Migration) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, m.DownSQL); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM schema_migrations WHERE version = $1`, m.Version,
	); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}

	return tx.Commit(ctx)
}
