package rule

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	ListEnabled(ctx context.Context) ([]Rule, error)
	ListEnabledBySourceEventType(ctx context.Context, source string, eventType string) ([]Rule, error)
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) ListEnabled(ctx context.Context) ([]Rule, error) {
	query := `
		SELECT id, name, source, event_type, enabled, priority, version, selector, targets, created_at, updated_at
		FROM rules
		WHERE enabled = TRUE
		ORDER BY priority DESC, id ASC
	`
	return r.queryRules(ctx, query)
}

func (r *PostgresRepository) ListEnabledBySourceEventType(ctx context.Context, source string, eventType string) ([]Rule, error) {
	query := `
		SELECT id, name, source, event_type, enabled, priority, version, selector, targets, created_at, updated_at
		FROM rules
		WHERE enabled = TRUE AND source = $1 AND event_type = $2
		ORDER BY priority DESC, id ASC
	`
	return r.queryRules(ctx, query, source, eventType)
}

func (r *PostgresRepository) queryRules(ctx context.Context, query string, args ...any) ([]Rule, error) {
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rules := make([]Rule, 0)
	for rows.Next() {
		var (
			rule          Rule
			selectorBytes []byte
			targetsBytes  []byte
		)

		err := rows.Scan(
			&rule.ID,
			&rule.Name,
			&rule.Source,
			&rule.EventType,
			&rule.Enabled,
			&rule.Priority,
			&rule.Version,
			&selectorBytes,
			&targetsBytes,
			&rule.CreatedAt,
			&rule.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(selectorBytes, &rule.Selector); err != nil {
			return nil, fmt.Errorf("unmarshal selector for rule %s: %w", rule.ID, err)
		}
		if err := json.Unmarshal(targetsBytes, &rule.Targets); err != nil {
			return nil, fmt.Errorf("unmarshal targets for rule %s: %w", rule.ID, err)
		}

		rules = append(rules, rule)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return rules, nil
}
