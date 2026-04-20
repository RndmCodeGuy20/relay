package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const advisoryLockKey int64 = 987654321 // arbitrary constant

func acquireLock(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	return nil
}

func releaseLock(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	return nil
}
