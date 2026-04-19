//go:build integration

package ingestion_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"rndmcodeguy.in/relay/internal/postgres/migrations"
)

func openIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")

	if dbHost == "" || dbPort == "" || dbName == "" || dbUser == "" || dbPassword == "" {
		t.Skip("integration test skipped: one or more database environment variables are not set")
	}

	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pgx pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test database: %v", err)
	}

	applyMigrations(t, pool)

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	migrationsDir := findMigrationsDir(t)
	migs, err := migrations.LoadFromDir(migrationsDir)
	if err != nil {
		t.Fatalf("load migrations from %s: %v", migrationsDir, err)
	}

	runner := migrations.NewRunner(pool, nil)
	if err := runner.Up(context.Background(), migs); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

func resetOutboxTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, "TRUNCATE TABLE dispatch_tasks, outbox_events RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate outbox tables: %v", err)
	}
}

func findMigrationsDir(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	cur := wd
	for {
		candidate := filepath.Join(cur, "migrations")
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return candidate
		}

		parent := filepath.Dir(cur)
		if parent == cur {
			t.Fatalf("could not locate migrations directory from %s", wd)
		}
		cur = parent
	}
}
