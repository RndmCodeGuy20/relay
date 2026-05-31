//go:build integration

package worker_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"rndmcodeguy.in/relay/internal/postgres/migrations"
)

var (
	testPool  *pgxpool.Pool
	setupOnce sync.Once
	setupErr  error
)

func openIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	setupOnce.Do(func() {
		testPool, setupErr = setupTestDB()
	})

	if setupErr != nil {
		t.Fatalf("test DB setup failed: %v", setupErr)
	}

	t.Cleanup(func() {
		// pool is shared across tests; do NOT close per test
	})

	return testPool
}

func setupTestDB() (*pgxpool.Pool, error) {
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")

	if dbHost == "" || dbPort == "" || dbName == "" || dbUser == "" || dbPassword == "" {
		return nil, fmt.Errorf("missing DB env vars")
	}

	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s",
		dbUser, dbPassword, dbHost, dbPort, dbName)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping DB: %w", err)
	}

	if err := applyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	migrationsDir, err := findMigrationsDir()
	if err != nil {
		return err
	}

	migs, err := migrations.LoadFromDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	runner := migrations.NewRunner(pool, nil)

	if err := runner.Up(ctx, migs); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}

// resetDispatchTasks wipes dispatch_tasks (and outbox_events because of the
// FK) so each test starts from a clean slate.
func resetDispatchTasks(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, `
		TRUNCATE TABLE dispatch_tasks, outbox_events RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func findMigrationsDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get wd: %w", err)
	}

	cur := wd
	for {
		candidate := filepath.Join(cur, "migrations")
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return candidate, nil
		}

		parent := filepath.Dir(cur)
		if parent == cur {
			return "", fmt.Errorf("migrations dir not found from %s", wd)
		}
		cur = parent
	}
}

// waitUntil polls check at ~20ms intervals until it returns true or the
// timeout elapses. Fails the test on timeout with msg.
func waitUntil(t *testing.T, timeout time.Duration, msg string, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waitUntil timed out after %s: %s", timeout, msg)
}
