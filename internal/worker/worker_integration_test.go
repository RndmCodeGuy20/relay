//go:build integration

package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"rndmcodeguy.in/relay/internal/stream"
	"rndmcodeguy.in/relay/internal/worker"
)

// insertOutbox inserts a minimal parent outbox_events row and returns its id.
func insertOutbox(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()

	id := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events
			(id, event_id, event_type, source, schema_version, payload)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, id, uuid.New(), "test.event", "test-source", "v1", []byte(`{"k":"v"}`))
	if err != nil {
		t.Fatalf("insert outbox: %v", err)
	}
	return id
}

// insertPendingTask inserts a dispatch_tasks row in pending status visible now.
func insertPendingTask(t *testing.T, pool *pgxpool.Pool, outboxID uuid.UUID, target string) uuid.UUID {
	t.Helper()

	id := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, `
		INSERT INTO dispatch_tasks
			(id, event_id, source, event_type, producer_event_id, payload, target, status, visible_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', NOW())
	`, id, outboxID, "test-source", "test.event", uuid.New(), []byte(`{"k":"v"}`), target)
	if err != nil {
		t.Fatalf("insert dispatch_tasks: %v", err)
	}
	return id
}

type taskRow struct {
	Status         string
	Attempts       int
	LastError      *string
	VisibleAt      time.Time
	ProcessedAt    *time.Time
	ClaimedBy      *string
	LeaseExpiresAt *time.Time
	DeadAt         *time.Time
	DeadReasonCode *string
	DeadErrorText  *string
	DeadContext    []byte
	DeadRetryCount int
}

func loadTask(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) taskRow {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var r taskRow
	err := pool.QueryRow(ctx, `
		SELECT status, attempts, last_error, visible_at, processed_at,
		       claimed_by, lease_expires_at,
		       dead_at, dead_reason_code, dead_error_text, dead_context, dead_retry_count
		FROM dispatch_tasks
		WHERE id = $1
	`, id).Scan(
		&r.Status, &r.Attempts, &r.LastError, &r.VisibleAt, &r.ProcessedAt,
		&r.ClaimedBy, &r.LeaseExpiresAt,
		&r.DeadAt, &r.DeadReasonCode, &r.DeadErrorText, &r.DeadContext, &r.DeadRetryCount,
	)
	if err != nil {
		t.Fatalf("load task: %v", err)
	}
	return r
}

func taskExists(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM dispatch_tasks WHERE id = $1)`, id,
	).Scan(&exists); err != nil {
		t.Fatalf("exists check: %v", err)
	}
	return exists
}

// startWorker spins the worker in a goroutine and returns a stop func that
// cancels the context and waits for Start to return.
func startWorker(t *testing.T, w *worker.Worker) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = w.Start(ctx)
	}()

	return func() {
		cancel()
		wg.Wait()
	}
}

func baseConfig() worker.Config {
	return worker.Config{
		Workers:           1,
		ClaimBatchSize:    4,
		ClaimInterval:     50 * time.Millisecond,
		VisibilityTimeout: 10 * time.Second,
		ReaperInterval:    10 * time.Second,
		MaxRetries:        3,
		Backoff:           worker.BackoffConfig{Base: 50 * time.Millisecond, Max: 1 * time.Second},
		PublishTimeout:    2 * time.Second,
		DoneRetention:     0,
		RetentionInterval: 10 * time.Second,
	}
}

func TestWorker_HappyPath_MarksDone(t *testing.T) {
	pool := openIntegrationPool(t)
	resetDispatchTasks(t, pool)

	outboxID := insertOutbox(t, pool)
	taskID := insertPendingTask(t, pool, outboxID, "ext.test.target")

	mock := stream.NewMockStream()
	w := worker.New(mock, pool, baseConfig())

	stop := startWorker(t, w)
	defer stop()

	waitUntil(t, 3*time.Second, "task did not reach done", func() bool {
		return loadTask(t, pool, taskID).Status == "done"
	})

	r := loadTask(t, pool, taskID)
	if r.ProcessedAt == nil {
		t.Fatalf("processed_at not set")
	}
	if r.ClaimedBy != nil {
		t.Fatalf("claimed_by not cleared: %v", *r.ClaimedBy)
	}
	if r.LeaseExpiresAt != nil {
		t.Fatalf("lease_expires_at not cleared")
	}
	if r.LastError != nil {
		t.Fatalf("last_error should be null, got %q", *r.LastError)
	}

	pubs := mock.PublishedToSubjects()
	if len(pubs) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(pubs))
	}
	if pubs[0].Subject != "ext.test.target" {
		t.Fatalf("unexpected subject %q", pubs[0].Subject)
	}
	if pubs[0].DedupKey != taskID.String() {
		t.Fatalf("dedupKey = %q want %q", pubs[0].DedupKey, taskID.String())
	}
}

func TestWorker_PublishFailure_RetriesWithBackoff(t *testing.T) {
	pool := openIntegrationPool(t)
	resetDispatchTasks(t, pool)

	outboxID := insertOutbox(t, pool)
	taskID := insertPendingTask(t, pool, outboxID, "ext.fail.target")

	mock := stream.NewMockStream()
	mock.SetError(errors.New("boom"))

	cfg := baseConfig()
	cfg.MaxRetries = 3
	cfg.Backoff = worker.BackoffConfig{Base: 50 * time.Millisecond, Max: 1 * time.Second}
	// Make backoff long enough that we don't bounce into a 2nd attempt before assertion.
	// Base=50ms, Max=1s, attempts=0 → ~25..50ms; we'll snapshot after status becomes failed.

	w := worker.New(mock, pool, cfg)
	stop := startWorker(t, w)
	defer stop()

	waitUntil(t, 3*time.Second, "task did not reach failed", func() bool {
		return loadTask(t, pool, taskID).Status == "failed"
	})

	r := loadTask(t, pool, taskID)
	if r.Attempts != 1 {
		t.Fatalf("attempts = %d want 1", r.Attempts)
	}
	if !r.VisibleAt.After(time.Now().Add(-50 * time.Millisecond)) {
		// VisibleAt should be roughly in the future (or just past, since the
		// backoff window is tiny). Just ensure it's been set to a real value.
		t.Fatalf("visible_at not set sensibly: %v", r.VisibleAt)
	}
	if r.LastError == nil || *r.LastError == "" {
		t.Fatalf("last_error not populated")
	}
	if r.ClaimedBy != nil {
		t.Fatalf("claimed_by not cleared: %v", *r.ClaimedBy)
	}
	if r.LeaseExpiresAt != nil {
		t.Fatalf("lease_expires_at not cleared")
	}
}

func TestWorker_Exhaustion_WritesDeadMetadata(t *testing.T) {
	pool := openIntegrationPool(t)
	resetDispatchTasks(t, pool)

	outboxID := insertOutbox(t, pool)
	taskID := insertPendingTask(t, pool, outboxID, "ext.dead.target")

	mock := stream.NewMockStream()
	mock.SetError(errors.New("persistent failure"))

	cfg := baseConfig()
	cfg.MaxRetries = 2
	cfg.Backoff = worker.BackoffConfig{Base: 20 * time.Millisecond, Max: 100 * time.Millisecond}

	w := worker.New(mock, pool, cfg)
	stop := startWorker(t, w)
	defer stop()

	waitUntil(t, 5*time.Second, "task did not reach dead", func() bool {
		return loadTask(t, pool, taskID).Status == "dead"
	})

	r := loadTask(t, pool, taskID)
	if r.DeadAt == nil {
		t.Fatalf("dead_at not set")
	}
	if r.DeadReasonCode == nil || *r.DeadReasonCode != "retries_exhausted" {
		t.Fatalf("dead_reason_code = %v want retries_exhausted", r.DeadReasonCode)
	}
	if r.DeadErrorText == nil || *r.DeadErrorText == "" {
		t.Fatalf("dead_error_text not populated")
	}
	if r.DeadRetryCount != 0 {
		t.Fatalf("dead_retry_count = %d want 0 (first death)", r.DeadRetryCount)
	}

	var ctxMap map[string]any
	if err := json.Unmarshal(r.DeadContext, &ctxMap); err != nil {
		t.Fatalf("dead_context invalid JSON: %v", err)
	}
	if ctxMap["target"] != "ext.dead.target" {
		t.Fatalf("dead_context.target = %v", ctxMap["target"])
	}
	if ctxMap["outbox_id"] != outboxID.String() {
		t.Fatalf("dead_context.outbox_id = %v want %s", ctxMap["outbox_id"], outboxID)
	}
	if _, ok := ctxMap["attempts"]; !ok {
		t.Fatalf("dead_context missing attempts")
	}
}

func TestWorker_ClaimAtomicity_NoDoubleProcessing(t *testing.T) {
	pool := openIntegrationPool(t)
	resetDispatchTasks(t, pool)

	const n = 10
	taskIDs := make(map[uuid.UUID]struct{}, n)
	for i := 0; i < n; i++ {
		outboxID := insertOutbox(t, pool)
		id := insertPendingTask(t, pool, outboxID, fmt.Sprintf("ext.target.%d", i))
		taskIDs[id] = struct{}{}
	}

	mockA := stream.NewMockStream()
	mockB := stream.NewMockStream()

	cfg := baseConfig()
	cfg.Workers = 2
	cfg.ClaimBatchSize = 4
	cfg.ClaimInterval = 25 * time.Millisecond

	wA := worker.New(mockA, pool, cfg)
	wB := worker.New(mockB, pool, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = wA.Start(ctx) }()
	go func() { defer wg.Done(); _ = wB.Start(ctx) }()

	// Let them race for a bit then ensure everything finished.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var doneCount int
		if err := pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM dispatch_tasks WHERE status = 'done'`,
		).Scan(&doneCount); err != nil {
			t.Fatalf("count done: %v", err)
		}
		if doneCount == n {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Hold for a brief tail then stop, to surface any double-process.
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	// Every row done exactly once.
	var doneCount int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM dispatch_tasks WHERE status = 'done'`,
	).Scan(&doneCount); err != nil {
		t.Fatalf("count done: %v", err)
	}
	if doneCount != n {
		t.Fatalf("done count = %d want %d", doneCount, n)
	}

	pubs := append([]stream.MockTargetPublish{}, mockA.PublishedToSubjects()...)
	pubs = append(pubs, mockB.PublishedToSubjects()...)
	if len(pubs) != n {
		t.Fatalf("union of publishes = %d want %d", len(pubs), n)
	}

	seen := make(map[string]int, n)
	for _, p := range pubs {
		seen[p.DedupKey]++
	}
	if len(seen) != n {
		t.Fatalf("unique dedup keys = %d want %d", len(seen), n)
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("dedup key %s seen %d times (want 1)", k, c)
		}
	}
}

func TestWorker_LeaseReap_ResetsToPending(t *testing.T) {
	pool := openIntegrationPool(t)

	t.Run("reap-then-process", func(t *testing.T) {
		resetDispatchTasks(t, pool)

		outboxID := insertOutbox(t, pool)
		taskID := uuid.New()
		const initialAttempts = 2

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := pool.Exec(ctx, `
			INSERT INTO dispatch_tasks
				(id, event_id, source, event_type, producer_event_id, payload, target,
				 status, attempts, visible_at, claimed_by, lease_expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7,
			        'processing', $8, NOW(), 'dead-worker', NOW() - INTERVAL '1 minute')
		`, taskID, outboxID, "test-source", "test.event", uuid.New(),
			[]byte(`{"k":"v"}`), "ext.reap.target", initialAttempts)
		if err != nil {
			t.Fatalf("insert stale processing row: %v", err)
		}

		mock := stream.NewMockStream()
		cfg := baseConfig()
		cfg.ReaperInterval = 100 * time.Millisecond
		cfg.ClaimInterval = 50 * time.Millisecond

		w := worker.New(mock, pool, cfg)
		stop := startWorker(t, w)
		defer stop()

		waitUntil(t, 3*time.Second, "stale task not reaped+processed", func() bool {
			return loadTask(t, pool, taskID).Status == "done"
		})

		r := loadTask(t, pool, taskID)
		if r.Attempts != initialAttempts {
			t.Fatalf("attempts = %d want %d (reap+happy-path must not bump)",
				r.Attempts, initialAttempts)
		}
	})

	t.Run("reap-only", func(t *testing.T) {
		resetDispatchTasks(t, pool)

		outboxID := insertOutbox(t, pool)
		taskID := uuid.New()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := pool.Exec(ctx, `
			INSERT INTO dispatch_tasks
				(id, event_id, source, event_type, producer_event_id, payload, target,
				 status, attempts, visible_at, claimed_by, lease_expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7,
			        'processing', 0, NOW(), 'dead-worker', NOW() - INTERVAL '1 minute')
		`, taskID, outboxID, "test-source", "test.event", uuid.New(),
			[]byte(`{"k":"v"}`), "ext.reap.only", )
		if err != nil {
			t.Fatalf("insert stale processing row: %v", err)
		}

		mock := stream.NewMockStream()
		cfg := baseConfig()
		cfg.ReaperInterval = 100 * time.Millisecond
		// Very long claim interval so the row is reaped but not re-claimed
		// within the assertion window.
		cfg.ClaimInterval = 10 * time.Second

		w := worker.New(mock, pool, cfg)
		stop := startWorker(t, w)
		defer stop()

		waitUntil(t, 2*time.Second, "stale task not reaped to pending", func() bool {
			r := loadTask(t, pool, taskID)
			return r.Status == "pending" && r.ClaimedBy == nil
		})

		r := loadTask(t, pool, taskID)
		if r.LeaseExpiresAt != nil {
			t.Fatalf("lease_expires_at not cleared after reap")
		}
	})
}

func TestWorker_Retention_DeletesOldDone(t *testing.T) {
	pool := openIntegrationPool(t)
	resetDispatchTasks(t, pool)

	// Three done rows with varying processed_at ages; only the oldest
	// should be deleted given DoneRetention=7d.
	type seed struct {
		id     uuid.UUID
		ageSQL string // expression to subtract from NOW()
	}
	seeds := []seed{
		{id: uuid.New(), ageSQL: "10 days"}, // delete
		{id: uuid.New(), ageSQL: "2 days"},  // keep
		{id: uuid.New(), ageSQL: "1 hour"},  // keep
	}

	for _, s := range seeds {
		outboxID := insertOutbox(t, pool)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		q := fmt.Sprintf(`
			INSERT INTO dispatch_tasks
				(id, event_id, source, event_type, producer_event_id, payload, target,
				 status, visible_at, processed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7,
			        'done', NOW(), NOW() - INTERVAL '%s')
		`, s.ageSQL)
		_, err := pool.Exec(ctx, q, s.id, outboxID, "test-source", "test.event",
			uuid.New(), []byte(`{"k":"v"}`), "ext.retention.target")
		cancel()
		if err != nil {
			t.Fatalf("insert done seed (%s): %v", s.ageSQL, err)
		}
	}

	mock := stream.NewMockStream()
	cfg := baseConfig()
	cfg.DoneRetention = 7 * 24 * time.Hour
	cfg.RetentionInterval = 200 * time.Millisecond
	// Avoid claiming anything — all rows are already done.
	cfg.ClaimInterval = 10 * time.Second

	w := worker.New(mock, pool, cfg)
	stop := startWorker(t, w)
	defer stop()

	// Give the retention loop time to tick at least twice.
	waitUntil(t, 3*time.Second, "old done row not deleted", func() bool {
		return !taskExists(t, pool, seeds[0].id)
	})

	if taskExists(t, pool, seeds[0].id) {
		t.Fatalf("10-day-old done row was not deleted")
	}
	if !taskExists(t, pool, seeds[1].id) {
		t.Fatalf("2-day-old done row was deleted (should survive)")
	}
	if !taskExists(t, pool, seeds[2].id) {
		t.Fatalf("1-hour-old done row was deleted (should survive)")
	}
}
