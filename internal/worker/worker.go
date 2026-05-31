// Package worker drains dispatch_tasks rows: claims pending work via
// SKIP LOCKED leasing, publishes to the per-target NATS subject, and
// transitions task state through the lifecycle pending → processing →
// done | failed | dead. A separate reaper recovers leases held by dead
// workers; a retention loop trims old `done` rows.
package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/stream"
)

// Config groups the runtime knobs that shape the worker pool.
type Config struct {
	Workers              int
	ClaimBatchSize       int
	ClaimInterval        time.Duration
	VisibilityTimeout    time.Duration
	ReaperInterval       time.Duration
	MaxRetries           int
	Backoff              BackoffConfig
	PublishTimeout       time.Duration
	DoneRetention        time.Duration
	RetentionInterval    time.Duration
}

// Worker owns the dispatch_tasks drain pipeline.
type Worker struct {
	publisher stream.Stream
	pool      *pgxpool.Pool
	cfg       Config
	identity  string

	// rngMu guards rng. math/rand sources are not safe for concurrent use;
	// processor goroutines compute backoff in parallel.
	rngMu sync.Mutex
	rng   *rand.Rand
}

// New constructs a Worker with a stable identity (hostname-pid-shortid) used
// as claimed_by so dead-worker leases can be attributed during reaping.
func New(publisher stream.Stream, pool *pgxpool.Pool, cfg Config) *Worker {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	short := uuid.NewString()[:8]
	identity := fmt.Sprintf("%s-%d-%s", host, os.Getpid(), short)

	return &Worker{
		publisher: publisher,
		pool:      pool,
		cfg:       cfg,
		identity:  identity,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Identity returns the stable claimed_by string for this worker process.
func (w *Worker) Identity() string { return w.identity }

// Start runs the claimer, processor pool, reaper, and retention loops until
// ctx is cancelled. It blocks for the lifetime of the worker.
func (w *Worker) Start(ctx context.Context) error {
	log := logger.FromContext(ctx)

	if w.cfg.Workers <= 0 {
		return fmt.Errorf("worker: pool size must be > 0")
	}

	// taskCh buffers claimed tasks between the single claimer goroutine and
	// the processor pool. Sized to the claim batch so the claimer can hand
	// off a full batch without blocking when processors are idle.
	bufSize := w.cfg.ClaimBatchSize
	if bufSize <= 0 {
		bufSize = 1
	}
	taskCh := make(chan claimedTask, bufSize)

	var wg sync.WaitGroup

	// Processors.
	for i := 0; i < w.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.processLoop(ctx, id, taskCh)
		}(i)
	}

	// Claimer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(taskCh)
		w.claimLoop(ctx, taskCh)
	}()

	// Reaper.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.reapLoop(ctx)
	}()

	// Retention.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.retentionLoop(ctx)
	}()

	log.Info("worker started",
		zap.String("identity", w.identity),
		zap.Int("workers", w.cfg.Workers),
		zap.Int("claim_batch", w.cfg.ClaimBatchSize),
		zap.Duration("visibility_timeout", w.cfg.VisibilityTimeout),
	)

	<-ctx.Done()
	log.Info("worker shutting down", zap.String("identity", w.identity))
	wg.Wait()
	log.Info("worker stopped", zap.String("identity", w.identity))

	return ctx.Err()
}

// computeBackoff is concurrency-safe; processors share the worker's rng.
func (w *Worker) computeBackoff(attempts int) time.Duration {
	w.rngMu.Lock()
	defer w.rngMu.Unlock()
	return nextBackoff(attempts, w.cfg.Backoff, w.rng)
}
