package dispatch

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/event"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/stream"
)

type Dispatcher struct {
	workers    int
	eventCh    chan *event.RelayEvent
	stream     stream.Stream
	pool       *pgxpool.Pool
	maxRetries int
}

func New(workers int, s stream.Stream, pool *pgxpool.Pool, maxRetries int) *Dispatcher {
	return &Dispatcher{
		workers:    workers,
		eventCh:    make(chan *event.RelayEvent, 256),
		stream:     s,
		pool:       pool,
		maxRetries: maxRetries,
	}
}

// Start spawns worker goroutines and waits for context cancellation
func (d *Dispatcher) Start(ctx context.Context) error {
	log := logger.FromContext(ctx)

	for i := 0; i < d.workers; i++ {
		go d.worker(ctx)
	}

	log.Info("dispatcher started", zap.Int("workers", d.workers))
	<-ctx.Done()
	log.Info("dispatcher shutting down")

	return ctx.Err()
}

// Submit sends event to the dispatcher queue and blocks when queue is full.
func (d *Dispatcher) Submit(ctx context.Context, e *event.RelayEvent) error {
	select {
	case d.eventCh <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// worker processes events from the queue
func (d *Dispatcher) worker(ctx context.Context) {
	log := logger.FromContext(ctx)

	for {
		select {
		case event := <-d.eventCh:
			if event == nil {
				return
			}

			log.Info("processing event", zap.String("event_id", event.EventID.String()), zap.String("relay_id", event.RelayID.String()), zap.Int("attempt", event.Attempt))

			d.processEvent(ctx, event)

		case <-ctx.Done():
			return
		}
	}
}

// processEvent handles a single event: insert task, publish, and update status
func (d *Dispatcher) processEvent(ctx context.Context, event *event.RelayEvent) {
	log := logger.FromContext(ctx)

	// Insert dispatch_tasks row
	if err := d.insertTask(ctx, event); err != nil {
		log.Error("failed to insert dispatch task", zap.Error(err), zap.String("event_id", event.EventID.String()))
		event.Nack(err)
		return
	}

	// Convert and publish event
	if err := d.publishEvent(ctx, event); err != nil {
		log.Debug("event publish failed", zap.Error(err), zap.String("event_id", event.EventID.String()), zap.Int("attempt", event.Attempt))
		errMsg := err.Error()
		if updateErr := d.updateTaskStatus(ctx, event.EventID, event.RelayID, "failed", &errMsg); updateErr != nil {
			log.Error("failed to update task status to failed", zap.Error(updateErr), zap.String("event_id", event.EventID.String()))
		}

		// Check if we should retry
		if event.Attempt < d.maxRetries {
			event.Attempt++
			// Resubmit to queue for retry
			select {
			case d.eventCh <- event:
				return
			case <-ctx.Done():
				return
			}
		}

		// Max retries exceeded, mark as dead
		errMsg = err.Error()
		if err := d.updateTaskStatus(ctx, event.EventID, event.RelayID, "dead", &errMsg); err != nil {
			log.Error("failed to update task status to dead", zap.Error(err), zap.String("event_id", event.EventID.String()))
		}

		event.Nack(err)
		return
	}

	// Publish succeeded, mark as done
	if err := d.updateTaskStatus(ctx, event.EventID, event.RelayID, "done", nil); err != nil {
		log.Error("failed to update task status to done", zap.Error(err), zap.String("event_id", event.EventID.String()))
		event.Nack(err)
		return
	}

	event.Ack()
}

// insertTask creates a dispatch_tasks row for tracking
func (d *Dispatcher) insertTask(ctx context.Context, event *event.RelayEvent) error {
	query := `
		INSERT INTO dispatch_tasks (event_id, relay_id, target, status, attempts, visible_at)
		SELECT $1, $2, $3, $4, $5, NOW()
		WHERE NOT EXISTS (
			SELECT 1
			FROM dispatch_tasks
			WHERE event_id = $1 AND relay_id = $2
		)
	`
	_, err := d.pool.Exec(ctx, query, event.EventID, event.RelayID, "", "pending", event.Attempt)
	return err
}

// publishEvent converts RelayEvent to stream.Publish and publishes to stream.
func (d *Dispatcher) publishEvent(ctx context.Context, event *event.RelayEvent) error {
	sp := &stream.Publish{
		EventID:     event.EventID,
		RelayID:     event.RelayID,
		LSN:         event.LSN.String(),
		Sequence:    event.Sequence,
		PublishedAt: time.Now(),
	}

	return d.stream.Publish(ctx, sp)
}

// updateTaskStatus updates the dispatch_tasks table with new status and optional error
func (d *Dispatcher) updateTaskStatus(ctx context.Context, eventID uuid.UUID, relayID uuid.UUID, status string, lastError *string) error {
	query := `
		UPDATE dispatch_tasks
		SET status = $1, last_error = $2, processed_at = NOW(), attempts = attempts + 1
		WHERE event_id = $3 AND relay_id = $4
	`
	_, err := d.pool.Exec(ctx, query, status, lastError, eventID, relayID)
	return err
}
