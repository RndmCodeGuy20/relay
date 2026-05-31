package worker

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
)

// claimedTask is a row claimed from dispatch_tasks ready for processing.
// `OutboxID` is dispatch_tasks.event_id (the FK to outbox_events.id PK).
type claimedTask struct {
	TaskID          uuid.UUID
	OutboxID        uuid.UUID
	Source          string
	EventType       string
	ProducerEventID uuid.UUID // zero value if absent
	Payload         []byte
	Target          string
	Attempts        int
}

// claimSQL atomically transitions pending|failed rows whose visibility has
// elapsed to processing, recording the lease. SKIP LOCKED lets two workers
// race the same set without blocking each other.
const claimSQL = `
WITH claimed AS (
  SELECT id FROM dispatch_tasks
  WHERE status IN ('pending','failed')
    AND visible_at <= NOW()
  ORDER BY visible_at
  FOR UPDATE SKIP LOCKED
  LIMIT $1
)
UPDATE dispatch_tasks t
SET status = 'processing',
    claimed_by = $2,
    lease_expires_at = NOW() + ($3::int * INTERVAL '1 second'),
    last_attempt_at = NOW()
FROM claimed
WHERE t.id = claimed.id
RETURNING t.id, t.event_id, t.source, t.event_type,
          t.producer_event_id, t.payload, t.target, t.attempts;
`

func (w *Worker) claimLoop(ctx context.Context, out chan<- claimedTask) {
	log := logger.FromContext(ctx)

	interval := w.cfg.ClaimInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// First claim immediately rather than waiting for the ticker so an empty
	// queue's first arrival isn't delayed by a full tick.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tasks, err := w.claimBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("worker claim failed", zap.Error(err))
		}

		for _, t := range tasks {
			select {
			case out <- t:
			case <-ctx.Done():
				return
			}
		}

		// If we got a full batch there's likely more waiting — loop again
		// immediately. Otherwise wait for the next tick.
		if len(tasks) >= w.cfg.ClaimBatchSize {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) claimBatch(ctx context.Context) ([]claimedTask, error) {
	visibilitySec := int(w.cfg.VisibilityTimeout / time.Second)
	if visibilitySec <= 0 {
		visibilitySec = 30
	}

	rows, err := w.pool.Query(ctx, claimSQL, w.cfg.ClaimBatchSize, w.identity, visibilitySec)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := make([]claimedTask, 0, w.cfg.ClaimBatchSize)
	for rows.Next() {
		var (
			t            claimedTask
			producerNull *uuid.UUID
		)
		if err := rows.Scan(
			&t.TaskID,
			&t.OutboxID,
			&t.Source,
			&t.EventType,
			&producerNull,
			&t.Payload,
			&t.Target,
			&t.Attempts,
		); err != nil {
			return nil, err
		}
		if producerNull != nil {
			t.ProducerEventID = *producerNull
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tasks, nil
}
