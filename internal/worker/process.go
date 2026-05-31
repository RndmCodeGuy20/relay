package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/stream"
)

const (
	// SQL fragments share the post-processing lease cleanup pattern across
	// done/failed/dead so the column list stays grep-able.

	markDoneSQL = `
		UPDATE dispatch_tasks
		SET status = 'done',
		    processed_at = NOW(),
		    claimed_by = NULL,
		    lease_expires_at = NULL,
		    last_error = NULL,
		    dead_at = NULL,
		    dead_reason_code = NULL,
		    dead_error_text = NULL,
		    dead_context = '{}'::jsonb
		WHERE id = $1
	`

	markFailedSQL = `
		UPDATE dispatch_tasks
		SET status = 'failed',
		    attempts = attempts + 1,
		    last_error = $2,
		    visible_at = $3,
		    claimed_by = NULL,
		    lease_expires_at = NULL
		WHERE id = $1
	`

	// dead_retry_count only increments when a row re-enters dead state
	// (locked decision Q4 / phase plan). The first time a row dies, the
	// counter stays at 0.
	markDeadSQL = `
		UPDATE dispatch_tasks
		SET status = 'dead',
		    attempts = attempts + 1,
		    dead_at = NOW(),
		    dead_reason_code = 'retries_exhausted',
		    dead_error_text = $2,
		    dead_context = $3,
		    dead_retry_count = dead_retry_count
		        + CASE WHEN dead_at IS NOT NULL THEN 1 ELSE 0 END,
		    claimed_by = NULL,
		    lease_expires_at = NULL
		WHERE id = $1
	`
)

func (w *Worker) processLoop(ctx context.Context, id int, in <-chan claimedTask) {
	log := logger.FromContext(ctx).With(zap.Int("processor", id))
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-in:
			if !ok {
				return
			}
			w.processTask(ctx, log, t)
		}
	}
}

func (w *Worker) processTask(ctx context.Context, log *zap.Logger, t claimedTask) {
	log = log.With(
		zap.String("task_id", t.TaskID.String()),
		zap.String("outbox_id", t.OutboxID.String()),
		zap.String("target", t.Target),
		zap.Int("attempts", t.Attempts),
	)

	publishCtx, cancel := context.WithTimeout(ctx, w.cfg.PublishTimeout)
	defer cancel()

	pub := &stream.Publish{
		OutboxID:        t.OutboxID,
		ProducerEventID: t.ProducerEventID,
		Source:          t.Source,
		EventType:       t.EventType,
		Payload:         t.Payload,
		Sequence:        0,
		PublishedAt:     time.Now().UTC(),
	}

	err := w.publisher.PublishToSubject(publishCtx, t.Target, t.TaskID.String(), pub)
	if err == nil {
		if updateErr := w.markDone(ctx, t.TaskID); updateErr != nil {
			log.Error("worker mark done failed", zap.Error(updateErr))
			return
		}
		log.Info("worker publish ok")
		return
	}

	// Distinguish parent-context cancellation (worker shutting down) from a
	// real publish error: on shutdown we leave the lease in place so the
	// reaper will surface the row after restart.
	if errors.Is(ctx.Err(), context.Canceled) {
		log.Warn("worker publish aborted by shutdown", zap.Error(err))
		return
	}

	nextAttempts := t.Attempts + 1
	if nextAttempts >= w.cfg.MaxRetries {
		deadCtx := buildDeadContext(t, err)
		if updateErr := w.markDead(ctx, t.TaskID, err.Error(), deadCtx); updateErr != nil {
			log.Error("worker mark dead failed", zap.Error(updateErr))
			return
		}
		log.Warn("worker task dead (retries exhausted)", zap.Error(err))
		return
	}

	visibleAt := time.Now().Add(w.computeBackoff(t.Attempts))
	if updateErr := w.markFailed(ctx, t.TaskID, err.Error(), visibleAt); updateErr != nil {
		log.Error("worker mark failed update failed", zap.Error(updateErr))
		return
	}
	log.Info("worker publish failed, retry scheduled",
		zap.Error(err),
		zap.Time("visible_at", visibleAt),
	)
}

func (w *Worker) markDone(ctx context.Context, taskID uuid.UUID) error {
	_, err := w.pool.Exec(ctx, markDoneSQL, taskID)
	return err
}

func (w *Worker) markFailed(ctx context.Context, taskID uuid.UUID, errText string, visibleAt time.Time) error {
	_, err := w.pool.Exec(ctx, markFailedSQL, taskID, errText, visibleAt)
	return err
}

func (w *Worker) markDead(ctx context.Context, taskID uuid.UUID, errText string, deadContext []byte) error {
	_, err := w.pool.Exec(ctx, markDeadSQL, taskID, errText, deadContext)
	return err
}

// buildDeadContext snapshots task fields useful for debugging a dead row
// without re-joining outbox or other tables.
func buildDeadContext(t claimedTask, err error) []byte {
	ctx := map[string]any{
		"attempts":   t.Attempts + 1,
		"target":     t.Target,
		"last_error": err.Error(),
		"source":     t.Source,
		"event_type": t.EventType,
		"outbox_id":  t.OutboxID.String(),
	}
	if t.ProducerEventID != uuid.Nil {
		ctx["producer_event_id"] = t.ProducerEventID.String()
	}
	data, _ := json.Marshal(ctx)
	return data
}
