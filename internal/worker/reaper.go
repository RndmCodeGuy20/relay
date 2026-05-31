package worker

import (
	"context"
	"time"

	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
)

// reapSQL resets rows whose lease has elapsed back to pending so another
// worker can claim them. `attempts` is intentionally not bumped: a reaped
// row crashed before its outcome was recorded, so we don't know whether
// the publish succeeded — counting it as a failed attempt would let
// unstable hosts exhaust retries via crashes alone.
const reapSQL = `
	UPDATE dispatch_tasks
	SET status = 'pending',
	    claimed_by = NULL,
	    lease_expires_at = NULL
	WHERE status = 'processing'
	  AND lease_expires_at < NOW()
`

func (w *Worker) reapLoop(ctx context.Context) {
	log := logger.FromContext(ctx)

	interval := w.cfg.ReaperInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.reapOnce(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("worker reap failed", zap.Error(err))
				continue
			}
			if n > 0 {
				log.Info("worker reaped stale leases", zap.Int64("count", n))
			}
		}
	}
}

func (w *Worker) reapOnce(ctx context.Context) (int64, error) {
	tag, err := w.pool.Exec(ctx, reapSQL)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
