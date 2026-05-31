package worker

import (
	"context"
	"time"

	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
)

const retentionSQL = `
	DELETE FROM dispatch_tasks
	WHERE status = 'done'
	  AND processed_at < NOW() - ($1::int * INTERVAL '1 day')
`

func (w *Worker) retentionLoop(ctx context.Context) {
	log := logger.FromContext(ctx)

	interval := w.cfg.RetentionInterval
	if interval <= 0 {
		interval = time.Hour
	}
	retentionDays := int(w.cfg.DoneRetention / (24 * time.Hour))
	if retentionDays <= 0 {
		// Disabled: skip running the loop entirely.
		log.Info("worker retention disabled (DoneRetention <= 0)")
		<-ctx.Done()
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.runRetentionOnce(ctx, retentionDays)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("worker retention sweep failed", zap.Error(err))
				continue
			}
			if n > 0 {
				log.Info("worker retention deleted done rows",
					zap.Int64("count", n),
					zap.Int("retention_days", retentionDays),
				)
			}
		}
	}
}

func (w *Worker) runRetentionOnce(ctx context.Context, retentionDays int) (int64, error) {
	tag, err := w.pool.Exec(ctx, retentionSQL, retentionDays)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
