package stream

import (
	"context"

	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
)

type LoggingStream struct{}

func NewLoggingStream() *LoggingStream {
	return &LoggingStream{}
}

func (s *LoggingStream) Publish(ctx context.Context, event *Publish) error {
	log := logger.FromContext(ctx)
	log.Info(
		"stream publish",
		zap.String("event_id", event.EventID.String()),
		zap.String("relay_id", event.RelayID.String()),
		zap.String("lsn", event.LSN),
		zap.Int64("sequence", event.Sequence),
	)

	return nil
}
