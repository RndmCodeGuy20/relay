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

func (s *LoggingStream) Subscribe(consumerGroup string) (Subscription, error) {
	return &loggingSubscription{}, nil
}

func (s *LoggingStream) Publish(ctx context.Context, event *Publish) error {
	log := logger.FromContext(ctx)
	log.Info(
		"stream publish",
		zap.String("outbox_id", event.OutboxID.String()),
		zap.String("event_id", event.ProducerEventID.String()),
		zap.String("source", event.Source),
		zap.String("event_type", event.EventType),
		zap.String("lsn", event.LSN),
		zap.Int64("sequence", event.Sequence),
	)

	return nil
}

func (s *LoggingStream) PublishToSubject(ctx context.Context, subject string, dedupKey string, event *Publish) error {
	log := logger.FromContext(ctx)
	log.Info(
		"stream publish to subject",
		zap.String("subject", subject),
		zap.String("dedup_key", dedupKey),
		zap.String("outbox_id", event.OutboxID.String()),
		zap.String("event_id", event.ProducerEventID.String()),
		zap.String("source", event.Source),
		zap.String("event_type", event.EventType),
		zap.String("lsn", event.LSN),
		zap.Int64("sequence", event.Sequence),
	)

	return nil
}

type loggingSubscription struct{}

func (s *loggingSubscription) Fetch(ctx context.Context, batch int) ([]*Message, error) {
	return nil, nil
}

func (s *loggingSubscription) Close() error {
	return nil
}
