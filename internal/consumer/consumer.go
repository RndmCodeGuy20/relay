package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/rule"
	"rndmcodeguy.in/relay/internal/stream"
)

// Consumer pulls events from the NATS hub subject, evaluates routing rules
// against the cached rule set, and writes one dispatch_tasks row per matched
// target. The worker (phase 3b) drains those rows and publishes to the
// per-target subject. Consumer does not publish.
type Consumer struct {
	stream        stream.Stream
	pool          *pgxpool.Pool
	cache         RuleSource
	workers       int
	batchSize     int
	consumerGroup string
	fetchTimeout  time.Duration
}

// RuleSource is the read side of the rule cache. Kept narrow so tests can
// stub it without spinning up a Postgres-backed cache.
type RuleSource interface {
	Get(source string, eventType string) []rule.Rule
}

// Config groups the knobs that influence consumer runtime behaviour.
type Config struct {
	Workers       int
	BatchSize     int
	ConsumerGroup string
	FetchTimeout  time.Duration
}

func New(s stream.Stream, pool *pgxpool.Pool, cache RuleSource, cfg Config) *Consumer {
	fetchTimeout := cfg.FetchTimeout
	if fetchTimeout <= 0 {
		fetchTimeout = 5 * time.Second
	}
	return &Consumer{
		stream:        s,
		pool:          pool,
		cache:         cache,
		workers:       cfg.Workers,
		batchSize:     cfg.BatchSize,
		consumerGroup: cfg.ConsumerGroup,
		fetchTimeout:  fetchTimeout,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	log := logger.FromContext(ctx)

	sub, err := c.stream.Subscribe(c.consumerGroup)
	if err != nil {
		return fmt.Errorf("consumer subscribe: %w", err)
	}
	defer sub.Close()

	log.Info("consumer started",
		zap.Int("workers", c.workers),
		zap.String("group", c.consumerGroup),
		zap.Int("batch_size", c.batchSize),
	)

	for i := 0; i < c.workers; i++ {
		go c.worker(ctx, sub)
	}

	<-ctx.Done()
	log.Info("consumer shutting down")
	return ctx.Err()
}

func (c *Consumer) worker(ctx context.Context, sub stream.Subscription) {
	log := logger.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batchCtx, cancel := context.WithTimeout(ctx, c.fetchTimeout)
		msgs, err := sub.Fetch(batchCtx, c.batchSize)
		cancel()

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Warn("consumer fetch failed", zap.Error(err))
			continue
		}

		for _, msg := range msgs {
			if err := c.processMessage(ctx, msg); err != nil {
				log.Error("consumer process message failed", zap.Error(err))
				// Negative-ack so NATS redelivers immediately rather than
				// waiting for AckWait to expire.
				if nakErr := msg.Nak(); nakErr != nil {
					log.Error("consumer nak failed", zap.Error(nakErr))
				}
				continue
			}
			if err := msg.Ack(); err != nil {
				log.Error("consumer ack failed", zap.Error(err))
			}
		}
	}
}

// publishedEvent mirrors stream.natsPublishPayload — kept private here so the
// stream layer owns the canonical wire shape.
type publishedEvent struct {
	OutboxID        string          `json:"outbox_id"`
	ProducerEventID string          `json:"event_id"`
	Source          string          `json:"source"`
	EventType       string          `json:"event_type"`
	Payload         json.RawMessage `json:"payload"`
	LSN             string          `json:"lsn"`
	Sequence        int64           `json:"sequence"`
	PublishedAt     time.Time       `json:"published_at"`
}

func (c *Consumer) processMessage(ctx context.Context, msg *stream.Message) error {
	log := logger.FromContext(ctx)

	var ev publishedEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		log.Error("consumer unmarshal failed", zap.Error(err))
		return fmt.Errorf("unmarshal message: %w", err)
	}

	if ev.Source == "" || ev.EventType == "" {
		log.Warn("consumer skipping message: missing source or event_type",
			zap.String("outbox_id", ev.OutboxID),
		)
		return nil
	}

	outboxID, err := uuid.Parse(ev.OutboxID)
	if err != nil {
		return fmt.Errorf("invalid outbox_id %q: %w", ev.OutboxID, err)
	}

	var producerEventID uuid.UUID
	if ev.ProducerEventID != "" {
		producerEventID, err = uuid.Parse(ev.ProducerEventID)
		if err != nil {
			return fmt.Errorf("invalid producer event_id %q: %w", ev.ProducerEventID, err)
		}
	}

	rules := c.cache.Get(ev.Source, ev.EventType)
	if len(rules) == 0 {
		log.Debug("consumer no rules registered for event",
			zap.String("outbox_id", ev.OutboxID),
			zap.String("source", ev.Source),
			zap.String("event_type", ev.EventType),
		)
		return nil
	}

	input := rule.InputEvent{
		Source:    ev.Source,
		EventType: ev.EventType,
		Payload:   ev.Payload,
	}

	result, err := rule.EvaluateEvent(input, rules)
	if err != nil {
		log.Error("consumer rule evaluation failed",
			zap.Error(err),
			zap.String("outbox_id", ev.OutboxID),
		)
		return fmt.Errorf("evaluate rules: %w", err)
	}

	if result.NoMatch || len(result.Intents) == 0 {
		log.Info("consumer rule no_match",
			zap.String("outbox_id", ev.OutboxID),
			zap.String("source", ev.Source),
			zap.String("event_type", ev.EventType),
		)
		return nil
	}

	for _, intent := range result.Intents {
		if err := c.insertTask(ctx, outboxID, producerEventID, ev, intent.Target.Subject); err != nil {
			return fmt.Errorf("insert task for target %s: %w", intent.Target.Subject, err)
		}
	}

	log.Info("consumer tasks inserted",
		zap.String("outbox_id", ev.OutboxID),
		zap.Int("targets", len(result.Intents)),
	)
	return nil
}

func (c *Consumer) insertTask(
	ctx context.Context,
	outboxID uuid.UUID,
	producerEventID uuid.UUID,
	ev publishedEvent,
	target string,
) error {
	// ON CONFLICT keeps the consumer idempotent: NATS at-least-once may
	// redeliver the same message, but (event_id, target) uniqueness on
	// dispatch_tasks ensures only one row per (outbox PK, target) pair.
	query := `
		INSERT INTO dispatch_tasks
		    (event_id, source, event_type, producer_event_id, payload, target,
		     status, attempts, visible_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, NOW())
		ON CONFLICT (event_id, target) DO NOTHING
	`

	var producer any
	if producerEventID != uuid.Nil {
		producer = producerEventID
	}

	payload := []byte(ev.Payload)
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}

	_, err := c.pool.Exec(ctx, query,
		outboxID,
		ev.Source,
		ev.EventType,
		producer,
		payload,
		target,
	)
	return err
}
