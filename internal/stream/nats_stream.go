package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/config"
	"rndmcodeguy.in/relay/internal/logger"
)

const natsMsgIDHeader = "Nats-Msg-Id"

type NATSStream struct {
	conn         *nats.Conn
	js           nats.JetStreamContext
	streamName   string
	subject      string
	publishAfter time.Duration
	drainAfter   time.Duration
}

// natsPublishPayload is the on-wire JSON shape.
//
// outbox_id is the canonical internal id used for dedup; event_id is the
// producer-supplied id, kept for downstream correlation.
type natsPublishPayload struct {
	OutboxID        string          `json:"outbox_id"`
	ProducerEventID string          `json:"event_id"`
	Source          string          `json:"source"`
	EventType       string          `json:"event_type"`
	Payload         json.RawMessage `json:"payload"`
	LSN             string          `json:"lsn"`
	Sequence        int64           `json:"sequence"`
	PublishedAt     time.Time       `json:"published_at"`
}

func NewNATSStream(ctx context.Context, cfg config.NATSConfig) (*NATSStream, error) {
	if err := validateNATSConfig(cfg); err != nil {
		return nil, err
	}

	log := logger.FromContext(ctx)

	conn, err := nats.Connect(
		cfg.URL,
		nats.Name("relay-jetstream-publisher"),
		nats.Timeout(time.Duration(cfg.ConnectTimeoutSec)*time.Second),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(time.Duration(cfg.ReconnectWaitMs)*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
			if disconnectErr != nil {
				log.Warn("nats disconnected", zap.Error(disconnectErr))
				return
			}
			log.Warn("nats disconnected")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			err := nc.LastError()
			if err != nil {
				log.Warn("nats connection closed", zap.Error(err))
				return
			}
			log.Warn("nats connection closed")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("init jetstream context: %w", err)
	}

	info, err := js.StreamInfo(cfg.StreamName)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("lookup jetstream stream %q: %w", cfg.StreamName, err)
	}

	if !isSubjectCovered(info.Config.Subjects, cfg.Subject) {
		conn.Close()
		return nil, fmt.Errorf("subject %q is not covered by stream %q subjects %v", cfg.Subject, cfg.StreamName, info.Config.Subjects)
	}

	return &NATSStream{
		conn:         conn,
		js:           js,
		streamName:   cfg.StreamName,
		subject:      cfg.Subject,
		publishAfter: time.Duration(cfg.PublishTimeoutSec) * time.Second,
		drainAfter:   time.Duration(cfg.DrainTimeoutSec) * time.Second,
	}, nil
}

func (s *NATSStream) Publish(ctx context.Context, event *Publish) error {
	if event == nil {
		return fmt.Errorf("publish stream event: nil event")
	}

	payload, err := buildNATSPayload(event)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	msg := nats.NewMsg(s.subject)
	msg.Data = payload
	msg.Header.Set("Content-Type", "application/json")
	msg.Header.Set(natsMsgIDHeader, streamMessageID(event))

	publishCtx, cancel := context.WithTimeout(ctx, s.publishAfter)
	defer cancel()

	ack, err := s.js.PublishMsg(msg, nats.Context(publishCtx))
	if err != nil {
		return fmt.Errorf("publish to jetstream stream %q subject %q: %w", s.streamName, s.subject, err)
	}
	if ack == nil {
		return fmt.Errorf("publish to jetstream stream %q subject %q: missing ack", s.streamName, s.subject)
	}
	if ack.Stream != "" && ack.Stream != s.streamName {
		return fmt.Errorf("publish ack stream mismatch: got %q want %q", ack.Stream, s.streamName)
	}

	return nil
}

func (s *NATSStream) PublishToSubject(ctx context.Context, subject string, dedupKey string, event *Publish) error {
	if event == nil {
		return fmt.Errorf("publish stream event: nil event")
	}
	if strings.TrimSpace(subject) == "" {
		return fmt.Errorf("publish stream event: subject required")
	}
	if strings.TrimSpace(dedupKey) == "" {
		return fmt.Errorf("publish stream event: dedup key required")
	}

	payload, err := buildNATSPayload(event)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	msg := nats.NewMsg(subject)
	msg.Data = payload
	msg.Header.Set("Content-Type", "application/json")
	msg.Header.Set(natsMsgIDHeader, dedupKey)

	publishCtx, cancel := context.WithTimeout(ctx, s.publishAfter)
	defer cancel()

	ack, err := s.js.PublishMsg(msg, nats.Context(publishCtx))
	if err != nil {
		return fmt.Errorf("publish to jetstream stream %q subject %q: %w", s.streamName, subject, err)
	}
	if ack == nil {
		return fmt.Errorf("publish to jetstream stream %q subject %q: missing ack", s.streamName, subject)
	}
	if ack.Stream != "" && ack.Stream != s.streamName {
		return fmt.Errorf("publish ack stream mismatch: got %q want %q", ack.Stream, s.streamName)
	}

	return nil
}

func (s *NATSStream) Close(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return nil
	}

	drainCtx, cancel := context.WithTimeout(ctx, s.drainAfter)
	defer cancel()

	drainResult := make(chan error, 1)
	go func() {
		drainResult <- s.conn.Drain()
	}()

	select {
	case err := <-drainResult:
		if err != nil {
			s.conn.Close()
			return fmt.Errorf("drain nats connection: %w", err)
		}
		return nil
	case <-drainCtx.Done():
		s.conn.Close()
		return fmt.Errorf("drain nats connection timeout: %w", drainCtx.Err())
	}
}

func validateNATSConfig(cfg config.NATSConfig) error {
	if strings.TrimSpace(cfg.URL) == "" {
		return fmt.Errorf("nats url is required")
	}
	if strings.TrimSpace(cfg.StreamName) == "" {
		return fmt.Errorf("nats stream name is required")
	}
	if strings.TrimSpace(cfg.Subject) == "" {
		return fmt.Errorf("nats subject is required")
	}
	if cfg.ConnectTimeoutSec <= 0 {
		return fmt.Errorf("nats connect timeout must be > 0")
	}
	if cfg.PublishTimeoutSec <= 0 {
		return fmt.Errorf("nats publish timeout must be > 0")
	}
	if cfg.ReconnectWaitMs <= 0 {
		return fmt.Errorf("nats reconnect wait must be > 0")
	}
	if cfg.DrainTimeoutSec <= 0 {
		return fmt.Errorf("nats drain timeout must be > 0")
	}

	return nil
}

func (s *NATSStream) Subscribe(consumerGroup string) (Subscription, error) {
	if s == nil || s.js == nil {
		return nil, fmt.Errorf("nats stream not initialized")
	}
	if s.streamName == "" {
		return nil, fmt.Errorf("stream name not configured")
	}
	if s.subject == "" {
		return nil, fmt.Errorf("subject not configured")
	}
	if consumerGroup == "" {
		return nil, fmt.Errorf("consumer group required for subscribe")
	}

	sub, err := s.js.PullSubscribe(s.subject, consumerGroup, nats.BindStream(s.streamName))
	if err != nil {
		return nil, fmt.Errorf("pull subscribe stream=%q subject=%q group=%q: %w", s.streamName, s.subject, consumerGroup, err)
	}

	return &natsSubscription{sub: sub}, nil
}

// NATSSubscription wraps a NATS pull subscription.
type natsSubscription struct {
	sub *nats.Subscription
}

// Fetch pulls up to batch messages from NATS JetStream.
func (ns *natsSubscription) Fetch(ctx context.Context, batch int) ([]*Message, error) {
	var opts []nats.PullOpt
	if deadline, ok := ctx.Deadline(); ok {
		opts = append(opts, nats.MaxWait(time.Until(deadline)))
	}

	msgs, err := ns.sub.Fetch(batch, opts...)
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil
		}
		return nil, fmt.Errorf("fetch from nats: %w", err)
	}

	result := make([]*Message, len(msgs))
	for i, msg := range msgs {
		result[i] = newMessage(msg)
	}
	return result, nil
}

// Close unsubscribes from NATS.
func (ns *natsSubscription) Close() error {
	return ns.sub.Unsubscribe()
}

func newMessage(msg *nats.Msg) *Message {
	headers := make(map[string]string)
	for k, v := range msg.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}
	return &Message{
		Data:    msg.Data,
		Subject: msg.Subject,
		Headers: headers,
		ack:     func() error { return msg.Ack() },
		nak:     func() error { return msg.Nak() },
		term:    func() error { return msg.Term() },
	}
}

func buildNATSPayload(event *Publish) ([]byte, error) {
	payload := natsPublishPayload{
		OutboxID:        event.OutboxID.String(),
		ProducerEventID: event.ProducerEventID.String(),
		Source:          event.Source,
		EventType:       event.EventType,
		Payload:         json.RawMessage(event.Payload),
		LSN:             event.LSN,
		Sequence:        event.Sequence,
		PublishedAt:     event.PublishedAt.UTC(),
	}

	return json.Marshal(payload)
}

// streamMessageID returns the dedup key for JetStream. It is the canonical
// internal id (outbox PK), so replays of the same outbox row from the WAL
// always produce the same message id and JetStream dedups them.
func streamMessageID(event *Publish) string {
	return event.OutboxID.String()
}

func isSubjectCovered(patterns []string, subject string) bool {
	for _, pattern := range patterns {
		if subjectMatchesPattern(pattern, subject) {
			return true
		}
	}

	return false
}

func subjectMatchesPattern(pattern string, subject string) bool {
	if pattern == "" || subject == "" {
		return false
	}

	patternTokens := strings.Split(pattern, ".")
	subjectTokens := strings.Split(subject, ".")

	si := 0
	for i, token := range patternTokens {
		switch token {
		case ">":
			return i == len(patternTokens)-1
		case "*":
			if si >= len(subjectTokens) {
				return false
			}
			si++
		default:
			if si >= len(subjectTokens) {
				return false
			}
			if token != subjectTokens[si] {
				return false
			}
			si++
		}
	}

	return si == len(subjectTokens)
}
