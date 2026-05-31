package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Publish is the relay's outbound wire representation of one event.
//
// OutboxID is the canonical internal id (outbox_events.id PK) used for NATS
// dedup. ProducerEventID is the producer-supplied id, propagated for
// downstream correlation; (Source, ProducerEventID) is the producer-visible
// unique key, not used as a dedup key inside the relay.
type Publish struct {
	OutboxID        uuid.UUID
	ProducerEventID uuid.UUID
	Source          string
	EventType       string
	Payload         []byte
	LSN             string
	Sequence        int64
	PublishedAt     time.Time
}

// Message represents a message consumed from the stream.
type Message struct {
	Data    []byte
	Subject string
	Headers map[string]string

	ack  func() error
	nak  func() error
	term func() error
}

// Ack acknowledges the message so it is not redelivered.
func (m *Message) Ack() error {
	if m.ack != nil {
		return m.ack()
	}
	return nil
}

// Nak negatively acknowledges the message so it is redelivered.
func (m *Message) Nak() error {
	if m.nak != nil {
		return m.nak()
	}
	return nil
}

// Term terminates the message so it is not redelivered.
func (m *Message) Term() error {
	if m.term != nil {
		return m.term()
	}
	return nil
}

// Subscription represents a pull-based subscription to a stream.
type Subscription interface {
	Fetch(ctx context.Context, batch int) ([]*Message, error)
	Close() error
}

type Stream interface {
	Publish(ctx context.Context, event *Publish) error
	// PublishToSubject sends event to the given subject, using dedupKey as the
	// JetStream Nats-Msg-Id. The worker passes dispatch_tasks.id so retried
	// publishes of the same task dedup, while different fan-out targets for
	// the same event do NOT dedup (each task is its own row with its own id).
	PublishToSubject(ctx context.Context, subject string, dedupKey string, event *Publish) error
	Subscribe(consumerGroup string) (Subscription, error)
}

// NewMessage creates a Message from raw data for testing.
func NewMessage(data []byte) *Message {
	return &Message{Data: data}
}

// NewMessageWithCallbacks creates a Message with explicit ack/nak/term callbacks for mocking.
func NewMessageWithCallbacks(data []byte, subject string, headers map[string]string, ack, nak, term func() error) *Message {
	return &Message{
		Data:    data,
		Subject: subject,
		Headers: headers,
		ack:     ack,
		nak:     nak,
		term:    term,
	}
}

// ErrSubscriptionClosed is returned when a subscription is no longer active.
var ErrSubscriptionClosed = fmt.Errorf("subscription closed")
