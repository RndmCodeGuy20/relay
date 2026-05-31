package event

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
)

// RelayEvent is a decoded outbox row in transit through the relay pipeline.
//
// OutboxID is outbox_events.id (server-generated PK) — the canonical internal
// identifier used for NATS dedup and dispatch_tasks correlation.
//
// ProducerEventID is outbox_events.event_id (producer-supplied). Unique only
// when combined with Source; propagated on the wire for downstream
// correlation, never used as a dedup key inside the relay.
type RelayEvent struct {
	OutboxID        uuid.UUID
	ProducerEventID uuid.UUID
	EventType       string
	Source          string
	Payload         []byte
	LSN             pglogrepl.LSN
	Sequence        int64
	ReceivedAt      time.Time
	Done            chan error
	Deadline        time.Time
	Attempt         int
}

func (e *RelayEvent) IsExpired(now time.Time) bool {
	return now.After(e.Deadline)
}

func (e *RelayEvent) Ack() {
	select {
	case e.Done <- nil:
	default:
	}
}

func (e *RelayEvent) Nack(err error) {
	select {
	case e.Done <- err:
	default:
	}
}
