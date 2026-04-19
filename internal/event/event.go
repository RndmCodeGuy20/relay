package event

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
)

type RelayEvent struct {
	EventID    uuid.UUID
	RelayID    uuid.UUID
	EventType  string
	Source     string
	LSN        pglogrepl.LSN
	Sequence   int64
	ReceivedAt time.Time
	Done       chan error
	Deadline   time.Time
	Attempt    int
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
