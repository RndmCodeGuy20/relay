package stream

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Publish struct {
	EventID     uuid.UUID
	RelayID     uuid.UUID
	LSN         string
	Sequence    int64
	PublishedAt time.Time
}

type Stream interface {
	Publish(ctx context.Context, event *Publish) error
}
