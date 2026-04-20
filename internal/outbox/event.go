package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EventInsert struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	SchemaVersion string          `json:"schema_version"`
	Source        string          `json:"source"`
	OccurredAt    *time.Time      `json:"occurred_at"`
	ReceivedAt    time.Time       `json:"received_at"`
}

type Event struct {
	ID            uuid.UUID       `json:"id" db:"id"`
	EventID       uuid.UUID       `json:"event_id" db:"event_id"`
	EventType     string          `json:"event_type" db:"event_type"`
	Payload       json.RawMessage `json:"payload" db:"payload"`
	SchemaVersion string          `json:"schema_version" db:"schema_version"`
	Source        string          `json:"source" db:"source"`
	Relayed       bool            `json:"relayed" db:"relayed"`
	OccurredAt    *time.Time      `json:"occurred_at" db:"occurred_at"`
	ReceivedAt    time.Time       `json:"received_at" db:"received_at"`
	RelayedAt     *time.Time      `json:"relayed_at" db:"relayed_at"`
	CreatedAt     time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at" db:"updated_at"`
}
