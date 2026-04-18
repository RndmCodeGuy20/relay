package ingestion

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"rndmcodeguy.in/relay/internal/validate"
)

type IngestRequest struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	SchemaVersion string          `json:"schema_version"`
	Source        string          `json:"source"`
	OccurredAt    *time.Time      `json:"occurred_at,omitempty"`
}

func (r *IngestRequest) Validate() error {
	if r.EventID == uuid.Nil {
		r.EventID = uuid.New()
	}

	var errs validate.ValidationErrors

	if r.EventType == "" {
		errs = append(errs, validate.Field("event_type", "is required"))
	}

	if r.Source == "" {
		errs = append(errs, validate.Field("source", "is required"))
	}

	if r.SchemaVersion == "" {
		errs = append(errs, validate.Field("schema_version", "is required"))
	}

	if len(r.Payload) == 0 {
		errs = append(errs, validate.Field("payload", "is required"))
	}

	// Optional: ensure payload is valid JSON
	if !json.Valid(r.Payload) {
		errs = append(errs, validate.Field("payload", "must be valid JSON"))
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
}
