package rule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	OperatorEq       = "eq"
	OperatorNeq      = "neq"
	OperatorIn       = "in"
	OperatorContains = "contains"
	OperatorExists   = "exists"
)

type Rule struct {
	ID        uuid.UUID `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Source    string    `json:"source" db:"source"`
	EventType string    `json:"event_type" db:"event_type"`
	Enabled   bool      `json:"enabled" db:"enabled"`
	Priority  int       `json:"priority" db:"priority"`
	Version   int       `json:"version" db:"version"`
	Selector  Selector  `json:"selector" db:"selector"`
	Targets   []Target  `json:"targets" db:"targets"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

type Selector struct {
	All []Predicate `json:"all,omitempty"`
	Any []Predicate `json:"any,omitempty"`
}

type Predicate struct {
	Path  string          `json:"path"`
	Op    string          `json:"op"`
	Value json.RawMessage `json:"value,omitempty"`
}

// Target identifies a destination for a matched rule. 0.1.0 supports
// subject-only routing; headers and modes are intentionally absent until
// there is a concrete use case (see phase plan, "deferred intentionally").
//
// JSON decoding rejects unknown fields so rules that accidentally include
// deferred or misspelled fields fail loudly at load time instead of
// silently dropping configuration.
type Target struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
}

// targetWire mirrors Target's shape — its sole purpose is to give
// UnmarshalJSON a type to decode into without recursing into Target's own
// UnmarshalJSON.
type targetWire struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
}

func (t *Target) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var w targetWire
	if err := dec.Decode(&w); err != nil {
		return fmt.Errorf("target: %w", err)
	}
	t.Name = w.Name
	t.Subject = w.Subject
	return nil
}

type InputEvent struct {
	Source    string          `json:"source"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type Intent struct {
	RuleID   uuid.UUID `json:"rule_id"`
	RuleName string    `json:"rule_name"`
	Target   Target    `json:"target"`
}

type EvaluationResult struct {
	Intents []Intent `json:"intents"`
	NoMatch bool     `json:"no_match"`
}
