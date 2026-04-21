package rule

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	OperatorEq       = "eq"
	OperatorNeq      = "neq"
	OperatorIn       = "in"
	OperatorContains = "contains"
	OperatorExists   = "exists"

	TargetModePublish = "publish"
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

type Target struct {
	Name    string            `json:"name"`
	Subject string            `json:"subject"`
	Headers map[string]string `json:"headers,omitempty"`
	Mode    string            `json:"mode,omitempty"`
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
