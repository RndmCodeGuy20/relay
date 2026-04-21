package rule

import (
	"encoding/json"
	"testing"
)

func TestRuleValidate_ValidRule(t *testing.T) {
	r := Rule{
		Name:      "order-rule",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Version:   1,
		Selector: Selector{All: []Predicate{
			{Path: "order.total", Op: OperatorEq, Value: json.RawMessage(`150`)},
		}},
		Targets: []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	if err := r.Validate(); err != nil {
		t.Fatalf("expected valid rule, got error: %v", err)
	}
}

func TestRuleValidate_EnabledWithoutTargets(t *testing.T) {
	r := Rule{
		Name:      "bad-rule",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Version:   1,
	}

	if err := r.Validate(); err == nil {
		t.Fatalf("expected validation error for enabled rule without targets")
	}
}

func TestRuleValidate_DuplicateTargetNames(t *testing.T) {
	r := Rule{
		Name:      "dup-targets",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Version:   1,
		Targets: []Target{
			{Name: "ops", Subject: "relay.ops"},
			{Name: "ops", Subject: "relay.ops.2"},
		},
	}

	if err := r.Validate(); err == nil {
		t.Fatalf("expected validation error for duplicate target names")
	}
}

func TestRuleValidate_InvalidPredicateOperator(t *testing.T) {
	r := Rule{
		Name:      "bad-op",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.total", Op: "bad", Value: json.RawMessage(`1`)}}},
		Targets:   []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	if err := r.Validate(); err == nil {
		t.Fatalf("expected validation error for invalid operator")
	}
}

func TestRuleValidate_ValueRequiredForNonExists(t *testing.T) {
	r := Rule{
		Name:      "missing-value",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.total", Op: OperatorEq}}},
		Targets:   []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	if err := r.Validate(); err == nil {
		t.Fatalf("expected validation error for missing value")
	}
}
