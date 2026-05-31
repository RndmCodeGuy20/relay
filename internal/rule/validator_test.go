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

func TestTarget_UnmarshalJSON_RejectsUnknownFields(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"headers-field", `{"name":"ops","subject":"relay.ops","headers":{"x-tenant":"abc"}}`},
		{"mode-field", `{"name":"ops","subject":"relay.ops","mode":"publish"}`},
		{"typo-field", `{"name":"ops","subject":"relay.ops","sjbject":"oops"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var tgt Target
			if err := json.Unmarshal([]byte(tc.raw), &tgt); err == nil {
				t.Fatalf("expected unknown-field rejection, got nil error and target=%+v", tgt)
			}
		})
	}
}

func TestTarget_UnmarshalJSON_AcceptsKnownFields(t *testing.T) {
	raw := `{"name":"ops","subject":"relay.ops"}`
	var tgt Target
	if err := json.Unmarshal([]byte(raw), &tgt); err != nil {
		t.Fatalf("expected success on known fields, got %v", err)
	}
	if tgt.Name != "ops" || tgt.Subject != "relay.ops" {
		t.Fatalf("unexpected target: %+v", tgt)
	}
}
