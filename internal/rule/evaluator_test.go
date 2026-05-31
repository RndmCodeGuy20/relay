package rule

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestEvaluateEvent_FanOutDeterministicOrder(t *testing.T) {
	event := InputEvent{
		Source:    "checkout",
		EventType: "order.created",
		Payload:   json.RawMessage(`{"order":{"total":150,"tags":["priority","gift"]}}`),
	}

	ruleHigh := Rule{
		ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Name:      "high-priority",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Priority:  100,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.total", Op: OperatorEq, Value: json.RawMessage(`150`)}}},
		Targets: []Target{
			{Name: "billing", Subject: "relay.billing"},
			{Name: "analytics", Subject: "relay.analytics"},
		},
	}

	ruleLow := Rule{
		ID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		Name:      "low-priority",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Priority:  10,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.tags", Op: OperatorContains, Value: json.RawMessage(`"priority"`)}}},
		Targets:   []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	result, err := EvaluateEvent(event, []Rule{ruleLow, ruleHigh})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.NoMatch {
		t.Fatalf("expected matches, got no_match=true")
	}

	if len(result.Intents) != 3 {
		t.Fatalf("expected 3 intents, got %d", len(result.Intents))
	}

	if result.Intents[0].RuleName != "high-priority" || result.Intents[0].Target.Name != "billing" {
		t.Fatalf("unexpected first intent ordering: %+v", result.Intents[0])
	}
	if result.Intents[1].RuleName != "high-priority" || result.Intents[1].Target.Name != "analytics" {
		t.Fatalf("unexpected second intent ordering: %+v", result.Intents[1])
	}
	if result.Intents[2].RuleName != "low-priority" || result.Intents[2].Target.Name != "ops" {
		t.Fatalf("unexpected third intent ordering: %+v", result.Intents[2])
	}

	if result.Intents[0].Target.Subject == "" {
		t.Fatalf("expected target subject to be populated")
	}
}

func TestEvaluateEvent_NoMatch(t *testing.T) {
	event := InputEvent{
		Source:    "checkout",
		EventType: "order.created",
		Payload:   json.RawMessage(`{"order":{"total":50}}`),
	}

	rule := Rule{
		ID:        uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		Name:      "high-value",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Priority:  1,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.total", Op: OperatorEq, Value: json.RawMessage(`150`)}}},
		Targets:   []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	result, err := EvaluateEvent(event, []Rule{rule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.NoMatch {
		t.Fatalf("expected no_match=true")
	}
	if len(result.Intents) != 0 {
		t.Fatalf("expected no intents, got %d", len(result.Intents))
	}
}

func TestEvaluateEvent_SkipsInvalidAndDisabledRules(t *testing.T) {
	event := InputEvent{
		Source:    "checkout",
		EventType: "order.created",
		Payload:   json.RawMessage(`{"order":{"total":150}}`),
	}

	disabled := Rule{
		ID:        uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		Name:      "disabled",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   false,
		Priority:  99,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.total", Op: OperatorEq, Value: json.RawMessage(`150`)}}},
		Targets:   []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	invalid := Rule{
		ID:        uuid.MustParse("55555555-5555-5555-5555-555555555555"),
		Name:      "invalid",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Priority:  98,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.total", Op: "bad_op", Value: json.RawMessage(`150`)}}},
		Targets:   []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	valid := Rule{
		ID:        uuid.MustParse("66666666-6666-6666-6666-666666666666"),
		Name:      "valid",
		Source:    "checkout",
		EventType: "order.created",
		Enabled:   true,
		Priority:  1,
		Version:   1,
		Selector:  Selector{All: []Predicate{{Path: "order.total", Op: OperatorEq, Value: json.RawMessage(`150`)}}},
		Targets:   []Target{{Name: "ops", Subject: "relay.ops"}},
	}

	result, err := EvaluateEvent(event, []Rule{disabled, invalid, valid})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Intents) != 1 {
		t.Fatalf("expected 1 intent, got %d", len(result.Intents))
	}
	if result.Intents[0].RuleName != "valid" {
		t.Fatalf("expected valid rule to match, got %s", result.Intents[0].RuleName)
	}
}

func TestEvaluateEvent_InvalidPayload(t *testing.T) {
	event := InputEvent{Source: "checkout", EventType: "order.created", Payload: json.RawMessage(`{"bad":`)}
	_, err := EvaluateEvent(event, nil)
	if err == nil {
		t.Fatalf("expected payload validation error")
	}
}
