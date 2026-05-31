package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"rndmcodeguy.in/relay/internal/rule"
	"rndmcodeguy.in/relay/internal/stream"
)

// stubCache is a test double for the consumer's RuleSource.
type stubCache struct {
	rules []rule.Rule
}

func (s *stubCache) Get(source, eventType string) []rule.Rule {
	out := make([]rule.Rule, 0, len(s.rules))
	for _, r := range s.rules {
		if r.Source == source && r.EventType == eventType {
			out = append(out, r)
		}
	}
	return out
}

func newTestConsumer(rules []rule.Rule) *Consumer {
	return New(nil, nil, &stubCache{rules: rules}, Config{
		Workers:       1,
		BatchSize:     10,
		ConsumerGroup: "test",
	})
}

func wireMessage(t *testing.T, body map[string]any) *stream.Message {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return stream.NewMessage(raw)
}

func validWireBody() map[string]any {
	return map[string]any{
		"outbox_id":    "22222222-2222-2222-2222-222222222222",
		"event_id":     "33333333-3333-3333-3333-333333333333",
		"source":       "svc",
		"event_type":   "e",
		"payload":      map[string]any{"user": map[string]any{"id": 1}},
		"lsn":          "0/0",
		"sequence":     1,
		"published_at": "2024-01-01T00:00:00Z",
	}
}

// All these tests verify the short-circuit paths in processMessage that
// return before touching the database, so a nil pool is safe.

func TestProcessMessage_NoRules(t *testing.T) {
	c := newTestConsumer(nil)

	err := c.processMessage(context.Background(), wireMessage(t, validWireBody()))
	if err != nil {
		t.Fatalf("expected nil error when no rules registered, got %v", err)
	}
}

func TestProcessMessage_InvalidJSON(t *testing.T) {
	c := newTestConsumer(nil)
	msg := stream.NewMessage([]byte("not json"))

	if err := c.processMessage(context.Background(), msg); err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestProcessMessage_MissingSourceOrEventType(t *testing.T) {
	c := newTestConsumer(nil)

	body := validWireBody()
	delete(body, "source")

	if err := c.processMessage(context.Background(), wireMessage(t, body)); err != nil {
		t.Fatalf("expected silent skip when source missing, got %v", err)
	}
}

func TestProcessMessage_NoMatch(t *testing.T) {
	rules := []rule.Rule{{
		ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Name:      "admin-only",
		Source:    "svc",
		EventType: "e",
		Enabled:   true,
		Priority:  1,
		Version:   1,
		Selector: rule.Selector{
			All: []rule.Predicate{{Path: "admin", Op: rule.OperatorExists}},
		},
		Targets: []rule.Target{{Name: "t1", Subject: "sub.1"}},
	}}
	c := newTestConsumer(rules)

	// validWireBody payload has "user" but not "admin" — no match expected.
	if err := c.processMessage(context.Background(), wireMessage(t, validWireBody())); err != nil {
		t.Fatalf("expected nil error on no-match, got %v", err)
	}
}

func TestProcessMessage_InvalidOutboxID(t *testing.T) {
	rules := []rule.Rule{{
		ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Name:      "any",
		Source:    "svc",
		EventType: "e",
		Enabled:   true,
		Priority:  1,
		Version:   1,
		Selector: rule.Selector{
			All: []rule.Predicate{{Path: "user", Op: rule.OperatorExists}},
		},
		Targets: []rule.Target{{Name: "t1", Subject: "sub.1"}},
	}}
	c := newTestConsumer(rules)

	body := validWireBody()
	body["outbox_id"] = "not-a-uuid"

	if err := c.processMessage(context.Background(), wireMessage(t, body)); err == nil {
		t.Fatal("expected error for invalid outbox_id")
	}
}

func TestProcessMessage_OptionalProducerEventID(t *testing.T) {
	// Producer event id is optional on the wire (e.g., legacy sources that
	// haven't been wired up yet). The consumer should accept its absence.
	rules := []rule.Rule{{
		ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Name:      "no-match-rule",
		Source:    "svc",
		EventType: "e",
		Enabled:   true,
		Priority:  1,
		Version:   1,
		Selector: rule.Selector{
			All: []rule.Predicate{{Path: "admin", Op: rule.OperatorExists}},
		},
		Targets: []rule.Target{{Name: "t1", Subject: "sub.1"}},
	}}
	c := newTestConsumer(rules)

	body := validWireBody()
	delete(body, "event_id")

	if err := c.processMessage(context.Background(), wireMessage(t, body)); err != nil {
		t.Fatalf("expected nil (no-match short-circuit) with missing event_id, got %v", err)
	}
}
