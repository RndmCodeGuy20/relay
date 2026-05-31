package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestMockStreamPublishToSubjectRecords(t *testing.T) {
	m := NewMockStream()
	e := &Publish{
		OutboxID:        uuid.New(),
		ProducerEventID: uuid.New(),
		Source:          "orders",
		EventType:       "order.created",
	}

	if err := m.PublishToSubject(context.Background(), "tenants.acme.orders", "task-1", e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := m.PublishedToSubjects()
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got[0].Subject != "tenants.acme.orders" {
		t.Fatalf("unexpected subject: %q", got[0].Subject)
	}
	if got[0].DedupKey != "task-1" {
		t.Fatalf("unexpected dedup key: %q", got[0].DedupKey)
	}
	if got[0].Event != e {
		t.Fatalf("expected event pointer to match")
	}
}

func TestMockStreamPublishToSubjectAccumulatesInOrder(t *testing.T) {
	m := NewMockStream()
	ctx := context.Background()

	e1 := &Publish{OutboxID: uuid.New()}
	e2 := &Publish{OutboxID: uuid.New()}
	e3 := &Publish{OutboxID: uuid.New()}

	if err := m.PublishToSubject(ctx, "s.a", "k1", e1); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m.PublishToSubject(ctx, "s.b", "k2", e2); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m.PublishToSubject(ctx, "s.c", "k3", e3); err != nil {
		t.Fatalf("err: %v", err)
	}

	got := m.PublishedToSubjects()
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	wantSubjects := []string{"s.a", "s.b", "s.c"}
	wantKeys := []string{"k1", "k2", "k3"}
	for i, entry := range got {
		if entry.Subject != wantSubjects[i] {
			t.Fatalf("entry %d: subject %q want %q", i, entry.Subject, wantSubjects[i])
		}
		if entry.DedupKey != wantKeys[i] {
			t.Fatalf("entry %d: dedup key %q want %q", i, entry.DedupKey, wantKeys[i])
		}
	}
}

func TestMockStreamPublishAndPublishToSubjectAreSeparate(t *testing.T) {
	m := NewMockStream()
	ctx := context.Background()

	hubEvent := &Publish{OutboxID: uuid.New()}
	targetEvent := &Publish{OutboxID: uuid.New()}

	if err := m.Publish(ctx, hubEvent); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m.PublishToSubject(ctx, "tenants.acme.orders", "task-1", targetEvent); err != nil {
		t.Fatalf("err: %v", err)
	}

	hub := m.Published()
	if len(hub) != 1 || hub[0] != hubEvent {
		t.Fatalf("expected only hub event recorded in Published(), got %+v", hub)
	}

	targets := m.PublishedToSubjects()
	if len(targets) != 1 || targets[0].Event != targetEvent {
		t.Fatalf("expected only target event recorded in PublishedToSubjects(), got %+v", targets)
	}
}

func TestMockStreamPublishToSubjectReturnsConfiguredError(t *testing.T) {
	m := NewMockStream()
	want := errors.New("boom")
	m.SetError(want)

	err := m.PublishToSubject(context.Background(), "s.a", "k", &Publish{OutboxID: uuid.New()})
	if !errors.Is(err, want) {
		t.Fatalf("expected error %v, got %v", want, err)
	}

	// Entry should still be recorded even when an error is returned.
	if len(m.PublishedToSubjects()) != 1 {
		t.Fatalf("expected entry to be recorded despite error")
	}
}
