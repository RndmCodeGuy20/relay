//go:build integration

package ingestion_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"rndmcodeguy.in/relay/internal/apperror"
	"rndmcodeguy.in/relay/internal/ingestion"
	"rndmcodeguy.in/relay/internal/outbox"
)

func TestIngest_PersistsEvent(t *testing.T) {
	pool := openIntegrationPool(t)
	resetOutboxTables(t, pool)

	svc := ingestion.NewIngestionService(outbox.NewPostgresOutboxWriter(), pool)

	eventID := uuid.New()
	payload := json.RawMessage(`{"order_id":"A-1001","status":"created"}`)
	receivedAt := time.Now().UTC().Truncate(time.Microsecond)

	event := outbox.EventInsert{
		EventID:       eventID,
		EventType:     "order.created",
		Payload:       payload,
		SchemaVersion: "v1",
		Source:        "integration-test",
		OccurredAt:    nil,
		ReceivedAt:    receivedAt,
	}

	if err := svc.Ingest(context.Background(), event); err != nil {
		t.Fatalf("ingest event: %v", err)
	}

	var (
		gotEventID       uuid.UUID
		gotEventType     string
		gotSource        string
		gotSchemaVersion string
		gotPayload       []byte
		gotOccurredAt    *time.Time
		gotReceivedAt    time.Time
	)

	err := pool.QueryRow(context.Background(), `
		SELECT event_id, event_type, source, schema_version, payload, occurred_at, received_at
		FROM outbox_events
		WHERE source = $1 AND event_id = $2
	`, event.Source, event.EventID).Scan(
		&gotEventID,
		&gotEventType,
		&gotSource,
		&gotSchemaVersion,
		&gotPayload,
		&gotOccurredAt,
		&gotReceivedAt,
	)
	if err != nil {
		t.Fatalf("query inserted event: %v", err)
	}

	if gotEventID != event.EventID {
		t.Fatalf("event_id mismatch: got %s, want %s", gotEventID, event.EventID)
	}
	if gotEventType != event.EventType {
		t.Fatalf("event_type mismatch: got %s, want %s", gotEventType, event.EventType)
	}
	if gotSource != event.Source {
		t.Fatalf("source mismatch: got %s, want %s", gotSource, event.Source)
	}
	if gotSchemaVersion != event.SchemaVersion {
		t.Fatalf("schema_version mismatch: got %s, want %s", gotSchemaVersion, event.SchemaVersion)
	}
	var gotPayloadJSON map[string]any
	if err := json.Unmarshal(gotPayload, &gotPayloadJSON); err != nil {
		t.Fatalf("unmarshal stored payload: %v", err)
	}

	var expectedPayloadJSON map[string]any
	if err := json.Unmarshal(event.Payload, &expectedPayloadJSON); err != nil {
		t.Fatalf("unmarshal expected payload: %v", err)
	}

	if !reflect.DeepEqual(gotPayloadJSON, expectedPayloadJSON) {
		t.Fatalf("payload mismatch: got %+v, want %+v", gotPayloadJSON, expectedPayloadJSON)
	}
	if gotOccurredAt != nil {
		t.Fatalf("occurred_at mismatch: got %v, want nil", gotOccurredAt)
	}
	if !gotReceivedAt.Equal(event.ReceivedAt) {
		t.Fatalf("received_at mismatch: got %s, want %s", gotReceivedAt, event.ReceivedAt)
	}
}

func TestIngest_ReturnsDuplicateAppError(t *testing.T) {
	pool := openIntegrationPool(t)
	resetOutboxTables(t, pool)

	svc := ingestion.NewIngestionService(outbox.NewPostgresOutboxWriter(), pool)

	event := outbox.EventInsert{
		EventID:       uuid.New(),
		EventType:     "order.updated",
		Payload:       json.RawMessage(`{"order_id":"A-1001","status":"paid"}`),
		SchemaVersion: "v1",
		Source:        "integration-test",
		OccurredAt:    nil,
		ReceivedAt:    time.Now().UTC().Truncate(time.Microsecond),
	}

	if err := svc.Ingest(context.Background(), event); err != nil {
		t.Fatalf("first ingest should succeed: %v", err)
	}

	err := svc.Ingest(context.Background(), event)
	if err == nil {
		t.Fatal("second ingest should fail with duplicate error")
	}

	var appErr *apperror.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected AppError, got %T (%v)", err, err)
	}
	if appErr.Code != apperror.CodeDuplicate {
		t.Fatalf("unexpected error code: got %s, want %s", appErr.Code, apperror.CodeDuplicate)
	}
	if appErr.Status != 409 {
		t.Fatalf("unexpected status: got %d, want %d", appErr.Status, 409)
	}
}

func TestIngest_PersistsOccurredAt(t *testing.T) {
	pool := openIntegrationPool(t)
	resetOutboxTables(t, pool)

	svc := ingestion.NewIngestionService(outbox.NewPostgresOutboxWriter(), pool)

	occurredAt := time.Now().UTC().Truncate(time.Microsecond)
	event := outbox.EventInsert{
		EventID:       uuid.New(),
		EventType:     "inventory.adjusted",
		Payload:       json.RawMessage(`{"sku":"ABC-123","delta":-3}`),
		SchemaVersion: "v1",
		Source:        "integration-test",
		OccurredAt:    &occurredAt,
		ReceivedAt:    time.Now().UTC().Truncate(time.Microsecond),
	}

	if err := svc.Ingest(context.Background(), event); err != nil {
		t.Fatalf("ingest event with occurred_at: %v", err)
	}

	var gotOccurredAt *time.Time
	err := pool.QueryRow(context.Background(), `
		SELECT occurred_at
		FROM outbox_events
		WHERE source = $1 AND event_id = $2
	`, event.Source, event.EventID).Scan(&gotOccurredAt)
	if err != nil {
		t.Fatalf("query occurred_at: %v", err)
	}

	if gotOccurredAt == nil {
		t.Fatal("occurred_at mismatch: got nil, want non-nil")
	}
	if !gotOccurredAt.Equal(occurredAt) {
		t.Fatalf("occurred_at mismatch: got %s, want %s", gotOccurredAt, occurredAt)
	}
}
