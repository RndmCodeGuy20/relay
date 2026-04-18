package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var outboxTracer = otel.Tracer("relay/internal/outbox")

type OutboxWriter interface {
	Write(ctx context.Context, tx pgx.Tx, event OutboxEventInsert) error
}

type PostgresOutboxWriter struct{}

func NewPostgresOutboxWriter() *PostgresOutboxWriter {
	return &PostgresOutboxWriter{}
}

func (w *PostgresOutboxWriter) Write(ctx context.Context, tx pgx.Tx, event OutboxEventInsert) error {
	ctx, span := outboxTracer.Start(ctx, "outbox.write",
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "INSERT"),
			attribute.String("db.sql.table", "outbox_events"),
		),
	)
	defer span.End()

	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (event_id, event_type, payload, schema_version, source, occurred_at, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, event.EventID, event.EventType, event.Payload, event.SchemaVersion, event.Source, event.OccurredAt, event.ReceivedAt)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "outbox insert failed")
		return err
	}

	span.SetStatus(codes.Ok, "outbox insert succeeded")

	return nil
}
