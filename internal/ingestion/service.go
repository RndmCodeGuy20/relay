package ingestion

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"rndmcodeguy.in/relay/internal/apperror"
	"rndmcodeguy.in/relay/internal/outbox"
	"rndmcodeguy.in/relay/internal/postgres"
)

type Service interface {
	Ingest(ctx context.Context, event outbox.EventInsert) error
}

type ServiceImpl struct {
	writer outbox.Writer
	pool   *pgxpool.Pool
}

func NewIngestionService(writer outbox.Writer, pool *pgxpool.Pool) *ServiceImpl {
	return &ServiceImpl{
		writer: writer,
		pool:   pool,
	}
}

func (s *ServiceImpl) Ingest(ctx context.Context, event outbox.EventInsert) error {
	ctx, span := startIngestionSpan(ctx, "ingestion.service.ingest",
		trace.WithAttributes(
			attribute.String("ingestion.event_id", event.EventID.String()),
			attribute.String("ingestion.event_type", event.EventType),
			attribute.String("ingestion.source", event.Source),
		),
	)
	defer span.End()

	return postgres.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		writeStart := time.Now()
		writeCtx, writeSpan := startIngestionSpan(ctx, "ingestion.service.write_outbox")
		defer writeSpan.End()

		err := s.writer.Write(writeCtx, tx, event)
		recordDBWriteLatency(writeCtx, time.Since(writeStart))

		if err != nil {
			span.RecordError(err)
			writeSpan.RecordError(err)

			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				if pgErr.Code == pgerrcode.UniqueViolation {
					span.SetAttributes(attribute.String("ingestion.result", ResultFailureDuplicate))
					span.SetStatus(codes.Error, "duplicate event")
					writeSpan.SetStatus(codes.Error, "outbox write duplicate")
					// unique constraint violation
					return apperror.New(
						apperror.CodeDuplicate,
						"event already exists",
						http.StatusConflict,
						err,
					)
				}
			}

			span.SetAttributes(attribute.String("ingestion.result", ResultFailureInternal))
			span.SetStatus(codes.Error, "outbox write failed")
			writeSpan.SetStatus(codes.Error, "outbox write failed")

			return apperror.New(
				apperror.CodeInternal,
				"failed to write event",
				http.StatusInternalServerError,
				err,
			)
		}

		span.SetAttributes(attribute.String("ingestion.result", ResultSuccess))
		span.SetStatus(codes.Ok, "outbox write succeeded")
		writeSpan.SetStatus(codes.Ok, "outbox write succeeded")
		return nil
	})
}
