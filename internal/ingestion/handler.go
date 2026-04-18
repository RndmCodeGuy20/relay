package ingestion

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/apperror"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/outbox"
	"rndmcodeguy.in/relay/internal/respond"
	"rndmcodeguy.in/relay/internal/validate"
)

type IngestionHandler struct {
	service IngestionService
}

func NewIngestionHandler(service IngestionService) *IngestionHandler {
	return &IngestionHandler{
		service: service,
	}
}

func (h *IngestionHandler) HandleIngest() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, span := startIngestionSpan(r.Context(), "ingestion.handle")
		start := time.Now()
		result := ResultFailureInternal

		defer func() {
			span.SetAttributes(attribute.String("ingestion.result", result))
			recordIngestLatency(ctx, time.Since(start))
			recordIngestResult(ctx, result)
			span.End()
		}()

		recordIngestRequest(ctx)

		reqLogger := logger.FromContext(ctx)

		var req IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			result = ResultFailureBadBody
			span.RecordError(err)
			span.SetStatus(codes.Error, "request body decode failed")
			reqLogger.Warn("invalid ingest request", zap.Error(err))
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		span.SetAttributes(
			attribute.String("ingestion.event_type", req.EventType),
			attribute.String("ingestion.source", req.Source),
		)

		if err := req.Validate(); err != nil {
			result = ResultFailureValidation
			span.RecordError(err)
			span.SetStatus(codes.Error, "request validation failed")
			var ve validate.ValidationErrors
			if errors.As(err, &ve) {
				respond.ValidationError(w, ve.Fields())
				return
			}
			respond.BadRequest(w, "BAD_REQUEST", "invalid request")
			return
		}

		event := outbox.OutboxEventInsert{
			EventID:       req.EventID,
			EventType:     req.EventType,
			Payload:       req.Payload,
			SchemaVersion: req.SchemaVersion,
			Source:        req.Source,
			OccurredAt:    req.OccurredAt,
			ReceivedAt:    time.Now().UTC(),
		}

		// call ingestion service to write to outbox
		reqLogger.Info("ingest request validated, writing to outbox", zap.String("event_id", event.EventID.String()), zap.String("event_type", event.EventType))
		if err := h.service.Ingest(ctx, event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "service ingestion failed")

			var apperr *apperror.AppError
			if errors.As(err, &apperr) {
				if apperr.Code == apperror.CodeDuplicate {
					result = ResultFailureDuplicate
				} else {
					result = ResultFailureInternal
				}

				reqLogger.Error("application error during ingestion", zap.Error(apperr), zap.String("event_id", event.EventID.String()))
				respond.Error(w, apperr.Status, string(apperr.Code), apperr.Message)
				return
			}

			result = ResultFailureInternal
			reqLogger.Error("failed to ingest event", zap.Error(err), zap.String("event_id", event.EventID.String()))
			respond.InternalError(w, "INGESTION_ERROR", "failed to ingest event")
			return
		}

		result = ResultSuccess
		span.SetStatus(codes.Ok, "ingestion accepted")

		respond.Accepted(w, map[string]string{
			"event_id": req.EventID.String(),
		})
	}
}
