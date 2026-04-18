package server

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	relayLogger "rndmcodeguy.in/relay/internal/logger"
)

// OTelLoggingMiddleware wraps an HTTP handler and logs request details along with OpenTelemetry Trace IDs.
func OTelLoggingMiddleware(logger *zap.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			// Extract trace and span IDs from the context
			ctx := r.Context()
			spanContext := trace.SpanFromContext(ctx).SpanContext()

			reqLogger := logger
			if spanContext.TraceID().IsValid() {
				reqLogger = reqLogger.With(zap.String("trace_id", spanContext.TraceID().String()))
			}
			if spanContext.SpanID().IsValid() {
				reqLogger = reqLogger.With(zap.String("span_id", spanContext.SpanID().String()))
			}

			// Attach the context-aware logger
			ctx = relayLogger.WithLogger(ctx, reqLogger)
			r = r.WithContext(ctx)

			next.ServeHTTP(ww, r)

			reqLogger.Info("http request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", ww.Status()),
				zap.Duration("latency", time.Since(start)),
			)
		})
	}
}
