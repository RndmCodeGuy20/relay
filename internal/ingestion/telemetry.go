package ingestion

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

const (
	ResultSuccess           = "success"
	ResultFailureBadBody    = "failure_bad_body"
	ResultFailureValidation = "failure_validation"
	ResultFailureDuplicate  = "failure_duplicate"
	ResultFailureInternal   = "failure_internal"
)

var (
	ingestionTracer = otel.Tracer("relay/internal/ingestion")
	ingestionMeter  = otel.Meter("relay/internal/ingestion")

	ingestRequestsTotal metric.Int64Counter
	ingestResultsTotal  metric.Int64Counter
	ingestLatencyMs     metric.Float64Histogram
	dbWriteLatencyMs    metric.Float64Histogram
)

func init() {
	ingestRequestsTotal = mustInt64Counter(
		"relay.ingestion.requests.total",
		"Total number of ingestion requests received.",
		"1",
	)

	ingestResultsTotal = mustInt64Counter(
		"relay.ingestion.results.total",
		"Total number of ingestion outcomes by result.",
		"1",
	)

	ingestLatencyMs = mustFloat64Histogram(
		"relay.ingestion.latency.ms",
		"End-to-end ingestion latency in milliseconds.",
		"ms",
	)

	dbWriteLatencyMs = mustFloat64Histogram(
		"relay.ingestion.db_write.latency.ms",
		"Outbox write latency in milliseconds.",
		"ms",
	)
}

func mustInt64Counter(name, description, unit string) metric.Int64Counter {
	counter, err := ingestionMeter.Int64Counter(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	if err == nil {
		return counter
	}

	fallbackMeter := noop.MeterProvider{}.Meter("relay/internal/ingestion")
	fallbackCounter, fallbackErr := fallbackMeter.Int64Counter(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	if fallbackErr != nil {
		return noop.Int64Counter{}
	}

	return fallbackCounter
}

func mustFloat64Histogram(name, description, unit string) metric.Float64Histogram {
	histogram, err := ingestionMeter.Float64Histogram(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	if err == nil {
		return histogram
	}

	fallbackMeter := noop.MeterProvider{}.Meter("relay/internal/ingestion")
	fallbackHistogram, fallbackErr := fallbackMeter.Float64Histogram(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	if fallbackErr != nil {
		return noop.Float64Histogram{}
	}

	return fallbackHistogram
}

func recordIngestRequest(ctx context.Context) {
	ingestRequestsTotal.Add(ctx, 1)
}

func recordIngestResult(ctx context.Context, result string) {
	ingestResultsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func recordIngestLatency(ctx context.Context, d time.Duration) {
	ingestLatencyMs.Record(ctx, float64(d.Milliseconds()))
}

func recordDBWriteLatency(ctx context.Context, d time.Duration) {
	dbWriteLatencyMs.Record(ctx, float64(d.Milliseconds()))
}

func startIngestionSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ingestionTracer.Start(ctx, name, opts...)
}
