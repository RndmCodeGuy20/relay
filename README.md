# Relay

Event relay service using the outbox pattern for reliable event publication.

## Overview

Relay is a Go-based service that ingests events and reliably publishes them using the transactional outbox pattern. It handles event validation, persistence, and asynchronous dispatch with observability built-in.

**Version:** 0.1.0  
**Author:** Shantanu Mane  
**License:** MIT

## Project Structure

```
cmd/              # Entry points
├── main.go       # Server application
└── migrate/      # Database migration CLI

internal/         # Private packages
├── apperror/     # Error handling
├── config/       # Configuration loading
├── dispatch/     # Event dispatch logic
├── ingestion/    # Event ingestion handler & validation
├── logger/       # Structured logging with OTEL integration
├── otel/         # OpenTelemetry setup
├── outbox/       # Outbox pattern implementation
├── postgres/     # Database layer
├── respond/      # HTTP response utilities
├── rule/         # Event routing rules
├── server/       # HTTP server & middleware
├── stream/       # Event streams
├── validate/     # Validation logic
└── worker/       # Background workers

migrations/       # SQL migrations (version-controlled)
deploy/           # Deployment configs (Docker Compose, Prometheus, Grafana, Loki)
taskfiles/        # Task automation configs
```

## Getting Started

### Prerequisites

- Go 1.25+
- PostgreSQL
- `task` CLI (for task automation)
- Docker & Docker Compose (for deployment)

### Development

1. **Clone & setup:**
   ```bash
   git clone github.com/rndmcodeguy20/relay
   cd relay
   ```

2. **Environment:**
   ```bash
   cp .env.example .env
   ```

3. **Database:**
   ```bash
   task db:up          # Run migrations
   ```

4. **Run server:**
   ```bash
   task dev:run        # or: go run cmd/main.go
   ```

5. **Run tests:**
   ```bash
   task test:unit      # Unit tests
   task test:integration  # Integration tests
   ```

## Building

- **Debug:** `task build:dev` — unoptimized, includes symbols
- **Production:** `task build:prod` — optimized, symbols stripped
- **Clean:** `task build:clean` — remove build artifacts

Binary output: `build/relay`

## Deployment

See [deploy/README.md](deploy/README.md) for Docker Compose setup including:
- PostgreSQL
- OTEL Collector
- Prometheus
- Grafana
- Loki (logs)

## API

Ingest events via HTTP POST to `/ingest`. See [requests/ingest.http](requests/ingest.http) for examples.

## Configuration

Load from environment variables or `relay.toml`. See `internal/config/loader.go`.

## Observability

- **Logging:** Structured logs via Zap with pretty printing in dev, JSON in prod
- **Tracing:** OpenTelemetry traces exported to stdout/collector
- **Metrics:** Prometheus metrics via OTEL SDK

## Testing

- Integration tests use test database fixtures
- See `taskfiles/test.yml` for test runner config
