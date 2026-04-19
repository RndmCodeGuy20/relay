package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"rndmcodeguy.in/relay/internal/config"
	"rndmcodeguy.in/relay/internal/ingestion"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/otel"
	"rndmcodeguy.in/relay/internal/outbox"
	"rndmcodeguy.in/relay/internal/postgres"
	"rndmcodeguy.in/relay/internal/relay"
	"rndmcodeguy.in/relay/internal/server"
	"rndmcodeguy.in/relay/internal/stream"
)

var (
	Env        string = "dev"
	Version    string
	BuildTime  string
	CommitHash string
	Author     string
)

func normalizeOTLPEndpoint(e string) string {
	if e == "" {
		return e
	}
	e = strings.TrimSpace(e)
	// strip scheme
	e = strings.TrimPrefix(e, "http://")
	e = strings.TrimPrefix(e, "https://")
	// remove any path after host:port
	if idx := strings.Index(e, "/"); idx != -1 {
		e = e[:idx]
	}
	return e
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// 1. Create a root context that cancels on interrupt signals (SIGINT, SIGTERM)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	otelEndpoint := cfg.Otel.ExporterEndpoint

	// Normalize to host:port (strip scheme/path) to avoid passing a URL to gRPC exporter
	otelEndpoint = normalizeOTLPEndpoint(otelEndpoint)

	shutdown, err := otel.InitOtel(ctx, "relay-service", cfg.Env, otelEndpoint)
	if err != nil {
		log.Fatalf("failed to initialize OpenTelemetry: %v", err)
	}
	defer func() {
		otelShutdownCtx, otelShutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer otelShutdownCancel()

		if err := shutdown(otelShutdownCtx); err != nil {
			log.Printf("failed to shutdown OpenTelemetry securely: %v", err)
		}
	}()

	baseLogger := logger.New(getLoggerConfig(cfg))

	l := baseLogger.With(
		zap.String("version", Version),
		zap.String("build_time", BuildTime),
		zap.String("commit_hash", CommitHash),
		zap.String("author", Author),
	)

	// Log the resolved OTEL endpoint so it's easy to debug misconfigured env vars.
	// This now goes through the OTel log provider as well.
	l.Info("using OTEL endpoint", zap.String("endpoint", otelEndpoint))
	ctx = logger.WithLogger(ctx, l)

	l.Info("starting relay service",
		zap.Int("port", cfg.Server.Port),
	)

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DBName,
	)

	// 2. Pass context to initialization that might do I/O or take time
	db, err := postgres.New(ctx,
		postgres.Config{
			DSN:             dsn,
			MaxConns:        int32(cfg.Postgres.MaxConns),
			MinConns:        int32(cfg.Postgres.MinConns),
			MaxConnLifetime: time.Duration(cfg.Postgres.MaxConnLifetimeSec) * time.Second,
			MaxConnIdleTime: time.Duration(cfg.Postgres.MaxConnIdleTimeSec) * time.Second,
			HealthTimeout:   time.Duration(cfg.Postgres.HealthTimeoutSec) * time.Second,
		})
	if err != nil {
		l.Fatal("failed to initialize postgres", zap.Error(err))
	}

	outboxWriter := outbox.NewPostgresOutboxWriter()
	ingestionService := ingestion.NewIngestionService(outboxWriter, db.Pool())
	ingestionHandler := ingestion.NewIngestionHandler(ingestionService)
	streamPublisher := stream.NewLoggingStream()
	replicationDSN := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable&replication=database",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DBName,
	)

	relayService, err := relay.NewRelayService(ctx, streamPublisher, db.Pool(), replicationDSN, cfg.Relay)
	if err != nil {
		l.Fatal("failed to initialize relay service", zap.Error(err))
	}

	go func() {
		if err := relayService.Run(ctx, 0); err != nil && err != context.Canceled {
			l.Error("relay service exited", zap.Error(err))
		}
	}()

	// 3. Initialize and start HTTP server
	srv := server.New(ctx, cfg.Server, l, func(r chi.Router) {
		r.Route("/v1", func(rr chi.Router) {
			ingestion.RegisterRoutes(rr, ingestionHandler)
		})
	})

	go func() {
		if err := srv.Start(); err != nil {
			l.Fatal("server crash", zap.Error(err))
		}
	}()

	// 4. Wait for interrupt signal (graceful shutdown trigger)
	<-ctx.Done()
	l.Info("received shutdown signal, initiating graceful shutdown")

	// 5. Create a shutdown context with a hard timeout. We load shred remaining connections.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		l.Warn("server forced to shutdown", zap.Error(err))
	}

	db.Close() // Ignore error for now
	l.Info("shutdown complete")
}

func getLoggerConfig(cfg *config.Config) logger.Config {
	switch cfg.Env {
	case "dev":
		return logger.Config{
			ServiceName: "relay",
			Environment: cfg.Env,
			Level:       zapcore.DebugLevel,
			EnableOTel:  true,
		}
	case "prod":
		return logger.Config{
			ServiceName: "relay",
			Environment: cfg.Env,
			Level:       zapcore.InfoLevel,
			EnableOTel:  true,
		}
	default:
		log.Fatalf("invalid environment: %s", cfg.Env)
		return logger.Config{}
	}
}
