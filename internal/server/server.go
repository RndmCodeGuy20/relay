package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"rndmcodeguy.in/relay/internal/config"
	relayLogger "rndmcodeguy.in/relay/internal/logger"
)

type Server struct {
	server *http.Server
	logger *zap.Logger
	config config.ServerConfig
}

func New(ctx context.Context, cfg config.ServerConfig, l *zap.Logger, registerRoutes func(chi.Router)) *Server {
	mux := chi.NewRouter()

	mux.Use(middleware.RequestID) // Inject request ID into context
	mux.Use(middleware.RealIP)
	mux.Use(middleware.Recoverer) // Prevent panics from taking down the server
	mux.Use(otelchi.Middleware("relay-service", otelchi.WithChiRoutes(mux)))
	mux.Use(OTelLoggingMiddleware(l))
	mux.Use(middleware.Timeout(60 * time.Second))
	sem := semaphore.NewWeighted(cfg.MaxConcurrency)
	mux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := sem.Acquire(r.Context(), 1); err != nil {
				reqLogger := relayLogger.FromContext(r.Context())
				reqLogger.Warn("load shedding: too many concurrent requests", zap.Error(err))
				http.Error(w, "Service Unavailable: Too many requests", http.StatusServiceUnavailable)
				return
			}
			defer sem.Release(1)

			next.ServeHTTP(w, r)
		})
	})

	mux.Get("/health", handleHealth())

	mux.Route("/api", func(r chi.Router) {
		registerRoutes(r)
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(cfg.IdleTimeout) * time.Second,
		// Propagate the root context to every incoming HTTP request
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	return &Server{
		server: srv,
		logger: l,
		config: cfg,
	}
}

// Start runs the server in a blocking manner.
func (s *Server) Start() error {
	s.logger.Info("http server listening", zap.String("addr", s.server.Addr))
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server failed: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the server, waiting for active connections to finish.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down http server gracefully...")
	return s.server.Shutdown(ctx)
}
