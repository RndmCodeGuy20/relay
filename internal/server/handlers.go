package server

import (
	"net/http"

	"go.uber.org/zap"
	relayLogger "rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/postgres"
)

// handleHealth returns a simple health check response.
func handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// handleGetUsers demonstrates context propagation to the DB layer.
func handleGetUsers(db *postgres.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// r.Context() contains the trace IDs, request timeouts, base context, AND the logger.
		ctx := r.Context()

		reqLogger := relayLogger.FromContext(ctx)

		// Pass the request context directly to the DB call.
		err := db.Pool().Ping(ctx)
		if err != nil {
			reqLogger.Error("db ping failed", zap.Error(err))
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"users":[]}`))
	}
}
