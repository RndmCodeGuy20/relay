package ingestion

import "github.com/go-chi/chi/v5"

func RegisterRoutes(r chi.Router, handler *Handler) {
	r.Post("/ingest", handler.HandleIngest())
}
