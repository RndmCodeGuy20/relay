package respond

import (
	"encoding/json"
	"net/http"
)

type Envelope[T any] struct {
	Data  T        `json:"data,omitempty"`
	Error *ErrBody `json:"error,omitempty"`
}

type ErrBody struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Fields  []FieldError `json:"fields,omitempty"`
}

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func write(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func OK[T any](w http.ResponseWriter, data T) {
	write(w, http.StatusOK, Envelope[T]{Data: data})
}

func Accepted[T any](w http.ResponseWriter, data T) {
	write(w, http.StatusAccepted, Envelope[T]{Data: data})
}

func Error(w http.ResponseWriter, status int, code string, message string) {
	write(w, status, Envelope[struct{}]{
		Error: &ErrBody{Code: code, Message: message},
	})
}

func BadRequest(w http.ResponseWriter, code string, message string) {
	write(w, http.StatusBadRequest, Envelope[struct{}]{
		Error: &ErrBody{Code: code, Message: message},
	})
}

func InternalError(w http.ResponseWriter, code string, message string) {
	write(w, http.StatusInternalServerError, Envelope[struct{}]{
		Error: &ErrBody{Code: code, Message: message},
	})
}

func ValidationError(w http.ResponseWriter, fields []FieldError) {
	write(w, http.StatusUnprocessableEntity, Envelope[struct{}]{
		Error: &ErrBody{
			Code:    "VALIDATION_ERROR",
			Message: "request validation failed",
			Fields:  fields,
		},
	})
}
