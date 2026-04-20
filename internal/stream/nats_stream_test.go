package stream

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"rndmcodeguy.in/relay/internal/config"
)

func TestValidateNATSConfig(t *testing.T) {
	base := config.NATSConfig{
		URL:               "nats://localhost:4222",
		StreamName:        "RELAY",
		Subject:           "relay.events",
		ConnectTimeoutSec: 5,
		PublishTimeoutSec: 5,
		MaxReconnects:     -1,
		ReconnectWaitMs:   2000,
		DrainTimeoutSec:   10,
	}

	tests := []struct {
		name    string
		mutate  func(cfg *config.NATSConfig)
		wantErr bool
	}{
		{name: "valid", mutate: func(_ *config.NATSConfig) {}, wantErr: false},
		{name: "missing url", mutate: func(cfg *config.NATSConfig) { cfg.URL = "" }, wantErr: true},
		{name: "missing stream", mutate: func(cfg *config.NATSConfig) { cfg.StreamName = "" }, wantErr: true},
		{name: "missing subject", mutate: func(cfg *config.NATSConfig) { cfg.Subject = "" }, wantErr: true},
		{name: "invalid connect timeout", mutate: func(cfg *config.NATSConfig) { cfg.ConnectTimeoutSec = 0 }, wantErr: true},
		{name: "invalid publish timeout", mutate: func(cfg *config.NATSConfig) { cfg.PublishTimeoutSec = 0 }, wantErr: true},
		{name: "invalid reconnect wait", mutate: func(cfg *config.NATSConfig) { cfg.ReconnectWaitMs = 0 }, wantErr: true},
		{name: "invalid drain timeout", mutate: func(cfg *config.NATSConfig) { cfg.DrainTimeoutSec = 0 }, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)

			err := validateNATSConfig(cfg)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("did not expect error, got %v", err)
			}
		})
	}
}

func TestSubjectMatchesPattern(t *testing.T) {
	tests := []struct {
		pattern string
		subject string
		want    bool
	}{
		{pattern: "relay.events", subject: "relay.events", want: true},
		{pattern: "relay.*", subject: "relay.events", want: true},
		{pattern: "relay.*", subject: "relay.events.v1", want: false},
		{pattern: "relay.>", subject: "relay.events.v1", want: true},
		{pattern: "relay.>", subject: "relay", want: true},
		{pattern: "", subject: "relay.events", want: false},
		{pattern: "relay.events", subject: "", want: false},
	}

	for _, tc := range tests {
		got := subjectMatchesPattern(tc.pattern, tc.subject)
		if got != tc.want {
			t.Fatalf("pattern %q subject %q: got %v want %v", tc.pattern, tc.subject, got, tc.want)
		}
	}
}

func TestIsSubjectCovered(t *testing.T) {
	patterns := []string{"relay.*", "audit.>"}

	if !isSubjectCovered(patterns, "relay.events") {
		t.Fatalf("expected subject to be covered")
	}

	if isSubjectCovered(patterns, "payments.events") {
		t.Fatalf("expected subject to not be covered")
	}
}

func TestBuildNATSPayload(t *testing.T) {
	ts := time.Date(2026, 4, 20, 9, 15, 30, 0, time.FixedZone("TZ", 5*60*60+30*60))
	e := &Publish{
		EventID:     uuid.MustParse("8d035f6e-5dcf-4642-81b9-c7bf3b2cc57f"),
		RelayID:     uuid.MustParse("4466a295-f11f-4893-a805-86db1fa399cd"),
		LSN:         "0/16B6D80",
		Sequence:    42,
		PublishedAt: ts,
	}

	data, err := buildNATSPayload(e)
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if payload["event_id"] != e.EventID.String() {
		t.Fatalf("unexpected event_id: %v", payload["event_id"])
	}
	if payload["relay_id"] != e.RelayID.String() {
		t.Fatalf("unexpected relay_id: %v", payload["relay_id"])
	}
	if payload["lsn"] != e.LSN {
		t.Fatalf("unexpected lsn: %v", payload["lsn"])
	}
	if payload["sequence"] != float64(e.Sequence) {
		t.Fatalf("unexpected sequence: %v", payload["sequence"])
	}
	if payload["published_at"] != ts.UTC().Format(time.RFC3339) {
		t.Fatalf("unexpected published_at: %v", payload["published_at"])
	}
}

func TestStreamMessageIDDeterministic(t *testing.T) {
	e := &Publish{
		EventID: uuid.MustParse("8d035f6e-5dcf-4642-81b9-c7bf3b2cc57f"),
		RelayID: uuid.MustParse("4466a295-f11f-4893-a805-86db1fa399cd"),
	}

	got := streamMessageID(e)
	want := "8d035f6e-5dcf-4642-81b9-c7bf3b2cc57f:4466a295-f11f-4893-a805-86db1fa399cd"
	if got != want {
		t.Fatalf("unexpected message id: got %q want %q", got, want)
	}
}
