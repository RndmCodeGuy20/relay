package rule

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type fakeRepo struct {
	rules []Rule
}

func (f *fakeRepo) ListEnabled(_ context.Context) ([]Rule, error) {
	return f.rules, nil
}

func (f *fakeRepo) ListEnabledBySourceEventType(_ context.Context, source string, eventType string) ([]Rule, error) {
	out := make([]Rule, 0)
	for _, r := range f.rules {
		if r.Enabled && r.Source == source && r.EventType == eventType {
			out = append(out, r)
		}
	}
	return out, nil
}

func TestCacheRefreshAndGet(t *testing.T) {
	repo := &fakeRepo{rules: []Rule{
		{ID: uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), Name: "low", Source: "checkout", EventType: "order.created", Enabled: true, Priority: 1, Version: 1},
		{ID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), Name: "high", Source: "checkout", EventType: "order.created", Enabled: true, Priority: 10, Version: 1},
	}}

	cache := NewCache(repo)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("unexpected refresh error: %v", err)
	}

	if !cache.HasLoaded() {
		t.Fatalf("expected cache to report loaded")
	}

	rules := cache.Get("checkout", "order.created")
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}
	if rules[0].Name != "high" {
		t.Fatalf("expected sorted order with high priority first, got %s", rules[0].Name)
	}
}
