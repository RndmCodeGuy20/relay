package rule

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"rndmcodeguy.in/relay/internal/logger"
)

type Cache struct {
	repo Repository

	mu          sync.RWMutex
	bySourceKey map[string][]Rule
	loaded      bool
	lastRefresh time.Time
}

func NewCache(repo Repository) *Cache {
	return &Cache{
		repo:        repo,
		bySourceKey: map[string][]Rule{},
	}
}

func (c *Cache) Refresh(ctx context.Context) error {
	rules, err := c.repo.ListEnabled(ctx)
	if err != nil {
		return err
	}

	next := make(map[string][]Rule)
	for _, rule := range rules {
		key := rule.Source + "::" + rule.EventType
		next[key] = append(next[key], rule)
	}

	for key := range next {
		sortRules(next[key])
	}

	c.mu.Lock()
	c.bySourceKey = next
	c.loaded = true
	c.lastRefresh = time.Now().UTC()
	c.mu.Unlock()

	return nil
}

func (c *Cache) StartRefreshLoop(ctx context.Context, interval time.Duration) error {
	if err := c.Refresh(ctx); err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Refresh(ctx); err != nil {
				log.Warn("rule cache refresh failed", zap.Error(err))
			}
		}
	}
}

func (c *Cache) Get(source string, eventType string) []Rule {
	key := source + "::" + eventType

	c.mu.RLock()
	defer c.mu.RUnlock()

	rules := c.bySourceKey[key]
	out := make([]Rule, len(rules))
	copy(out, rules)
	return out
}

func (c *Cache) HasLoaded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loaded
}

func (c *Cache) LastRefresh() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastRefresh
}
