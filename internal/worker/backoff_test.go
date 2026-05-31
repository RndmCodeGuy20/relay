package worker

import (
	"math/rand"
	"testing"
	"time"
)

func TestNextBackoff(t *testing.T) {
	t.Run("attempts_zero_in_half_to_base_range", func(t *testing.T) {
		cfg := BackoffConfig{Base: 1 * time.Second, Max: 5 * time.Minute}
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 100; i++ {
			d := nextBackoff(0, cfg, rng)
			if d < cfg.Base/2 || d > cfg.Base {
				t.Fatalf("delay %v out of bounds [%v, %v]", d, cfg.Base/2, cfg.Base)
			}
		}
	})

	t.Run("exponential_growth_saturates_at_max", func(t *testing.T) {
		cfg := BackoffConfig{Base: 100 * time.Millisecond, Max: 1 * time.Second}
		rng := rand.New(rand.NewSource(42))
		for attempts := 1; attempts <= 10; attempts++ {
			d := nextBackoff(attempts, cfg, rng)
			if d > cfg.Max {
				t.Fatalf("attempts=%d delay %v exceeds max %v", attempts, d, cfg.Max)
			}
			if d <= 0 {
				t.Fatalf("attempts=%d delay %v not positive", attempts, d)
			}
		}
	})

	t.Run("huge_attempts_no_overflow", func(t *testing.T) {
		cfg := BackoffConfig{Base: 1 * time.Second, Max: 5 * time.Minute}
		rng := rand.New(rand.NewSource(42))
		d := nextBackoff(64, cfg, rng)
		if d > cfg.Max {
			t.Fatalf("delay %v exceeds max %v", d, cfg.Max)
		}
		if d <= 0 {
			t.Fatalf("delay %v not positive", d)
		}
	})

	t.Run("deterministic_with_same_seed", func(t *testing.T) {
		cfg := BackoffConfig{Base: 1 * time.Second, Max: 5 * time.Minute}
		rngA := rand.New(rand.NewSource(42))
		rngB := rand.New(rand.NewSource(42))
		for attempts := 0; attempts < 20; attempts++ {
			a := nextBackoff(attempts, cfg, rngA)
			b := nextBackoff(attempts, cfg, rngB)
			if a != b {
				t.Fatalf("attempts=%d not deterministic: %v vs %v", attempts, a, b)
			}
		}
	})

	t.Run("base_zero_returns_zero", func(t *testing.T) {
		cfg := BackoffConfig{Base: 0, Max: 5 * time.Minute}
		rng := rand.New(rand.NewSource(42))
		if d := nextBackoff(3, cfg, rng); d != 0 {
			t.Fatalf("expected 0, got %v", d)
		}
	})

	t.Run("half_zero_does_not_panic", func(t *testing.T) {
		cfg := BackoffConfig{Base: 1 * time.Nanosecond, Max: 5 * time.Minute}
		rng := rand.New(rand.NewSource(42))
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		d := nextBackoff(0, cfg, rng)
		if d < 0 {
			t.Fatalf("negative delay %v", d)
		}
	})
}
