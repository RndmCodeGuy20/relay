package worker

import (
	"math/rand"
	"time"
)

// BackoffConfig parametrizes nextBackoff.
type BackoffConfig struct {
	Base time.Duration // initial step (e.g., 1s)
	Max  time.Duration // hard cap (e.g., 5min)
}

const maxAttemptsShift = 30

// nextBackoff returns the wait duration for the given attempt count using
// equal-jitter exponential backoff:
//
//	expWindow = min(Base * 2^attempts, Max)
//	delay     = expWindow/2 + rand_in(0, expWindow/2)
//
// Equal jitter spreads retries to avoid thundering herd while keeping a
// predictable lower bound. attempts is 0-based: attempts=0 returns the
// first delay, attempts=1 doubles the window, etc. rng must be non-nil.
//
// attempts is clamped at a sane upper bound internally (~30) so the shift
// never overflows.
func nextBackoff(attempts int, cfg BackoffConfig, rng *rand.Rand) time.Duration {
	if cfg.Base <= 0 {
		return 0
	}
	if attempts < 0 {
		attempts = 0
	}
	if attempts > maxAttemptsShift {
		attempts = maxAttemptsShift
	}

	expWindow := cfg.Base << uint(attempts)
	if expWindow <= 0 {
		expWindow = cfg.Base
		if cfg.Max > 0 {
			expWindow = cfg.Max
		}
	}
	if cfg.Max > 0 && expWindow > cfg.Max {
		expWindow = cfg.Max
	}

	half := expWindow / 2
	if half <= 0 {
		return expWindow
	}
	return half + time.Duration(rng.Int63n(int64(half)))
}
