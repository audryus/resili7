package retry

import "sync/atomic"

// Budget implements a retry budget mechanism to prevent "retry storms".
// It tracks successes and retries to ensure that the ratio of retries
// does not exceed a specified percentage of total traffic.
type Budget struct {
	success atomic.Uint64
	retries atomic.Uint64

	ratio int64 // Scale factor of 1000 (e.g., 10% = 100)
}

// defaultBudgetRatio is used when the caller passes an out-of-range ratio.
const defaultBudgetRatio = 0.2

// NewBudget creates a new retry budget with the given ratio (0.0 to 1.0).
// A ratio of 0.1 allows 1 retry for every 10 successful requests.
// Out-of-range values (<=0, >1, NaN, +Inf) fall back to the default of 0.2.
func NewBudget(ratio float64) *Budget {
	if !(ratio > 0 && ratio <= 1) {
		ratio = defaultBudgetRatio
	}

	return &Budget{
		ratio: int64(ratio * 1000),
	}
}

// RecordSuccess should be called whenever a request succeeds.
// This increases the available retry quota.
func (b *Budget) RecordSuccess() {
	b.success.Add(1)
}

// AllowRetry checks if a retry is permitted within the current budget.
// If permitted, it increments the retry counter and returns true.
// This is a thread-safe, lock-free implementation using a CAS loop so
// concurrent callers cannot overshoot the budget.
func (b *Budget) AllowRetry() bool {
	ratio := uint64(b.ratio)
	for {
		success := b.success.Load()
		retries := b.retries.Load()

		// allowed = (success * ratio) / 1000
		allowed := (success * ratio) / 1000

		if retries >= allowed {
			return false
		}

		// Only increment the retry counter if the retry is actually allowed.
		if b.retries.CompareAndSwap(retries, retries+1) {
			return true
		}
	}
}

// Decay reduces the historical data by half to give more weight to recent performance.
// This should be called periodically (e.g., every 10-60 seconds).
// It uses CAS loops so concurrent AllowRetry/RecordSuccess calls cannot lose updates.
func (b *Budget) Decay() {
	for {
		s := b.success.Load()
		if b.success.CompareAndSwap(s, s/2) {
			break
		}
	}
	for {
		r := b.retries.Load()
		if b.retries.CompareAndSwap(r, r/2) {
			break
		}
	}
}
