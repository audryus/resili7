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

// NewBudget creates a new retry budget with the given ratio (0.0 to 1.0).
// A ratio of 0.1 allows 1 retry for every 10 successful requests.
func NewBudget(ratio float64) *Budget {
	if ratio <= 0 {
		ratio = 0.2 // Default to 20% if invalid
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
// This is a thread-safe, lock-free implementation.
func (b *Budget) AllowRetry() bool {
	success := b.success.Load()
	retries := b.retries.Load()

	// allowed = (success * ratio) / 1000
	allowed := (success * uint64(b.ratio)) / 1000

	if retries >= allowed {
		return false
	}

	// Only increment the retry counter if the retry is actually allowed.
	b.retries.Add(1)
	return true
}

// Decay reduces the historical data by half to give more weight to recent performance.
// This should be called periodically (e.g., every 10-60 seconds).
func (b *Budget) Decay() {
	b.success.Store(b.success.Load() / 2)
	b.retries.Store(b.retries.Load() / 2)
}
