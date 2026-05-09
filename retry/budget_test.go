package retry_test

import (
	"testing"

	"codeberg.org/audryus/resili7/retry"
)

// TestBudget_AllowRetry verifies that retries are allowed only when
// the ratio of successes to retries is within the configured limit.
func TestBudget_AllowRetry(t *testing.T) {
	// Ratio of 0.1 (10%)
	b := retry.NewBudget(0.1)

	// With no successes recorded, no retries should be allowed.
	if b.AllowRetry() {
		t.Error("Expected AllowRetry to be false when no successes recorded")
	}

	// Record 10 successes -> should allow 1 retry (10 * 0.1 = 1).
	for range 10 {
		b.RecordSuccess()
	}

	if !b.AllowRetry() {
		t.Error("Expected AllowRetry to be true after 10 successes")
	}

	// Subsequent retry should be denied as the quota is exhausted.
	if b.AllowRetry() {
		t.Error("Expected second AllowRetry to be false (quota exceeded)")
	}

	// Record another 10 successes -> should allow 1 more retry.
	for range 10 {
		b.RecordSuccess()
	}

	if !b.AllowRetry() {
		t.Error("Expected AllowRetry to be true again after more successes")
	}
}

// TestBudget_Decay verifies that halving the historical data maintains
// proportional quotas and prevents stale data from dominating future decisions.
func TestBudget_Decay(t *testing.T) {
	b := retry.NewBudget(0.5)

	for range 10 {
		b.RecordSuccess()
	}

	// Quota is 5 retries. Consume them all.
	for i := range 5 {
		if !b.AllowRetry() {
			t.Errorf("Expected retry %d to be allowed", i)
		}
	}

	if b.AllowRetry() {
		t.Error("Expected retry 6 to be denied")
	}

	// Apply decay (halves both success and retry counters).
	b.Decay()

	// New state: 5 successes, 2 retries (truncated).
	// New quota: 5 * 0.5 = 2.5 -> 2 retries allowed.
	// Since 2 retries were already "consumed", the next one should be denied.
	if b.AllowRetry() {
		t.Error("Expected retry after decay to be denied if quota was already consumed proportionally")
	}
}
