package retry_test

import (
	"errors"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/retry"
)

var errTest = errors.New("test error")

// Defaults: linear backoff, 3 attempts, 30s try deadline, error retry predicate.
func TestNewRetryPolicyDefaults(t *testing.T) {
	p := retry.NewRetryPolicy[int]()
	if p.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", p.MaxAttempts)
	}
	if p.TryDeadline != 30*time.Second {
		t.Fatalf("TryDeadline = %v, want 30s", p.TryDeadline)
	}
	if p.Backoff == nil || p.ShouldRetry == nil {
		t.Fatal("Backoff/ShouldRetry must default")
	}
	if p.Backoff(2) != 2*time.Second {
		t.Fatalf("default linear backoff(2) = %v, want 2s", p.Backoff(2))
	}
	if !p.ShouldRetry(0, errTest) || p.ShouldRetry(0, nil) {
		t.Fatal("default OnErrorRetry predicate broken")
	}
}

// Options override defaults.
func TestNewRetryPolicyOverrides(t *testing.T) {
	b := retry.NewBudget(0.5)
	p := retry.NewRetryPolicy[int](
		retry.WithMaxAttempts[int](5),
		retry.WithTryDeadline[int](time.Second),
		retry.WithBackoff[int](retry.ExponentialBackoff),
		retry.WithShouldRetry[int](func(_ int, _ error) bool { return false }),
		retry.WithBudget[int](b),
	)
	if p.MaxAttempts != 5 || p.TryDeadline != time.Second || p.Budget != b {
		t.Fatalf("overrides not applied: %+v", p)
	}
	if p.Backoff(2) != 4*time.Second {
		t.Fatalf("exponential backoff(2) = %v, want 4s", p.Backoff(2))
	}
	if p.ShouldRetry(0, errTest) {
		t.Fatal("custom ShouldRetry not applied")
	}
}

// Backoff strategies produce the documented sequences.
func TestBackoffSequences(t *testing.T) {
	for i, want := range []time.Duration{0, time.Second, 2 * time.Second} {
		if got := retry.LinearBackoff(i); got != want {
			t.Fatalf("LinearBackoff(%d) = %v, want %v", i, got, want)
		}
	}
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		if got := retry.ExponentialBackoff(i); got != want {
			t.Fatalf("ExponentialBackoff(%d) = %v, want %v", i, got, want)
		}
	}
}
