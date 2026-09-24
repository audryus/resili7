package retry_test

import (
	"net/http"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rhttp"
)

// M5: the Option constructor must be usable for typed policies without casts.
func TestNewRetryPolicyTypedOptions(t *testing.T) {
	b := retry.NewBudget(0.5)
	p := retry.NewRetryPolicy[*http.Response](
		retry.WithMaxAttempts[*http.Response](3),
		retry.WithTryDeadline[*http.Response](2*time.Second),
		retry.WithBackoff[*http.Response](retry.ExponentialBackoff),
		retry.WithShouldRetry[*http.Response](rhttp.ShouldRetryDefault),
		retry.WithBudget[*http.Response](b),
	)
	if p.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", p.MaxAttempts)
	}
	if p.TryDeadline != 2*time.Second {
		t.Fatalf("TryDeadline = %v, want 2s", p.TryDeadline)
	}
	if p.Budget != b {
		t.Fatal("Budget not wired")
	}
	if p.ShouldRetry == nil || p.Backoff == nil {
		t.Fatal("ShouldRetry/Backoff not wired")
	}
	if !p.ShouldRetry(&http.Response{StatusCode: 500}, nil) {
		t.Fatal("typed ShouldRetry predicate broken")
	}
}
