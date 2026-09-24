package rhttp_test

import (
	"net/http"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rhttp"
)

// M6: Retry must stay active when Hedge is also configured with
// fanoutLimit==0 (stricter fan-out must not silently drop retries).
func TestHedgeRetryFanoutZeroKeepsRetry(t *testing.T) {
	calls := 0
	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}
	client, err := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return &http.Response{StatusCode: 500}, nil
			}
			return &http.Response{StatusCode: 200}, nil
		},
		Retry: rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{
			MaxAttempts: 2,
			Backoff:     func(_ int) time.Duration { return 0 },
			ShouldRetry: rhttp.ShouldRetryDefault,
			Budget:      budget,
		}),
		Hedge:                 rhttp.NewHedgeMiddleware(0, 1),
		RetryHedgeFanoutLimit: 0,
	})
	if err != nil {
		t.Fatalf("client creation failed: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expected retry to recover, got %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 after retry, got %d", resp.StatusCode)
	}
	if calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", calls)
	}
}
