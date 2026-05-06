package retry_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codeberg.org/audryus/resili7"
	"codeberg.org/audryus/resili7/retry"
)

// TestClientRetry verifies that the middleware correctly retries failed HTTP requests
// based on status codes and respects the retry budget.
func TestClientRetry(t *testing.T) {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		// Simulate a slow response that eventually fails with 500.
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	// Initialize budget with enough successes to allow retries.
	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	client, err := resili7.NewClient(resili7.Pipeline{
		HttpCient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Retry: retry.NewRetryMiddleware(&retry.RetryPolicy{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: retry.ShouldRetryDefault,
			Budget:      budget,
		}),
	})

	if err != nil {
		t.Fatalf("Client should have been created")
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)

	_, err = client.Do(req)
	// After 2 attempts fail with 500, we expect ErrRetry.
	if !errors.Is(err, retry.ErrRetry) {
		t.Fatalf("Expected retry error, got %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected 2 attempts, got %d", count)
	}
}

// TestClientPerRetry verifies the per-attempt timeout logic.
// It ensures that slow individual attempts are interrupted even if the global timeout allows more time.
func TestClientPerRetry(t *testing.T) {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		// Server takes 2 seconds, while per-try timeout is only 1 second.
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	client, err := resili7.NewClient(resili7.Pipeline{
		HttpCient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Retry: retry.NewRetryMiddleware(&retry.RetryPolicy{
			MaxAttempts: 2,
			TryDeadline: 1 * time.Second, // Each attempt limited to 1s.
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: retry.ShouldRetryDefault,
			Budget:      budget,
		}),
	})

	if err != nil {
		t.Fatalf("Client should have been created")
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)

	_, err = client.Do(req)
	// We expect the first attempt to timeout and be reported as ErrTimeout.
	if !errors.Is(err, retry.ErrTimeout) {
		t.Fatalf("Expected timeout error, got %v", err)
	}
}

// BenchmarkRetry measures the memory overhead of the retry middleware.
// This middleware is designed to be zero-allocation during execution.
//
// BenchmarkRetry/With_Retry-12         	15982428	        74.19 ns/op	       0 B/op	       0 allocs/op
func BenchmarkRetry(b *testing.B) {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}
	dummyResp := &http.Response{StatusCode: 200}

	client, _ := resili7.NewClient(resili7.Pipeline{
		HttpHandler: func(r resili7.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Retry: retry.NewRetryMiddleware(&retry.RetryPolicy{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: retry.ShouldRetryDefault,
			Budget:      budget,
		}),
	})

	b.Run("With_Retry", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Do(req)
		}
	})
}
