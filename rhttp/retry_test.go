package rhttp_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rhttp"
)

// TestRetry verifies that the middleware correctly retries failed HTTP requests
// based on status codes and respects the retry budget.
func TestRetry(t *testing.T) {
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

	client, err := rhttp.NewClient(rhttp.Pipeline{
		HttpCient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Retry: rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: rhttp.ShouldRetryDefault,
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

// TestRetryPerTry verifies the per-attempt timeout logic.
// It ensures that slow individual attempts are interrupted even if the global timeout allows more time.
func TestRetryPerTry(t *testing.T) {
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

	client, err := rhttp.NewClient(rhttp.Pipeline{
		HttpCient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Retry: rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{
			MaxAttempts: 2,
			TryDeadline: 1 * time.Second, // Each attempt limited to 1s.
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: rhttp.ShouldRetryDefault,
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

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		err      error
		resp     *http.Response
		name     string
		expected bool
	}{
		{
			name:     "Error present",
			resp:     nil,
			err:      http.ErrHandlerTimeout,
			expected: true,
		},
		{
			name:     "Success 200",
			resp:     &http.Response{StatusCode: 200},
			err:      nil,
			expected: false,
		},
		{
			name:     "Client Error 400",
			resp:     &http.Response{StatusCode: 400},
			err:      nil,
			expected: false,
		},
		{
			name:     "Server Error 500",
			resp:     &http.Response{StatusCode: 500},
			err:      nil,
			expected: true,
		},
		{
			name:     "Rate Limit 429",
			resp:     &http.Response{StatusCode: 429},
			err:      nil,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var shouldRetry = rhttp.ShouldRetryDefault
			if got := shouldRetry(tt.resp, tt.err); got != tt.expected {
				t.Errorf("ShouldRetryDefault() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// BenchmarkRetry measures the memory overhead of the retry middleware.
// This middleware is designed to be zero-allocation during execution.
//
// BenchmarkRetry/With_Retry-12         	16394527	        72.16 ns/op	       0 B/op	       0 allocs/op
func BenchmarkRetry(b *testing.B) {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}
	dummyResp := &http.Response{StatusCode: 200}

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Retry: rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: rhttp.ShouldRetryDefault,
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
