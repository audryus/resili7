package resili7_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"codeberg.org/audryus/resili7"
	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/hedge"
	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/timeout"
)

// TestNewClient ensures the client is correctly initialized with all pipeline components.
func TestNewClient(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(10))
	cb := breaker.NewBreaker()

	pipeline := resili7.Pipeline{
		HttpCient:      http.DefaultClient,
		Timeout:        timeout.NewTimeoutMiddleware(1 * time.Second),
		Retry:          retry.NewRetryMiddleware(&retry.RetryPolicy{MaxAttempts: 3}),
		Hedge:          hedge.NewHedgeMiddleware(100*time.Millisecond, 2),
		Limiter:        limiter.NewLimiterMiddleware(l),
		CircuitBreaker: breaker.NewBreakerMiddleware(cb),
	}

	client, err := resili7.NewClient(pipeline)
	if err != nil {
		t.Fatalf("Expected nil error, got %v", err)
	}

	if client == nil {
		t.Fatal("Expected client to be non-nil")
	}
}

// BenchmarkFullStack measures the total performance and memory allocations of the entire pipeline.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
//
// BenchmarkFullStack/With_All-12         	 1600312	       754.2 ns/op	      64 B/op	       1 allocs/op
func BenchmarkFullStack(b *testing.B) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))
	cb := breaker.NewBreaker()

	// Pre-allocate a response to avoid noise during benchmarking.
	dummyResp := &http.Response{StatusCode: 200}

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	client, _ := resili7.NewClient(resili7.Pipeline{
		// Use a mock HttpHandler that returns instantly to isolate library overhead.
		HttpHandler: func(r resili7.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Timeout: timeout.NewTimeoutMiddleware(1 * time.Second),
		Retry: retry.NewRetryMiddleware(&retry.RetryPolicy{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: retry.ShouldRetryDefault,
			Budget:      budget,
			TryDeadline: 1 * time.Second,
		}),
		Hedge:          hedge.NewHedgeMiddleware(100*time.Millisecond, 2),
		Limiter:        limiter.NewLimiterMiddleware(l),
		CircuitBreaker: breaker.NewBreakerMiddleware(cb),
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)

	b.Run("With_All", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			resp, err := client.Do(req)
			if err != nil || resp.StatusCode != 200 {
				b.Fatal("Expected success")
			}
		}
	})
}

// TestPipelineErrors verifies that errors from the handler correctly propagate through the chain.
func TestPipelineErrors(t *testing.T) {
	expectedErr := errors.New("custom failure")
	client, _ := resili7.NewClient(resili7.Pipeline{
		HttpHandler: func(r resili7.Request) (*http.Response, error) {
			return nil, expectedErr
		},
		Retry: retry.NewRetryMiddleware(&retry.RetryPolicy{MaxAttempts: 1}),
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)
	_, err := client.Do(req)

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected %v, got %v", expectedErr, err)
	}
}
