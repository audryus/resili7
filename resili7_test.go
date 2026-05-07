package resili7_test

import (
	"net/http"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rhttp"
)

// BenchmarkFullStack measures the total performance and memory allocations of the entire pipeline.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
//
// BenchmarkHttpFullStack/With_All-12               1418569               857.7 ns/op            64 B/op          1 allocs/op
func BenchmarkHttpFullStack(b *testing.B) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))
	cb := breaker.NewBreaker()

	// Pre-allocate a response to avoid noise during benchmarking.
	dummyResp := &http.Response{StatusCode: 200}

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		// Use a mock HttpHandler that returns instantly to isolate library overhead.
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Timeout: rhttp.NewTimeoutMiddleware(1 * time.Second),
		Retry: rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: rhttp.ShouldRetryDefault,
			Budget:      budget,
			TryDeadline: 1 * time.Second,
		}),
		Hedge:          rhttp.NewHedgeMiddleware(100*time.Millisecond, 2),
		Limiter:        rhttp.NewLimiterMiddleware(l),
		CircuitBreaker: rhttp.NewBreakerMiddleware(cb),
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
