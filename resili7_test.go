package resili7_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rgrpc"
	"codeberg.org/audryus/resili7/rhttp"
	"google.golang.org/grpc"
)

// BenchmarkHttpFullStack measures the total performance and memory allocations of the entire pipeline.
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

// BenchmarkGrpcFullStack measures the total performance and memory allocations of the entire pipeline.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
//
// BenchmarkGrpcFullStack/With_All-12         	  896580	      1284 ns/op	     304 B/op	       3 allocs/op
func BenchmarkGrpcFullStack(b *testing.B) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))
	cb := breaker.NewBreaker()

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	mockInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	pipeline := rgrpc.Pipeline{
		Timeout: rgrpc.NewTimeoutMiddleware(1 * time.Second),
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: func(_ rgrpc.Request, err error) bool { return rgrpc.ShouldRetryDefault(err) },
			Budget:      budget,
			TryDeadline: 1 * time.Second,
		}),
		Hedge:          rgrpc.NewHedgeMiddleware(100*time.Millisecond, 2),
		Limiter:        rgrpc.NewLimiterMiddleware(l),
		CircuitBreaker: rgrpc.NewBreakerMiddleware(cb),
	}

	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	b.Run("With_All", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			err := interceptor(context.Background(), "/test", nil, nil, nil, mockInvoker)
			if err != nil {
				b.Fatal("Expected success")
			}
		}
	})
}
