package resili7_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rgrpc"
	"codeberg.org/audryus/resili7/rhttp"
	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

// BenchmarkHttpFullStack measures the total performance and memory allocations of the entire pipeline.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
//
// BenchmarkHttpFullStack/With_All-12               1275505               921.6 ns/op            64 B/op          1 allocs/op
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
// BenchmarkGrpcFullStack/With_All-12                922318              1377 ns/op             304 B/op          3 allocs/op
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

// BenchmarkWsFullStack measures the total performance and memory allocations of the entire pipeline.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
// Note: URL is intentionally not set in the request to isolate library overhead.
// Setting URL triggers actual WebSocket dials, which include HTTP/TCP handshake allocations (see WS dialing overhead test).
//
// BenchmarkWsFullStack/With_All-12                 5205252               235.1 ns/op             0 B/op          0 allocs/op
func BenchmarkWsFullStack(b *testing.B) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
	}))
	defer server.Close()

	_ = server // server is used for setup, URL not needed for mock handler

	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))
	cb := breaker.NewBreaker()

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	okData := []byte("ok")
	resp := &rws.Response{MessageType: websocket.TextMessage, Data: okData}

	client, _ := rws.NewClient(rws.Pipeline{
		// Use a mock WsHandler that returns instantly to isolate library overhead.
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return resp, nil
		},
		Timeout: rws.NewTimeoutMiddleware(rws.TimeoutConfig{
			Read: 1 * time.Second,
		}),
		Retry: rws.NewRetryMiddleware(&retry.RetryPolicy[*rws.Response]{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: rws.ShouldRetryDefault,
			Budget:      budget,
			TryDeadline: 1 * time.Second,
		}),
		// Note: Hedge is disabled here because we're testing mock handler overhead, not dialing overhead.
		// Hedge with a URL would trigger real WebSocket dials which include all HTTP/TCP handshake allocations.
		Hedge:          rws.NewHedgeMiddleware(websocket.DefaultDialer.Dial, 100*time.Millisecond, 2),
		Limiter:        rws.NewLimiterMiddleware(l),
		CircuitBreaker: rws.NewBreakerMiddleware(cb),
	})

	// Don't set URL in request - this prevents Hedge from attempting real dials.
	// The Hedge middleware will skip hedging when URL is empty.
	req := rws.Request{MessageType: websocket.TextMessage, Data: okData}

	b.Run("With_All", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			resp, err := client.Send(req)
			if err != nil || resp == nil {
				b.Fatalf("Expected success: %+v", err)
			}
		}
	})
}
