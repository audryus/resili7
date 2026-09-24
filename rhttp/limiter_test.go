package rhttp_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/audryus/resili7/limiter"
	"github.com/audryus/resili7/rhttp"
)

// TestHttpLimiter verifies the end-to-end behavior of the limiter middleware.
// It uses a mock server and confirms that requests are rejected with ErrLimited
// when the concurrency capacity is exceeded.
func TestHttpLimiter(t *testing.T) {
	// Create a limiter with capacity for only 1 concurrent request.
	l := limiter.NewLimiter(limiter.WithInitialLimit(1))

	// Manually exhaust the limiter.
	l.Acquire(time.Now().UnixNano())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Limiter: rhttp.NewLimiterMiddleware(l),
	})

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)

	// Attempt a request - should be rejected immediately by the middleware.
	_, err := client.Do(req)
	if !errors.Is(err, limiter.ErrLimited) {
		t.Fatalf("Expected rate limited error, got %v", err)
	}
}

// BenchmarkHttpLimiter measures the baseline overhead of the adaptive limiter.
// This is designed to be highly efficient and near-zero-allocation on the hot path.
//
// BenchmarkHttpLimiter/Limiter_Overhead-12         	15625302	        75.28 ns/op	       0 B/op	       0 allocs/op
func BenchmarkHttpLimiter(b *testing.B) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))
	dummyResp := &http.Response{StatusCode: 200}

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Limiter: rhttp.NewLimiterMiddleware(l),
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)

	b.Run("Limiter_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Do(req)
		}
	})
}
