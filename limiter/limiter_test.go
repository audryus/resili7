package limiter_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codeberg.org/audryus/resili7"
	"codeberg.org/audryus/resili7/limiter"
)

// TestLimiterMiddleware verifies the end-to-end behavior of the limiter middleware.
// It uses a mock server and confirms that requests are rejected with ErrLimited
// when the concurrency capacity is exceeded.
func TestLimiterMiddleware(t *testing.T) {
	// Create a limiter with capacity for only 1 concurrent request.
	l := limiter.NewLimiter(limiter.WithInitialLimit(1))

	// Manually exhaust the limiter.
	l.Acquire(time.Now().UnixNano())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, _ := resili7.NewClient(resili7.Pipeline{
		HttpCient: &http.Client{
			Timeout: 10 * time.Second,
		},
		Limiter: limiter.NewLimiterMiddleware(l),
	})

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)

	// Attempt a request - should be rejected immediately by the middleware.
	_, err := client.Do(req)
	if !errors.Is(err, limiter.ErrLimited) {
		t.Fatalf("Expected rate limited error, got %v", err)
	}
}

// BenchmarkLimiter measures the baseline overhead of the adaptive limiter.
// This is designed to be highly efficient and zero-allocation on the hot path.
//
// BenchmarkLimiter/Limiter_Overhead-12         	15625302	        75.28 ns/op	       0 B/op	       0 allocs/op
func BenchmarkLimiter(b *testing.B) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))
	dummyResp := &http.Response{StatusCode: 200}

	client, _ := resili7.NewClient(resili7.Pipeline{
		HttpHandler: func(r resili7.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Limiter: limiter.NewLimiterMiddleware(l),
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
