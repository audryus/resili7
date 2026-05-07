package rhttp_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rhttp"
	"codeberg.org/audryus/resili7/timeout"
)

// TestTimeout verifies that the global timeout correctly interrupts slow requests
// and returns the expected ErrTimeout.
func TestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a server that is slower than the client's timeout.
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, err := rhttp.NewClient(rhttp.Pipeline{
		HttpCient: &http.Client{
			// The internal http.Client timeout should be higher than our middleware timeout
			// to test our custom middleware enforcement.
			Timeout: 10 * time.Second,
		},
		Timeout: rhttp.NewTimeoutMiddleware(1 * time.Second),
	})

	if err != nil {
		t.Fatalf("Client should have been created")
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)

	_, err = client.Do(req)
	if !errors.Is(err, timeout.ErrTimeout) {
		t.Fatalf("Expected timeout error, got %v", err)
	}
}

// BenchmarkTimeout measures the baseline overhead of the timeout middleware.
// This middleware is designed to be zero-allocation when the deadline is not exceeded.
//
// BenchmarkTimeout/Timeout_Overhead-12         	32296514	        37.75 ns/op	       0 B/op	       0 allocs/op
func BenchmarkTimeout(b *testing.B) {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)
	dummyResp := &http.Response{StatusCode: 200}

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		Timeout: rhttp.NewTimeoutMiddleware(10 * time.Second),
	})

	b.Run("Timeout_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Do(req)
		}
	})
}
