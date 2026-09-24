package rhttp_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rhttp"
)

// TestHttpHedgeWithVariableLatency verifies that hedging successfully reduces tail latency
// by firing parallel attempts when the primary request is slow.
func TestHttpHedgeWithVariableLatency(t *testing.T) {
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := count.Add(1)

		// Simulate variable latency:
		// First attempt is slow (100ms), second attempt is fast (10ms).
		if attempt == 1 {
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, err := rhttp.NewClient(rhttp.Pipeline{
		HttpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		// Hedge delay is set to 50ms.
		// Since the first attempt takes 100ms, the hedge attempt will fire at 50ms.
		Hedge: rhttp.NewHedgeMiddleware(50*time.Millisecond, 2),
	})

	if err != nil {
		t.Fatalf("Client should have been created")
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	// Logic check:
	// Total duration should be ~50ms (hedge delay) + ~10ms (fast second attempt) = ~60ms.
	// Without hedging, it would have taken 100ms (the slow first attempt).
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Hedge didn't optimize latency: took %v", elapsed)
	}

	// Verify that at least 2 attempts were actually fired.
	if count.Load() < 2 {
		t.Fatalf("Expected at least 2 attempts with hedge, got %d", count.Load())
	}
}

// TestHttpHedgeDisabledWithInvalidParams ensures that the middleware gracefully degrades
// to a simple pass-through if parameters are invalid.
func TestHttpHedgeDisabledWithInvalidParams(t *testing.T) {
	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200}, nil
		},
		// Invalid parameters: 0 delay or <=1 max attempts should disable hedging.
		Hedge: rhttp.NewHedgeMiddleware(0, 1),
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)
	_, err := client.Do(req)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

// BenchmarkHttpHedge measures the overhead of the hedging middleware itself.
// Since hedging involves starting goroutines, it is expected to have at least 1 allocation.
//
// BenchmarkHttpHedge/Hedge_Overhead-12         	 1392134	       888.7 ns/op	     512 B/op	       5 allocs/op
func BenchmarkHttpHedge(b *testing.B) {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)
	dummyResp := &http.Response{StatusCode: 200}

	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		// Use a high delay so hedging doesn't actually trigger, measuring baseline overhead.
		Hedge: rhttp.NewHedgeMiddleware(10*time.Second, 2),
	})

	b.Run("Hedge_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Do(req)
		}
	})
}
