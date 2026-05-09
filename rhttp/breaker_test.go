package rhttp_test

import (
	"net/http"
	"testing"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/rhttp"
)

// BenchmarkHttpCircuitBreak measures the total performance and memory allocations of the circuit breaker middleware.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
//
// BenchmarkHttpCircuitBreak/With_Circuit_Breaker-12         	29280568	        40.79 ns/op	       0 B/op	       0 allocs/op
func BenchmarkHttpCircuitBreak(b *testing.B) {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)

	cb := breaker.NewBreaker()
	dummyResp := &http.Response{StatusCode: 200}
	client, err := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		CircuitBreaker: rhttp.NewBreakerMiddleware(cb),
	})

	if err != nil {
		b.Fatal("Client should have been created")
	}

	b.Run("With_Circuit_Breaker", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Do(req)
		}
	})

}
