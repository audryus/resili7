package breaker_test

import (
	"net/http"
	"testing"

	"codeberg.org/audryus/resili7"
	"codeberg.org/audryus/resili7/breaker"
)

// BenchmarkCircuitBreak measures the total performance and memory allocations of the circuit breaker middleware.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
//
// BenchmarkCircuitBreak/With_Circuit_Breaker-12         	26868436	        41.08 ns/op	       0 B/op	       0 allocs/op
func BenchmarkCircuitBreak(b *testing.B) {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)

	cb := breaker.NewBreaker()
	dummyResp := &http.Response{StatusCode: 200}
	client, err := resili7.NewClient(resili7.Pipeline{
		HttpHandler: func(r resili7.Request) (*http.Response, error) {
			return dummyResp, nil
		},
		CircuitBreaker: breaker.NewBreakerMiddleware(cb),
	})

	if err != nil {
		b.FailNow()
	}

	b.Run("With_Circuit_Breaker", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Do(req)
		}
	})

}
