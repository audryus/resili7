package rws_test

import (
	"testing"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

// BenchmarkWsCircuitBreak measures the total performance and memory allocations of the circuit breaker middleware.
// The goal of resili7 is to achieve near-zero allocations (typically 1-2 per request due to goroutines).
//
// BenchmarkWsCircuitBreak/With_Circuit_Breaker-12         	25469554	        47.32 ns/op	       0 B/op	       0 allocs/op
func BenchmarkWsCircuitBreak(b *testing.B) {
	cb := breaker.NewBreaker()

	okData := []byte("ok")
	resp := &rws.Response{MessageType: websocket.TextMessage, Data: okData}

	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return resp, nil
		},
		CircuitBreaker: rws.NewBreakerMiddleware(cb),
	})

	req := rws.Request{MessageType: websocket.TextMessage, Data: okData}

	b.Run("With_Circuit_Breaker", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Send(req)
		}
	})

}
