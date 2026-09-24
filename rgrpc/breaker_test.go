package rgrpc_test

import (
	"context"
	"testing"

	"github.com/audryus/resili7/breaker"
	"github.com/audryus/resili7/rgrpc"
	"google.golang.org/grpc"
)

// BenchmarkGrpcCircuitBreak measures the total performance and memory allocations of the circuit breaker middleware for gRPC.
// The goal of resili7 is to achieve zero allocations on the hot path.
//
// BenchmarkGrpcCircuitBreak/With_Circuit_Breaker-12         	15622276	        73.78 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGrpcCircuitBreak(b *testing.B) {
	cb := breaker.NewBreaker()
	pipeline := rgrpc.Pipeline{
		CircuitBreaker: rgrpc.NewBreakerMiddleware(cb),
	}

	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)
	mockInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	b.Run("With_Circuit_Breaker", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = interceptor(context.Background(), "/test", nil, nil, nil, mockInvoker)
		}
	})
}
