package rgrpc_test

import (
	"context"
	"errors"
	"testing"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/rgrpc"
	"google.golang.org/grpc"
)

// The breaker middleware passes successes through the interceptor.
func TestGrpcBreakerMiddlewarePassthrough(t *testing.T) {
	cb := breaker.NewBreaker()
	pipeline := rgrpc.Pipeline{
		CircuitBreaker: rgrpc.NewBreakerMiddleware(cb),
	}
	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)
	mockInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}
	if err := interceptor(context.Background(), "/test", nil, nil, nil, mockInvoker); err != nil {
		t.Fatalf("expected passthrough, got %v", err)
	}
}

// The breaker middleware rejects while the circuit is open.
func TestGrpcBreakerMiddlewareOpen(t *testing.T) {
	cb := breaker.NewBreaker()
	pipeline := rgrpc.Pipeline{
		CircuitBreaker: rgrpc.NewBreakerMiddleware(cb),
	}
	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)
	boom := errors.New("boom")
	failInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return boom
	}
	// Default minRequests is 20 with a 50% threshold: 20 failures trip it.
	for range 20 {
		_ = interceptor(context.Background(), "/test", nil, nil, nil, failInvoker)
	}
	if err := interceptor(context.Background(), "/test", nil, nil, nil, failInvoker); !errors.Is(err, breaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}
