package rgrpc_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
)

// TestHedgeWithVariableLatency verifies that hedging successfully reduces tail latency
// by firing parallel attempts when the primary request is slow.
func TestHedgeWithVariableLatency(t *testing.T) {
	var count atomic.Int32

	lis, stop := fakeServer(t, func() error {
		attempt := count.Add(1)

		// Simulate variable latency:
		// First attempt is slow (100ms), second attempt is fast (10ms).
		if attempt == 1 {
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	})
	defer stop()

	pipeline := rgrpc.Pipeline{
		Hedge: rgrpc.NewHedgeMiddleware(50*time.Millisecond, 2),
	}

	// Dial needs to know the address that the listener grabbed (lis.Addr())
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(rgrpc.NewUnaryClientInterceptor(pipeline)),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Make the actual call
	client := grpc_testing.NewTestServiceClient(conn)

	start := time.Now()
	// Empty call (Empty is a type from the grpc_testing package)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
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

// TestHedgeDisabledWithInvalidParams ensures that the middleware gracefully degrades
// to a simple pass-through if parameters are invalid.
func TestHedgeDisabledWithInvalidParams(t *testing.T) {
	lis, stop := fakeServer(t, func() error {
		return nil
	})
	defer stop()

	pipeline := rgrpc.Pipeline{
		// Invalid parameters: 0 delay or <=1 max attempts should disable hedging.
		Hedge: rgrpc.NewHedgeMiddleware(0, 1),
	}

	// Dial needs to know the address that the listener grabbed (lis.Addr())
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(rgrpc.NewUnaryClientInterceptor(pipeline)),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Make the actual call
	client := grpc_testing.NewTestServiceClient(conn)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

// BenchmarkHedge measures the overhead of the hedging middleware itself.
// Since hedging involves starting goroutines, it is expected to have at least 1 allocation.
//
// BenchmarkHedge/Hedge_Overhead-12         	 1609233	       744.8 ns/op	     304 B/op	       3 allocs/op
func BenchmarkHedge(b *testing.B) {
	mockInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	pipeline := rgrpc.Pipeline{
		Hedge: rgrpc.NewHedgeMiddleware(50*time.Millisecond, 2),
	}

	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	b.Run("Hedge_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = interceptor(context.Background(), "/test", nil, nil, nil, mockInvoker)
		}
	})
}
