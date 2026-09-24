package rgrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/audryus/resili7/rgrpc"
	"github.com/audryus/resili7/timeout"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
)

// TestGrpcTimeout verifies that the global timeout correctly interrupts slow requests
// and returns the expected ErrTimeout.
func TestGrpcTimeout(t *testing.T) {
	lis, stop := fakeServer(t, func() error {
		time.Sleep(2 * time.Second)
		return nil
	})
	defer stop()

	pipeline := rgrpc.Pipeline{
		Timeout: rgrpc.NewTimeoutMiddleware(1 * time.Second),
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

	// Empty call (Empty is a type from the grpc_testing package)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})

	if err == nil {
		t.Fatalf("Expected some error")
	}

	if !errors.Is(err, timeout.ErrTimeout) {
		t.Fatalf("Expected timeout error, got %v", err)
	}
}

// BenchmarkGrpcTimeout measures the baseline overhead of the timeout middleware.
// This middleware is designed to be zero-allocation when the deadline is not exceeded.
//
// BenchmarkGrpcTimeout/Timeout_Overhead-12         	11311341	       106.4 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGrpcTimeout(b *testing.B) {
	mockInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	pipeline := rgrpc.Pipeline{
		Timeout: rgrpc.NewTimeoutMiddleware(1 * time.Second),
	}

	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	b.Run("Timeout_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = interceptor(context.Background(), "/test", nil, nil, nil, mockInvoker)
		}
	})
}
