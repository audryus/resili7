package rgrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/rgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
)

// TestLimiter verifies the end-to-end behavior of the limiter middleware.
// It uses a mock server and confirms that requests are rejected with ErrLimited
// when the concurrency capacity is exceeded.
func TestLimiter(t *testing.T) {
	// Create a limiter with capacity for only 1 concurrent request.
	l := limiter.NewLimiter(limiter.WithInitialLimit(1))

	// Manually exhaust the limiter.
	l.Acquire(time.Now().UnixNano())

	lis, stop := fakeServer(t, func() error {
		time.Sleep(2 * time.Second)
		return nil
	})
	defer stop()

	pipeline := rgrpc.Pipeline{
		Limiter: rgrpc.NewLimiterMiddleware(l),
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

	if !errors.Is(err, limiter.ErrLimited) {
		t.Fatalf("Expected rate limited error, got %v", err)
	}
}

// BenchmarkLimiter measures the baseline overhead of the adaptive limiter.
// This is designed to be highly efficient and zero-allocation on the hot path.
//
// BenchmarkLimiter/Limiter_Overhead-12         	10465168	       114.4 ns/op	       0 B/op	       0 allocs/op
func BenchmarkLimiter(b *testing.B) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))

	mockInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	pipeline := rgrpc.Pipeline{
		Limiter: rgrpc.NewLimiterMiddleware(l),
	}

	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	b.Run("Limiter_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = interceptor(context.Background(), "/test", nil, nil, nil, mockInvoker)
		}
	})
}
