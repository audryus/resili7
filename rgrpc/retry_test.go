package rgrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
)

// TestRetry verifies that the middleware correctly retries failed gRPC requests
// based on status codes and respects the retry budget.
func TestRetry(t *testing.T) {
	count := 0
	lis, stop := fakeServer(t, func() error {
		count++
		// Simulate a transient failure.
		return status.Error(codes.Unavailable, "overloaded")
	})
	defer stop()

	// Initialize budget with enough successes to allow retries.
	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	pipeline := rgrpc.Pipeline{
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 2,
			Backoff:     retry.ExponentialBackoff,
			Budget:      budget,
		}),
	}

	myInterceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(myInterceptor),
	)
	if err != nil {
		t.Fatalf("dial fail: %v", err)
	}
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})

	// After 2 attempts fail, we expect ErrRetry if the policy exhausted attempts.
	if !errors.Is(err, retry.ErrRetry) {
		t.Fatalf("Expected ErrRetry, got %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected 2 attempts, got %d", count)
	}
}

// TestRetryPerTry verifies the per-attempt timeout logic.
func TestRetryPerTry(t *testing.T) {
	count := 0
	lis, stop := fakeServer(t, func() error {
		count++
		// Server takes 2 seconds, while per-try timeout is only 1 second.
		time.Sleep(2 * time.Second)
		return status.Error(codes.Unavailable, "overloaded")
	})
	defer stop()

	pipeline := rgrpc.Pipeline{
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 2,
			TryDeadline: 1 * time.Second, // Each attempt limited to 1s.
		}),
	}

	myInterceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(myInterceptor),
	)
	if err != nil {
		t.Fatalf("dial fail: %v", err)
	}
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})

	if !errors.Is(err, retry.ErrTimeout) {
		t.Fatalf("Expected timeout error, got %v", err)
	}
}

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"Nil error", nil, false},
		{"DeadlineExceeded", status.Error(codes.DeadlineExceeded, ""), true},
		{"Internal", status.Error(codes.Internal, ""), true},
		{"Unavailable", status.Error(codes.Unavailable, ""), true},
		{"ResourceExhausted", status.Error(codes.ResourceExhausted, ""), true},
		{"InvalidArgument", status.Error(codes.InvalidArgument, ""), false},
		{"NotFound", status.Error(codes.NotFound, ""), false},
		{"Non-gRPC error", errors.New("generic error"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rgrpc.ShouldRetryDefault(tt.err); got != tt.expected {
				t.Errorf("ShouldRetryDefault() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// BenchmarkRetry measures the memory overhead of the retry middleware.
// This middleware is designed to be zero-allocation during execution.
//
// BenchmarkRetry/Retry_Overhead-12         	11231556	       107.5 ns/op	       0 B/op	       0 allocs/op
func BenchmarkRetry(b *testing.B) {
	pipeline := rgrpc.Pipeline{
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 2,
		}),
	}
	interceptor := rgrpc.NewUnaryClientInterceptor(pipeline)
	mockInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	b.Run("Retry_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = interceptor(context.Background(), "/test", nil, nil, nil, mockInvoker)
		}
	})
}
