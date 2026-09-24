package rgrpc_test

import (
	"context"
	"errors"
	"testing"

	"github.com/audryus/resili7/retry"
	"github.com/audryus/resili7/rgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
)

// C2: exhausted gRPC retries must preserve the last network error
// (codes.Unavailable), not replace it with ErrRetry.
func TestGrpcRetryPreservesNetworkError(t *testing.T) {
	count := 0
	lis, stop := fakeServer(t, func() error {
		count++
		return status.Error(codes.Unavailable, "overloaded")
	})
	defer stop()

	budget := retry.NewBudget(0.5)
	for range 25 {
		budget.RecordSuccess()
	}

	pipeline := rgrpc.Pipeline{
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 2,
			Backoff:     retry.LinearBackoff,
			Budget:      budget,
		}),
	}

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(rgrpc.NewUnaryClientInterceptor(pipeline)),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable preserved, got %v", err)
	}
	if !errors.Is(err, retry.ErrRetry) {
		t.Fatalf("expected errors.Is(err, ErrRetry), got %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 attempts, got %d", count)
	}
}
