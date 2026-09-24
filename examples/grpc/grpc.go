package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
)

type testServer struct {
	grpc_testing.UnimplementedTestServiceServer
	callCount int
	failUntil int
}

func (s *testServer) EmptyCall(ctx context.Context, req *grpc_testing.Empty) (*grpc_testing.Empty, error) {
	s.callCount++
	if s.failUntil > 0 && s.callCount <= s.failUntil {
		return nil, status.Error(codes.Unavailable, "simulated transient failure")
	}
	return &grpc_testing.Empty{}, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return fmt.Errorf("listen failed: %w", err)
	}

	srv := grpc.NewServer()
	grpc_testing.RegisterTestServiceServer(srv, &testServer{
		failUntil: 2,
	})
	go srv.Serve(lis)
	defer srv.Stop()

	cb := breaker.NewBreaker(
		breaker.WithTimeout(5*time.Second),
		breaker.WithWindow(10*time.Second),
		breaker.WithMinRequests(5),
		breaker.WithErrorThreshold(0.5),
	)

	l := limiter.NewLimiter(
		limiter.WithInitialLimit(100),
		limiter.WithLimits(10, 1000),
	)
	defer l.Close()

	budget := retry.NewBudget(0.5)
	for i := 0; i < 50; i++ {
		budget.RecordSuccess()
	}

	pipeline := rgrpc.Pipeline{
		Timeout: rgrpc.NewTimeoutMiddleware(10 * time.Second),
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 3,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: func(_ rgrpc.Request, err error) bool {
				return rgrpc.ShouldRetryDefault(err)
			},
			Budget:      budget,
			TryDeadline: 2 * time.Second,
		}),
		Hedge:          rgrpc.NewHedgeMiddleware(50*time.Millisecond, 3),
		Limiter:        rgrpc.NewLimiterMiddleware(l),
		CircuitBreaker: rgrpc.NewBreakerMiddleware(cb),
	}

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(rgrpc.NewUnaryClientInterceptor(pipeline)),
	)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.EmptyCall(ctx, &grpc_testing.Empty{})
	if err != nil {
		return fmt.Errorf("gRPC call failed: %w", err)
	}

	_ = resp

	fmt.Println("gRPC call succeeded")
	fmt.Println("gRPC example completed successfully")

	return nil
}
