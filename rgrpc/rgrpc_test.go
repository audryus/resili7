package rgrpc_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"

	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rgrpc"
)

func fakeServer(t *testing.T, onCall func() error) (net.Listener, func()) {
	// Create a listener on a random port (port 0)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	// Create the gRPC server with controlled behavior
	srv := grpc.NewServer()

	// Register a "mock" service from the test package
	grpc_testing.RegisterTestServiceServer(srv, &mockServer{
		onCall: onCall,
	})

	// Start the server in a goroutine
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	return lis, srv.Stop
}

// TestGrpcIntegrationWithServer tests the real flow of Dial -> Request -> Retry
func TestGrpcIntegrationWithServer(t *testing.T) {
	callCount := 0

	lis, stop := fakeServer(t, func() error {
		callCount++

		if callCount < 3 {
			// Fail on first 2 attempts (simulates unstable server)
			return status.Error(codes.Unavailable, "service unavailable")
		}
		// Succeed on 3rd attempt
		return nil
	})
	defer stop()
	// Create the client with our resilience interceptor
	pipeline := rgrpc.Pipeline{
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 3,
		}),
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

	// Validate
	if err != nil {
		t.Errorf("Expected success after retries, got error: %v", err)
	}
	if callCount != 3 {
		t.Errorf("Expected 3 server calls, got %d", callCount)
	}
}

// mockServer implements the test service server interface
type mockServer struct {
	grpc_testing.UnimplementedTestServiceServer
	onCall func() error
}

func (m *mockServer) EmptyCall(ctx context.Context, in *grpc_testing.Empty) (*grpc_testing.Empty, error) {
	if m.onCall != nil {
		if err := m.onCall(); err != nil {
			return nil, err
		}
	}
	return &grpc_testing.Empty{}, nil
}
