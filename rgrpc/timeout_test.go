package rgrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rgrpc"
	"codeberg.org/audryus/resili7/timeout"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
)

// TestTimeout verifies that the global timeout correctly interrupts slow requests
// and returns the expected ErrTimeout.
func TestTimeout(t *testing.T) {
	lis, stop := fakeServer(t, func() error {
		time.Sleep(2 * time.Second)
		return nil
	})
	defer stop()

	pipeline := rgrpc.Pipeline{
		Timeout: rgrpc.NewTimeoutMiddleware(1 * time.Second),
	}

	myInterceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	// O Dial precisa saber o endereço que o listener pegou (lis.Addr())
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(myInterceptor),
	)
	if err != nil {
		t.Fatalf("dial fail: %v", err)
	}
	defer conn.Close()

	// 4. Fazer a chamada real
	client := grpc_testing.NewTestServiceClient(conn)

	// Chamada vazia (Empty é um tipo do pacote grpc_testing)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})

	if err == nil {
		t.Fatalf("Expected some error")
	}

	if !errors.Is(err, timeout.ErrTimeout) {
		t.Fatalf("Expected timeout error, got %v", err)
	}
}

// BenchmarkTimeout measures the baseline overhead of the timeout middleware.
// This middleware is designed to be zero-allocation when the deadline is not exceeded.
//
// BenchmarkTimeout/Timeout_Overhead-12         	11311341	       106.4 ns/op	       0 B/op	       0 allocs/op
func BenchmarkTimeout(b *testing.B) {
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
