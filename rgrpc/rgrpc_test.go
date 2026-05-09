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
	// 1. Criar um Listener em uma porta aleatória (porta 0)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("falha ao escutar: %v", err)
	}

	// 2. Criar o servidor gRPC com comportamento controlado
	srv := grpc.NewServer()

	// Registramos um serviço "falso" do pacote de testes
	grpc_testing.RegisterTestServiceServer(srv, &mockServer{
		onCall: onCall,
	})

	// Iniciar o servidor em uma goroutine
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("Servidor finalizado: %v", err)
		}
	}()

	return lis, srv.Stop
}

// TestIntegrationWithServer testa o fluxo real de Dial -> Request -> Retry
func TestIntegrationWithServer(t *testing.T) {
	callCount := 0

	lis, stop := fakeServer(t, func() error {
		callCount++
		t.Logf("Servidor chamado pela %d vez\n", callCount)

		if callCount < 3 {
			// Falha nas 2 primeiras tentativas (Simula servidor instável)
			return status.Error(codes.Unavailable, "estou fora do ar")
		}
		// Sucesso na 3ª
		return nil
	})
	defer stop()
	// 3. Criar o Client com nosso Interceptor de Resiliência
	pipeline := rgrpc.Pipeline{
		Retry: rgrpc.NewRetryMiddleware(&retry.RetryPolicy[rgrpc.Request]{
			MaxAttempts: 3,
		}),
	}

	myInterceptor := rgrpc.NewUnaryClientInterceptor(pipeline)

	// O Dial precisa saber o endereço que o listener pegou (lis.Addr())
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(myInterceptor),
	)

	if err != nil {
		t.Fatalf("falha ao dial: %v", err)
	}
	defer conn.Close()

	// 4. Fazer a chamada real
	client := grpc_testing.NewTestServiceClient(conn)

	// Chamada vazia (Empty é um tipo do pacote grpc_testing)
	_, err = client.EmptyCall(context.Background(), &grpc_testing.Empty{})

	// 5. Validações
	if err != nil {
		t.Errorf("Esperado sucesso após retries, obteve erro: %v", err)
	}
	if callCount != 3 {
		t.Errorf("Esperado 3 chamadas ao servidor, obteve %d", callCount)
	}
}

// mockServer implementa a interface do servidor de testes
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
