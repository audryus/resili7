package rws_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

// TestWsTimeoutRead verifies that the read timeout correctly interrupts slow reads
// and returns the expected ErrReadTimeout.
func TestWsTimeoutRead(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("Upgrade error: %v", err)
			return
		}
		defer conn.Close()

		// Server delays response, causing client read timeout.
		time.Sleep(2 * time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	pipeline := rws.Pipeline{
		Connector: func() (*websocket.Conn, error) {
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			return conn, err
		},
		Timeout: rws.NewTimeoutMiddleware(rws.TimeoutConfig{
			Read: 1 * time.Second,
		}),
	}

	client, err := rws.NewClient(pipeline)
	if err != nil {
		t.Fatalf("Client should have been created: %v", err)
	}

	_, err = client.Send(rws.Request{MessageType: websocket.TextMessage, Data: []byte("ping")})
	if !errors.Is(err, rws.ErrReadTimeout) {
		t.Fatalf("Expected read timeout error, got %v", err)
	}
}

// TestWsTimeoutNoConnection verifies that the timeout middleware doesn't interfere
// with errors that occur before the read (e.g., connection issues).
func TestWsTimeoutNoConnection(t *testing.T) {
	pipeline := rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return nil, io.EOF
		},
		Timeout: rws.NewTimeoutMiddleware(rws.TimeoutConfig{
			Read: 1 * time.Second,
		}),
	}

	client, err := rws.NewClient(pipeline)
	if err != nil {
		t.Fatalf("Client should have been created: %v", err)
	}

	_, err = client.Send(rws.Request{MessageType: websocket.TextMessage, Data: []byte("ping")})
	if err != io.EOF {
		t.Fatalf("Expected EOF error, got %v", err)
	}
}

// TestWsTimeoutSessionDeadline verifies that the session timeout correctly
// interrupts requests that exceed the total session duration.
func TestWsTimeoutSessionDeadline(t *testing.T) {
	pipeline := rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			// Handler takes longer than session timeout.
			time.Sleep(100 * time.Millisecond)
			return &rws.Response{MessageType: websocket.TextMessage, Data: []byte("ok")}, nil
		},
		Timeout: rws.NewTimeoutMiddleware(rws.TimeoutConfig{
			Session: 50 * time.Millisecond,
		}),
	}

	client, err := rws.NewClient(pipeline)
	if err != nil {
		t.Fatalf("Client should have been created: %v", err)
	}

	_, err = client.Send(rws.Request{MessageType: websocket.TextMessage, Data: []byte("ping")})
	if !errors.Is(err, rws.ErrSessionTimeout) {
		t.Fatalf("Expected session timeout, got %v", err)
	}
}

// BenchmarkWsTimeout measures the baseline overhead of the timeout middleware.
// This middleware is designed to be zero-allocation when the deadline is not exceeded.
//
// BenchmarkWsTimeout/Optimized-12         	15207934        79.25 ns/op	       0 B/op	       0 allocs/op
func BenchmarkWsTimeout(b *testing.B) {
	okData := []byte("ok")
	resp := &rws.Response{MessageType: 1, Data: okData}
	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return resp, nil
		},
		Timeout: rws.NewTimeoutMiddleware(rws.TimeoutConfig{
			Read: 10 * time.Second,
		}),
	})

	b.Run("Optimized", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		req := rws.Request{
			MessageType: websocket.TextMessage,
			Data:       okData,
		}
		for i := 0; i < b.N; i++ {
			_, _ = client.Send(req)
		}
	})
}
