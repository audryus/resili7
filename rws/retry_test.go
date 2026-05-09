package rws_test

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

// TestWsRetry verifies that the middleware correctly retries failed WebSocket operations
// when connecting to a real (fake) server that occasionally fails.
func TestWsRetry(t *testing.T) {
	serverCount := 0
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer conn.Close()

		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			serverCount++
			if serverCount == 1 {
				// Sends a close frame and terminates.
				conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "error"),
					time.Now().Add(time.Second))
				return
			}
			conn.WriteMessage(mt, msg)
		}
	}))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// We use the Connector instead of a fixed connection.
	// The PersistentHandler (created internally by NewClient) will call this function on each attempt.
	client, err := rws.NewClient(rws.Pipeline{
		Connector: func() (*websocket.Conn, error) {
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			return conn, err
		},
		Retry: rws.NewRetryMiddleware(&retry.RetryPolicy[*rws.Response]{
			MaxAttempts: 2,
			Backoff:     func(n int) time.Duration { return 1 * time.Millisecond },
			ShouldRetry: rws.ShouldRetryDefault,
		}),
	})

	if err != nil {
		t.Fatalf("Client should have been created: %v", err)
	}

	resp, err := client.Send(rws.Request{MessageType: websocket.TextMessage, Data: []byte("ping")})
	if err != nil {
		t.Fatalf("Expected success after retry, got %v", err)
	}
	if string(resp.Data) != "ping" {
		t.Fatalf("Expected 'ping' response, got %s", string(resp.Data))
	}
	if serverCount != 2 {
		t.Fatalf("Expected 2 attempts on server for first message, got %d", serverCount)
	}

	// Second send should reuse the connection established during the previous retry.
	resp, err = client.Send(rws.Request{MessageType: websocket.TextMessage, Data: []byte("ping2")})
	if err != nil {
		t.Fatalf("Expected success on second message, got %v", err)
	}
	if string(resp.Data) != "ping2" {
		t.Fatalf("Expected 'ping2' response, got %s", string(resp.Data))
	}
	if serverCount != 3 {
		t.Fatalf("Expected 3 attempts total (1 fail, 1 success-retry, 1 reuse), got %d", serverCount)
	}
}

// TestRetryBudget verifies that retries respect the budget when connecting to a real (fake) server.
func TestWsRetryBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	count := 0
	budget := retry.NewBudget(0.1)
	budget.RecordSuccess()

	client, _ := rws.NewClient(rws.Pipeline{
		Connector: func() (*websocket.Conn, error) {
			count++
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			return conn, err
		},
		Retry: rws.NewRetryMiddleware(&retry.RetryPolicy[*rws.Response]{
			MaxAttempts: 3,
			ShouldRetry: rws.ShouldRetryDefault,
			Budget:      budget,
		}),
	})

	_, err := client.Send(rws.Request{MessageType: websocket.TextMessage})

	// Budget should prevent the second attempt.
	if count != 1 {
		t.Fatalf("Expected only 1 attempt due to budget, got %d", count)
	}
	if !errors.Is(err, retry.ErrRetry) {
		t.Fatalf("Expected retry.ErrRetry, got %v", err)
	}
}

func TestWsRetryPolicy(t *testing.T) {
	tests := []struct {
		err      error
		resp     *rws.Response
		name     string
		expected bool
	}{
		{
			name:     "Network Timeout",
			resp:     nil,
			err:      &net.OpError{Op: "read", Err: errors.New("timeout")},
			expected: true,
		},
		{
			name:     "Close Service Restart",
			resp:     nil,
			err:      &websocket.CloseError{Code: websocket.CloseServiceRestart},
			expected: true,
		},
		{
			name:     "Close Try Again Later",
			resp:     nil,
			err:      &websocket.CloseError{Code: websocket.CloseTryAgainLater},
			expected: true,
		},
		{
			name:     "Abnormal Closure",
			resp:     nil,
			err:      &websocket.CloseError{Code: websocket.CloseAbnormalClosure},
			expected: true,
		},
		{
			name:     "Normal Closure (No Retry)",
			resp:     nil,
			err:      &websocket.CloseError{Code: websocket.CloseNormalClosure},
			expected: false,
		},
		{
			name:     "Going Away (No Retry)",
			resp:     nil,
			err:      &websocket.CloseError{Code: websocket.CloseGoingAway},
			expected: false,
		},
		{
			name:     "Success (No Retry)",
			resp:     &rws.Response{},
			err:      nil,
			expected: false,
		},
		{
			name:     "Unexpected EOF",
			resp:     nil,
			err:      io.ErrUnexpectedEOF,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rws.ShouldRetryDefault(tt.resp, tt.err); got != tt.expected {
				t.Errorf("ShouldRetryDefault() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// BenchmarkWsRetry measures the overhead of the retry middleware in rws.
// BenchmarkWsRetry/With_Retry_Overhead-12         	14997517	        81.48 ns/op	       0 B/op	       0 allocs/op
func BenchmarkWsRetry(b *testing.B) {
	okData := []byte("ok")
	resp := &rws.Response{MessageType: websocket.TextMessage, Data: okData}

	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return resp, nil
		},
		Retry: rws.NewRetryMiddleware(&retry.RetryPolicy[*rws.Response]{
			MaxAttempts: 2,
			Backoff:     func(n int) time.Duration { return 0 },
			ShouldRetry: rws.ShouldRetryDefault,
		}),
	})

	req := rws.Request{MessageType: websocket.TextMessage, Data: okData}

	b.Run("With_Retry_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Send(req)
		}
	})
}
