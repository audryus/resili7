package rws_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

// TestWsLimiter verifies the end-to-end behavior of the limiter middleware.
// It uses a mock server and confirms that requests are rejected with ErrLimited
// when the concurrency capacity is exceeded.
func TestWsLimiter(t *testing.T) {
	// Create a limiter with capacity for only 1 concurrent request.
	l := limiter.NewLimiter(limiter.WithInitialLimit(1))

	// Manually exhaust the limiter.
	l.Acquire(time.Now().UnixNano())

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Echo logic
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		conn.WriteMessage(mt, msg)
	}))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	client, _ := rws.NewClient(rws.Pipeline{
		Connector: func() (*websocket.Conn, error) {
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			return conn, err
		},
		Limiter: rws.NewLimiterMiddleware(l),
	})

	// Attempt a request - should be rejected immediately by the middleware.
	_, err := client.Send(rws.Request{MessageType: websocket.TextMessage, Data: []byte("ping")})

	if !errors.Is(err, limiter.ErrLimited) {
		t.Fatalf("Expected rate limited error, got %v", err)
	}
}

// BenchmarkWsLimiter measures the baseline overhead of the adaptive limiter.
// This is designed to be highly efficient and zero-allocation on the hot path.
//
// BenchmarkWsLimiter/Limiter_Overhead-12         	14265361	        82.90 ns/op	       0 B/op	       0 allocs/op
func BenchmarkWsLimiter(b *testing.B) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1000000))
	okData := []byte("ok")
	resp := &rws.Response{MessageType: websocket.TextMessage, Data: okData}

	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return resp, nil
		},
		Limiter: rws.NewLimiterMiddleware(l),
	})

	req := rws.Request{MessageType: websocket.TextMessage, Data: okData}

	b.Run("Limiter_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Send(req)
		}
	})
}
