package rws_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

// TestWsHedgeWithVariableLatency verifies that hedging successfully reduces tail latency
// by firing parallel dial attempts when the primary connection is slow.
func TestWsHedgeWithVariableLatency(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	var dialCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := dialCount.Add(1)

		// First dial attempt is slow (100ms), subsequent attempts are fast.
		if attempt == 1 {
			time.Sleep(100 * time.Millisecond)
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Echo messages.
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			conn.WriteMessage(mt, msg)
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	client, err := rws.NewClient(rws.Pipeline{
		Connector: func() (*websocket.Conn, error) {
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			return conn, err
		},
		Hedge: rws.NewHedgeMiddleware(websocket.DefaultDialer.DialContext, 50*time.Millisecond, 2),
	})
	if err != nil {
		t.Fatalf("Client should have been created: %+v", err)
	}

	start := time.Now()
	resp, err := client.Send(rws.Request{
		URL:         wsURL,
		MessageType: websocket.TextMessage,
		Data:       []byte("ping"),
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if resp == nil {
		t.Fatal("Expected response")
	}

	// Logic check: total duration should be ~50ms (hedge delay) + ~10ms = ~60ms.
	// Without hedging, it would have taken 100ms (the slow first dial).
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Hedge didn't optimize latency: took %v", elapsed)
	}

	// Verify that at least 2 dial attempts were fired.
	if dialCount.Load() < 2 {
		t.Fatalf("Expected at least 2 attempts with hedge, got %d", dialCount.Load())
	}
}

// TestWsHedgeDisabledWithInvalidParams ensures that the middleware gracefully degrades
// to a simple pass-through if parameters are invalid.
func TestWsHedgeDisabledWithInvalidParams(t *testing.T) {
	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return &rws.Response{MessageType: websocket.TextMessage, Data: []byte("ok")}, nil
		},
		// Invalid parameters: 0 delay or <=1 max attempts disables hedging.
		Hedge: rws.NewHedgeMiddleware(websocket.DefaultDialer.DialContext, 0, 1),
	})

	_, err := client.Send(rws.Request{MessageType: websocket.TextMessage, Data: []byte("ok")})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

// BenchmarkWsHedge measures the overhead of the hedging middleware itself.
// Since hedging involves starting goroutines, it is expected to have allocations.
//
// BenchmarkWsHedge/Hedge_Overhead-12         	  599160	      1872 ns/op	     344 B/op	       6 allocs/op
func BenchmarkWsHedge(b *testing.B) {
	okData := []byte("ok")
	resp := &rws.Response{MessageType: websocket.TextMessage, Data: okData}

	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return resp, nil
		},
		Hedge: rws.NewHedgeMiddleware(websocket.DefaultDialer.DialContext, 10*time.Second, 2),
	})

	req := rws.Request{URL: "/test", MessageType: websocket.TextMessage, Data: okData}

	b.Run("Hedge_Overhead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = client.Send(req)
		}
	})
}
