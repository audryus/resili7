package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if err := conn.WriteMessage(mt, msg); err != nil {
			return
		}
	}
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
	server := http.Server{Handler: http.HandlerFunc(echoHandler)}
	go server.Serve(lis)
	defer server.Close()

	wsURL := "ws://" + lis.Addr().String()

	time.Sleep(100 * time.Millisecond)

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
	for range 50 {
		budget.RecordSuccess()
	}

	pipeline := rws.Pipeline{
		Connector: func() (*websocket.Conn, error) {
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			return conn, err
		},
		Timeout: rws.NewTimeoutMiddleware(rws.TimeoutConfig{
			Dial:    5 * time.Second,
			Read:    10 * time.Second,
			Session: 30 * time.Minute,
		}),
		Retry: rws.NewRetryMiddleware(&retry.RetryPolicy[*rws.Response]{
			MaxAttempts: 3,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: rws.ShouldRetryDefault,
			Budget:      budget,
			TryDeadline: 2 * time.Second,
		}),
		Hedge:          rws.NewHedgeMiddleware(websocket.DefaultDialer.DialContext, 50*time.Millisecond, 3),
		Limiter:        rws.NewLimiterMiddleware(l),
		CircuitBreaker: rws.NewBreakerMiddleware(cb),
		MessageType:    websocket.TextMessage,
	}

	client, err := rws.NewClient(pipeline)
	if err != nil {
		return fmt.Errorf("client creation failed: %w", err)
	}

	resp, err := client.Send(rws.Request{
		Data:        []byte("hello from resili7"),
		MessageType: websocket.TextMessage,
		URL:         wsURL,
	})
	if err != nil {
		return fmt.Errorf("WebSocket send failed: %w", err)
	}

	fmt.Printf("WebSocket response: %s\n", resp.Data)
	fmt.Println("WebSocket example completed successfully")

	return nil
}
