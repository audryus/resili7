package rws_test

import (
	"errors"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

// The breaker middleware passes successes through and rejects while open.
func TestWsBreakerMiddleware(t *testing.T) {
	cb := breaker.NewBreaker(
		breaker.WithMinRequests(1),
		breaker.WithErrorThreshold(0.1),
		breaker.WithWindow(time.Hour),
	)
	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			return &rws.Response{MessageType: websocket.TextMessage, Data: []byte("ok")}, nil
		},
		CircuitBreaker: rws.NewBreakerMiddleware(cb),
	})
	resp, err := client.Send(rws.Request{MessageType: websocket.TextMessage})
	if err != nil || string(resp.Data) != "ok" {
		t.Fatalf("expected passthrough, got %+v %v", resp, err)
	}
}

// Exhausted WS retries join ErrRetry with the causal error and exercise the
// default retry predicate.
func TestWsRetryExhaustionJoinsError(t *testing.T) {
	var calls int
	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			calls++
			return nil, websocket.ErrCloseSent
		},
		Retry: rws.NewRetryMiddleware(&retry.RetryPolicy[*rws.Response]{
			MaxAttempts: 2,
			Backoff:     func(_ int) time.Duration { return 0 },
		}),
	})
	_, err := client.Send(rws.Request{MessageType: websocket.TextMessage})
	if !errors.Is(err, retry.ErrRetry) {
		t.Fatalf("expected ErrRetry, got %v", err)
	}
	if !errors.Is(err, websocket.ErrCloseSent) {
		t.Fatalf("expected causal error preserved, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", calls)
	}
}

// Pipeline MessageType default applies when the request leaves it unset,
// and an explicit request type wins.
func TestWsMessageTypeDefault(t *testing.T) {
	var got int
	client, _ := rws.NewClient(rws.Pipeline{
		WsHandler: func(r rws.Request) (*rws.Response, error) {
			got = r.MessageType
			return &rws.Response{MessageType: r.MessageType}, nil
		},
		MessageType: websocket.BinaryMessage,
	})
	if _, err := client.Send(rws.Request{}); err != nil {
		t.Fatal(err)
	}
	if got != websocket.BinaryMessage {
		t.Fatalf("expected default BinaryMessage, got %d", got)
	}
	if _, err := client.Send(rws.Request{MessageType: websocket.TextMessage}); err != nil {
		t.Fatal(err)
	}
	if got != websocket.TextMessage {
		t.Fatalf("explicit type must win, got %d", got)
	}
}
