package rhttp_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/breaker"
	"codeberg.org/audryus/resili7/limiter"
	"codeberg.org/audryus/resili7/retry"
	"codeberg.org/audryus/resili7/rhttp"
)

func TestNewClient(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(10))
	cb := breaker.NewBreaker()

	pipeline := rhttp.Pipeline{
		HttpCient:      http.DefaultClient,
		Timeout:        rhttp.NewTimeoutMiddleware(1 * time.Second),
		Retry:          rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{MaxAttempts: 3}),
		Hedge:          rhttp.NewHedgeMiddleware(100*time.Millisecond, 2),
		Limiter:        rhttp.NewLimiterMiddleware(l),
		CircuitBreaker: rhttp.NewBreakerMiddleware(cb),
	}

	client, err := rhttp.NewClient(pipeline)
	if err != nil {
		t.Fatalf("Expected nil error, got %v", err)
	}

	if client == nil {
		t.Fatal("Expected client to be non-nil")
	}
}

// TestPipelineErrors verifies that errors from the handler correctly propagate through the chain.
func TestPipelineErrors(t *testing.T) {
	expectedErr := errors.New("custom failure")
	client, _ := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return nil, expectedErr
		},
		Retry: rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{MaxAttempts: 1}),
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost", nil)
	_, err := client.Do(req)

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected %v, got %v", expectedErr, err)
	}
}
