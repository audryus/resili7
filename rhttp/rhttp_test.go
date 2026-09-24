package rhttp_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/audryus/resili7/breaker"
	"github.com/audryus/resili7/limiter"
	"github.com/audryus/resili7/retry"
	"github.com/audryus/resili7/rhttp"
)

func TestHttpNewClient(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(10))
	cb := breaker.NewBreaker()

	pipeline := rhttp.Pipeline{
		HttpClient:     http.DefaultClient,
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

// TestHttpPipelineErrors verifies that errors from the handler correctly propagate through the chain.
func TestHttpPipelineErrors(t *testing.T) {
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
