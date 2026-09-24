package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/audryus/resili7/breaker"
	"github.com/audryus/resili7/limiter"
	"github.com/audryus/resili7/retry"
	"github.com/audryus/resili7/rhttp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
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

	pipeline := rhttp.Pipeline{
		HttpClient: http.DefaultClient,
		Timeout:    rhttp.NewTimeoutMiddleware(10 * time.Second),
		Retry: rhttp.NewRetryMiddleware(&retry.RetryPolicy[*http.Response]{
			MaxAttempts: 3,
			Backoff:     retry.ExponentialBackoff,
			ShouldRetry: rhttp.ShouldRetryDefault,
			Budget:      budget,
			TryDeadline: 2 * time.Second,
		}),
		Hedge:          rhttp.NewHedgeMiddleware(50*time.Millisecond, 3),
		Limiter:        rhttp.NewLimiterMiddleware(l),
		CircuitBreaker: rhttp.NewBreakerMiddleware(cb),
		Classifier:     rhttp.NewClassifierMiddleware(),
	}

	client, err := rhttp.NewClient(pipeline)
	if err != nil {
		return fmt.Errorf("client creation failed: %w", err)
	}

	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/zen", nil)
	if err != nil {
		return fmt.Errorf("request creation failed: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body failed: %w", err)
	}

	fmt.Printf("HTTP %d | %d bytes\n", resp.StatusCode, len(body))
	fmt.Println("HTTP example completed successfully")
	return nil
}
