package retry

import (
	"net/http"
	"time"
)

// ShouldRetryStrategy defines a function that determines if an operation should be retried
// based on the HTTP response and any error encountered.
type ShouldRetryStrategy func(*http.Response, error) bool

// BackoffStrategy defines a function that returns the delay duration for a specific retry attempt.
type BackoffStrategy func(attempt int) time.Duration

// RetryPolicy aggregates all settings for the retry middleware.
type RetryPolicy struct {
	Backoff     BackoffStrategy     // Strategy to calculate delay between attempts.
	ShouldRetry ShouldRetryStrategy // Strategy to decide if a retry is warranted.
	Budget      *Budget             // Optional budget to limit total retries.
	MaxAttempts int                 // Maximum number of attempts (initial + retries).
	TryDeadline time.Duration       // Maximum duration for a single attempt.
}

// NewPolicy creates a default retry policy with standard settings.
func NewPolicy(maxAttempts int) *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts: maxAttempts,
		Backoff:     ExponentialBackoff,
		ShouldRetry: ShouldRetryDefault,
	}
}

// ExponentialBackoff returns a strategy that doubles the delay with each attempt (1s, 2s, 4s, etc.).
var ExponentialBackoff = func(attempt int) time.Duration {
	return time.Duration(1<<uint(attempt)) * time.Second
}

var LinearBackoff = func(attempt int) time.Duration {
	return time.Duration(attempt) * time.Second
}

// ShouldRetryDefault returns a strategy that retries on network errors (err != nil)
// or server-side HTTP errors (status >= 500), except for common rate-limiting signals.
var ShouldRetryDefault = func(response *http.Response, err error) bool {
	if err != nil {
		return true
	}

	if response == nil {
		return true
	}

	switch response.StatusCode {
	case http.StatusRequestTimeout, // 408
		http.StatusTooEarly,            // 425
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	}

	return false
}
