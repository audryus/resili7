package retry

import (
	"time"
)

// RetryPolicy aggregates all settings for the retry middleware.
type RetryPolicy[R any] struct {
	Backoff     func(attempt int) time.Duration // Strategy to calculate delay between attempts.
	ShouldRetry func(R, error) bool             // Optional strategy to decide if a retry is warranted.
	Budget      *Budget                         // Optional budget to limit total retries.
	MaxAttempts int                             // Maximum number of attempts (initial + retries).
	TryDeadline time.Duration                   // Maximum duration for a single attempt.
}

// ExponentialBackoff returns a strategy that doubles the delay with each attempt (1s, 2s, 4s, etc.).
var ExponentialBackoff = func(attempt int) time.Duration {
	return time.Duration(1<<uint(attempt)) * time.Second
}

var LinearBackoff = func(attempt int) time.Duration {
	return time.Duration(attempt) * time.Second
}
