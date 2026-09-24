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

// Option defines a functional configuration for a RetryPolicy of type R.
// It is generic so typed policies (e.g. RetryPolicy[*http.Response]) can be
// built without casts.
type Option[R any] func(*RetryPolicy[R])

// WithMaxAttempts sets the maximum number of attempts (initial + retries).
func WithMaxAttempts[R any](n int) Option[R] {
	return func(p *RetryPolicy[R]) { p.MaxAttempts = n }
}

// WithTryDeadline sets the maximum duration for a single attempt.
func WithTryDeadline[R any](d time.Duration) Option[R] {
	return func(p *RetryPolicy[R]) { p.TryDeadline = d }
}

// WithBackoff sets the strategy to calculate delay between attempts.
func WithBackoff[R any](fn func(attempt int) time.Duration) Option[R] {
	return func(p *RetryPolicy[R]) { p.Backoff = fn }
}

// WithShouldRetry sets the strategy to decide if a retry is warranted.
func WithShouldRetry[R any](fn func(R, error) bool) Option[R] {
	return func(p *RetryPolicy[R]) { p.ShouldRetry = fn }
}

// WithBudget sets the retry budget to limit total retries.
func WithBudget[R any](b *Budget) Option[R] {
	return func(p *RetryPolicy[R]) { p.Budget = b }
}

// NewRetryPolicy creates a new RetryPolicy with default or custom options.
func NewRetryPolicy[R any](opts ...Option[R]) *RetryPolicy[R] {
	p := &RetryPolicy[R]{
		Backoff:     LinearBackoff,
		MaxAttempts: 3,
		TryDeadline: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.ShouldRetry == nil {
		p.ShouldRetry = OnErrorRetry[R]
	}

	return p
}

// OnErrorRetry retries whenever the attempt returned an error.
func OnErrorRetry[R any](_ R, err error) bool {
	return err != nil
}

// ExponentialBackoff returns a strategy that doubles the delay with each attempt (1s, 2s, 4s, etc.).
var ExponentialBackoff = func(attempt int) time.Duration {
	return time.Duration(1<<uint(attempt)) * time.Second
}

// LinearBackoff returns a strategy that increases delay linearly with each attempt.
var LinearBackoff = func(attempt int) time.Duration {
	return time.Duration(attempt) * time.Second
}
