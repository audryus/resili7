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

// Option defines a functional configuration for a RetryPolicy.
type Option func(*RetryPolicy[any])

// WithMaxAttempts sets the maximum number of attempts (initial + retries).
func WithMaxAttempts(n int) Option {
	return func(p *RetryPolicy[any]) { p.MaxAttempts = n }
}

// WithTryDeadline sets the maximum duration for a single attempt.
func WithTryDeadline(d time.Duration) Option {
	return func(p *RetryPolicy[any]) { p.TryDeadline = d }
}

// WithBackoff sets the strategy to calculate delay between attempts.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(p *RetryPolicy[any]) { p.Backoff = fn }
}

// WithShouldRetry sets the strategy to decide if a retry is warranted.
func WithShouldRetry(fn func(any, error) bool) Option {
	return func(p *RetryPolicy[any]) { p.ShouldRetry = fn }
}

// WithBudget sets the retry budget to limit total retries.
func WithBudget(b *Budget) Option {
	return func(p *RetryPolicy[any]) { p.Budget = b }
}

// NewRetryPolicy creates a new RetryPolicy with default or custom options.
func NewRetryPolicy(opts ...Option) *RetryPolicy[any] {
	p := &RetryPolicy[any]{
		Backoff:     LinearBackoff,
		ShouldRetry: OnErrorRetry,
		MaxAttempts: 3,
		TryDeadline: 30 * time.Second,
	}

	for _, opt := range opts {
		opt((*RetryPolicy[any])(p))
	}

	return p
}

var OnErrorRetry = func(_ any, err error) bool {
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
