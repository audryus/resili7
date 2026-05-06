package retry

import (
	"context"
	"errors"
	"net/http"
	"time"

	"codeberg.org/audryus/resili7"
)

var ErrTimeout = errors.New("request timeout")
var ErrRetry = errors.New("exausted retry attempts")

// NewRetryMiddleware creates a middleware that implements sequential retries.
// It uses a provided RetryPolicy to manage backoff, budgets, and per-try deadlines.
func NewRetryMiddleware(policy *RetryPolicy) resili7.Middleware {
	maxAttempts := policy.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	backoffFn := policy.Backoff
	if backoffFn == nil {
		backoffFn = LinearBackoff
	}
	shouldRetryFn := policy.ShouldRetry
	if shouldRetryFn == nil {
		shouldRetryFn = ShouldRetryDefault
	}

	budget := policy.Budget
	perTryTimeout := int64(policy.TryDeadline)

	return func(next resili7.Handler) resili7.Handler {
		return func(req resili7.Request) (*http.Response, error) {
			var resp *http.Response
			var err error

			// Calculate the per-try deadline once per request start.
			var tryDeadline int64
			if perTryTimeout > 0 {
				tryDeadline = req.Now + perTryTimeout
			}

			for attempt := range maxAttempts {
				// Enforce per-try timeout before starting the attempt.
				if tryDeadline > 0 && req.Now > tryDeadline {
					return nil, ErrTimeout
				}

				// Execute the next handler in the chain.
				resp, err = next(req)

				// Update cached time after a potentially slow network call.
				req.Now = time.Now().UnixNano()

				// Success condition: No error and status code is successful (< 400).
				if err == nil && resp != nil && resp.StatusCode < 400 {
					if budget != nil {
						budget.RecordSuccess()
					}
					return resp, nil
				}

				// Check if we should even attempt a retry based on the error/response.
				if !shouldRetryFn(resp, err) {
					return resp, err
				}

				// Verify the retry budget to prevent overloading the downstream service.
				if budget != nil && !budget.AllowRetry() {
					return resp, ErrRetry
				}

				// Calculate and wait for backoff delay.
				delay := backoffFn(attempt)

				// Respect global RequestDeadline during backoff sleep.
				if req.RequestDeadline > 0 {
					remaining := req.RequestDeadline - req.Now
					if remaining <= 0 {
						return nil, ErrTimeout
					}
					if int64(delay) > remaining {
						delay = time.Duration(remaining)
					}
				}

				time.Sleep(delay)
				// Refresh time after sleeping.
				req.Now = time.Now().UnixNano()
			}

			// If we exhausted all attempts:
			// Return the actual network error if present, otherwise return ErrRetry.
			if err == nil {
				return resp, ErrRetry
			}
			return resp, err
		}
	}
}

// PerTryTimeoutMiddleware provides an alternative way to enforce timeouts per attempt.
// This specific implementation uses context.WithTimeout to cancel network calls.
func PerTryTimeoutMiddleware(duration time.Duration) resili7.Middleware {
	return func(next resili7.Handler) resili7.Handler {
		return func(req resili7.Request) (*http.Response, error) {
			req.TryDeadline = req.Now + int64(duration)

			ctx, cancel := context.WithTimeout(req.Req.Context(), duration)
			defer cancel()

			req.Req = req.Req.WithContext(ctx)

			resp, err := next(req)
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				return nil, ErrTimeout
			}
			return resp, err
		}
	}
}
