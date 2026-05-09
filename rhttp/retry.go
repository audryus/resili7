package rhttp

import (
	"net/http"

	"codeberg.org/audryus/resili7/retry"
)

// NewRetryMiddleware creates a middleware that implements sequential retries.
// It uses a provided RetryPolicy to manage backoff, budgets, and per-try deadlines.
func NewRetryMiddleware(policy *retry.RetryPolicy[*http.Response]) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*http.Response, error) {
			return retry.ExecuteWithResult(policy, action{
				h:   next,
				req: req,
			})
		}
	}
}

func (a action) IsSuccess(resp *http.Response, err error) bool {
	return err == nil && resp != nil && resp.StatusCode < 400
}

func (a action) ShouldRetryDefault(resp *http.Response, err error) bool {
	return ShouldRetryDefault(resp, err)
}
func (a action) Err(err error) error {
	if err == nil {
		return retry.ErrRetry
	}
	return err
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
