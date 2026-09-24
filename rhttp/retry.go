package rhttp

import (
	"errors"
	"net/http"

	"github.com/audryus/resili7/retry"
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

// IsSuccess reports whether the response represents a successful HTTP outcome.
func (a action) IsSuccess(resp *http.Response, err error) bool {
	return err == nil && resp != nil && resp.StatusCode < 400
}

// ShouldRetryDefault delegates retry decisions to the package-level default predicate.
func (a action) ShouldRetryDefault(resp *http.Response, err error) bool {
	return ShouldRetryDefault(resp, err)
}

// Err normalizes the final error returned after the retry loop is exhausted.
// It joins retry.ErrRetry with the last network error so errors.Is(err,
// retry.ErrRetry) still signals exhaustion while the causal error stays
// unwrappable. A nil error (e.g. exhausted HTTP 5xx retries) yields ErrRetry.
func (a action) Err(err error) error {
	if err == nil {
		return retry.ErrRetry
	}
	return errors.Join(retry.ErrRetry, err)
}

// DiscardAttempt closes the body of a response the retry loop will not
// return, preventing connection leaks from discarded attempts.
func (a action) DiscardAttempt(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}

// ShouldRetryDefault returns a strategy that retries on network errors (err != nil)
// or server-side HTTP errors (status >= 500). 429 is treated as a signal to slow down
// and is not retried by default unless the caller explicitly opts in.
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
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	}

	return false
}

// ShouldRetryWithRateLimit allows the caller to opt into retrying 429 responses.
func ShouldRetryWithRateLimit(response *http.Response, err error) bool {
	if ShouldRetryDefault(response, err) {
		return true
	}
	return response != nil && response.StatusCode == http.StatusTooManyRequests
}

// Retry429PolicyFor returns a retry predicate that respects the caller's chosen 429 handling mode.
func Retry429PolicyFor(policy Retry429Policy) func(*http.Response, error) bool {
	switch policy {
	case Retry429Always:
		return ShouldRetryWithRateLimit
	case Retry429Never:
		return ShouldRetryDefault
	case Retry429Default:
		fallthrough
	default:
		return ShouldRetryDefault
	}
}
