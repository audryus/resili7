package rhttp

import (
	"errors"
	"net/http"
)

var (
	ErrRateLimited = errors.New("rate limit")    // 429 Too Many Requests.
	ErrClientError = errors.New("client error")  // 408 Request Timeout, 425 Too Early.
	ErrServerError = errors.New("server error")  // 5xx server errors.
	ErrUnknown     = errors.New("unknown error") // Nil response or unclassified error.
)

// NewClassifierMiddleware classifies HTTP responses and converts status codes into errors.
// This enables the circuit breaker to track failures and the retry middleware to make decisions
// based on status codes rather than transport-level errors alone.
func NewClassifierMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*http.Response, error) {
			resp, err := next(req)
			return resp, ClassifyError(resp, err)
		}
	}
}

// ClassifyError converts an HTTP response into a classification error.
// Returns the original error if present; otherwise classifies by status code.
// Used by the circuit breaker and retry middlewares to determine failure classification.
func ClassifyError(response *http.Response, err error) error {
	if err != nil {
		return err
	}

	if response == nil {
		return ErrUnknown
	}

	switch response.StatusCode {
	case http.StatusRequestTimeout, // 408
		http.StatusTooEarly: // 425
		return ErrClientError
	case http.StatusTooManyRequests: // 429
		return ErrRateLimited
	case
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return ErrServerError
	}

	return nil
}
