package timeout

import (
	"errors"
	"net/http"
	"time"

	"codeberg.org/audryus/resili7"
)

var ErrTimeout = errors.New("request timeout")

// NewTimeoutMiddleware creates a middleware that enforces a global timeout for the entire request pipeline.
// It sets the RequestDeadline field in the Request struct and checks it both before and after
// calling the next handler in the chain.
func NewTimeoutMiddleware(d time.Duration) resili7.Middleware {
	return func(next resili7.Handler) resili7.Handler {
		return func(req resili7.Request) (*http.Response, error) {
			// Initialize the RequestDeadline if it hasn't been set by an outer middleware.
			if req.RequestDeadline == 0 {
				req.RequestDeadline = req.Now + int64(d)
			}

			// Pre-execution check: fail immediately if the deadline has already passed.
			if req.Now > req.RequestDeadline {
				return nil, ErrTimeout
			}

			// Execute the rest of the pipeline.
			resp, err := next(req)

			// Post-execution check: even if the handler succeeded, if it finished after the deadline,
			// we return ErrTimeout to ensure strict timing guarantees.
			// Note: We call time.Now() here to get the most accurate finish time.
			if err == nil && time.Now().UnixNano() > req.RequestDeadline {
				return resp, ErrTimeout
			}

			return resp, err
		}
	}
}
