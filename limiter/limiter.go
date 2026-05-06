package limiter

import (
	"errors"
	"net/http"

	"codeberg.org/audryus/resili7"
)

var ErrLimited = errors.New("rate limited")

// NewLimiterMiddleware creates a middleware that enforces concurrency limits using a Limiter instance.
// It integrates with the resilience pipeline to provide adaptive backpressure.
func NewLimiterMiddleware(l *Limiter) resili7.Middleware {
	return func(next resili7.Handler) resili7.Handler {
		return func(req resili7.Request) (*http.Response, error) {
			// Attempt to acquire a concurrency permit.
			// Passing req.Now to avoid an extra system time call.
			start, ok := l.Acquire(req.Now)
			if !ok {
				// Reject the request immediately if the limit is reached.
				return nil, ErrLimited
			}

			// Execute the rest of the pipeline.
			resp, err := next(req)

			// Report the result back to the limiter.
			// Success is defined as err == nil.
			l.Done(start, err == nil)

			return resp, err
		}
	}
}
