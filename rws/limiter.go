package rws

import (
	"github.com/audryus/resili7/limiter"
)

// NewLimiterMiddleware creates a middleware that enforces concurrency limits using a Limiter instance.
// It integrates with the WebSocket resilience pipeline to provide adaptive backpressure.
func NewLimiterMiddleware(l *limiter.Limiter) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*Response, error) {
			return limiter.ExecuteWithResult(l, action{
				h:   next,
				req: req,
			})
		}
	}
}
