package rgrpc

import (
	"time"

	"codeberg.org/audryus/resili7/hedge"
)

// NewHedgeMiddleware creates a middleware that implements the Hedged Requests pattern for gRPC.
// If the primary request is slow (takes longer than 'delay'), additional parallel attempts
// are fired. The first successful response (or the last failure) is returned.
func NewHedgeMiddleware(delay time.Duration, maxAttempts int) Middleware {
	return func(next Handler) Handler {
		return func(req Request) error {
			return hedge.Execute(delay, maxAttempts, action{
				h:   next,
				req: req,
			})
		}
	}
}
