package rgrpc

import (
	"time"

	"codeberg.org/audryus/resili7/timeout"
)

// NewTimeoutMiddleware creates a middleware that enforces a global timeout for the entire gRPC request pipeline.
// It sets the RequestDeadline field in the Request struct and checks it both before and after
// calling the next handler in the chain.
func NewTimeoutMiddleware(d time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(req Request) error {
			return timeout.Execute(d, action{
				h:   next,
				req: req,
			})
		}
	}
}
