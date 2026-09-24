package rgrpc

import (
	"context"
	"time"

	"codeberg.org/audryus/resili7/hedge"
)

// NewHedgeMiddleware creates a middleware that implements the Hedged Requests pattern for gRPC.
// If the primary request is slow (takes longer than 'delay'), additional parallel attempts
// are fired. The first successful response (or the last failure) is returned.
//
// Each attempt runs with its own context derived from the request context: when
// the first attempt wins, losers are cancelled through the call context so
// in-flight RPCs abort instead of running to completion.
func NewHedgeMiddleware(delay time.Duration, maxAttempts int) Middleware {
	return func(next Handler) Handler {
		return func(req Request) error {
			ctx := req.Ctx
			if ctx == nil {
				ctx = context.Background()
			}
			_, err := hedge.ExecuteWithResultCtx(ctx, delay, maxAttempts, func(ctx context.Context) (Request, error) {
				r := req
				r.Ctx = ctx
				return r, next(r)
			})
			return err
		}
	}
}
