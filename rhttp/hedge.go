package rhttp

import (
	"context"
	"net/http"
	"time"

	"github.com/audryus/resili7/hedge"
)

// NewHedgeMiddleware creates a middleware that implements the Hedged Requests pattern.
// If the primary request is slow (takes longer than 'delay'), additional parallel attempts
// are fired. The first successful response (or the last failure) is returned.
//
// Each attempt runs with its own context derived from the request context: when
// the first attempt wins, losers are cancelled through the request context so
// the transport aborts in-flight requests instead of running them to completion.
func NewHedgeMiddleware(delay time.Duration, maxAttempts int) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*http.Response, error) {
			ctx := context.Background()
			if req.Req != nil && req.Req.Context() != nil {
				ctx = req.Req.Context()
			}
			return hedge.ExecuteWithResultCtx(ctx, delay, maxAttempts, func(ctx context.Context) (*http.Response, error) {
				r := req
				if r.Req != nil {
					r.Req = r.Req.WithContext(ctx)
				}
				return next(r)
			})
		}
	}
}
