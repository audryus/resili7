package rhttp

import (
	"errors"
	"net/http"
	"time"

	"codeberg.org/audryus/resili7/timeout"
)

// NewTimeoutMiddleware creates a middleware that enforces a global timeout for the entire request pipeline.
// It sets the RequestDeadline field in the Request struct and checks it both before and after
// calling the next handler in the chain.
func NewTimeoutMiddleware(d time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*http.Response, error) {
			// Propagate the deadline to the request struct so subsequent handlers can use it.
			if req.RequestDeadline == 0 {
				req.RequestDeadline = req.Now + int64(d)
			}

			// Guard the inner handler: a response that completes after the
			// deadline must have its body closed here (connection-close
			// semantics) because the timeout layer reports a zero response
			// and the caller will never see — or close — this body.
			guarded := Handler(func(r Request) (*http.Response, error) {
				resp, err := next(r)
				if err == nil && resp != nil && r.RequestDeadline > 0 &&
					time.Now().UnixNano() > r.RequestDeadline {
					if resp.Body != nil {
						resp.Body.Close()
					}
					return nil, timeout.ErrTimeout
				}
				return resp, err
			})

			resp, err := timeout.ExecuteWithResult(d, action{
				h:   guarded,
				req: req,
			})
			if errors.Is(err, timeout.ErrTimeout) {
				return nil, err
			}
			return resp, err
		}
	}
}
