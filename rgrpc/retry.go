package rgrpc

import (
	"errors"

	"github.com/audryus/resili7/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewRetryMiddleware creates a middleware that implements sequential retries for gRPC calls.
// It uses a provided RetryPolicy to manage backoff, budgets, and per-try deadlines.
func NewRetryMiddleware(policy *retry.RetryPolicy[Request]) Middleware {
	return func(next Handler) Handler {
		return func(req Request) error {
			return retry.Execute(policy, action{
				h:   next,
				req: req,
			})
		}
	}
}

func (a action) IsSuccess(_ Request, err error) bool {
	return err == nil
}

func (a action) ShouldRetryDefault(_ Request, err error) bool {
	return ShouldRetryDefault(err)
}
// Err reports the final error after retries are exhausted. It joins
// retry.ErrRetry with the last network error so errors.Is(err, retry.ErrRetry)
// still signals exhaustion while status.FromError(err) keeps the causal
// gRPC code (e.g. codes.Unavailable).
func (a action) Err(err error) error {
	if err == nil {
		return retry.ErrRetry
	}
	return errors.Join(retry.ErrRetry, err)
}

// ShouldRetryDefault returns true if the error is a gRPC status that warrants a retry.
// It retries on typical transient errors like Unavailable, Internal, and DeadlineExceeded.
func ShouldRetryDefault(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		// Not a standard gRPC status error; could be a transport-level error.
		return true
	}

	switch st.Code() {
	case codes.DeadlineExceeded,
		codes.Internal,
		codes.Unavailable,
		codes.ResourceExhausted:
		return true
	}

	return false
}
