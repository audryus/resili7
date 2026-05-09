package rgrpc

import (
	"codeberg.org/audryus/resili7/retry"
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
func (a action) Err(err error) error {
	if err != nil {
		return retry.ErrRetry
	}
	return err
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
