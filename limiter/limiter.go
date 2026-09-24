package limiter

import (
	"errors"
	"time"
)

var ErrLimited = errors.New("rate limited")

// ResultAction represents a typed operation that returns a result.
// The generic return value enables pass-by-value semantics, avoiding heap allocations
// from pointer-based side-channels in the hot path.
type ResultAction[R any] interface {
	Execute() (R, error)
	Now() int64
}

func Execute[R any, A ResultAction[R]](l *Limiter, action A) (err error) {
	_, err = ExecuteWithResult(l, action)
	return err
}

// ExecuteWithResult executes the action with concurrency control.
// Returns (R, error) to support pass-by-value semantics for zero-allocation in the hot path.
func ExecuteWithResult[R any, A ResultAction[R]](l *Limiter, action A) (resp R, err error) {
	// Attempt to acquire a concurrency permit.
	// Passing req.Now to avoid an extra system time call.
	start, ok := l.Acquire(action.Now())
	if !ok {
		// Reject the request immediately if the limit is reached.
		return resp, ErrLimited
	}

	// Execute the rest of the pipeline.
	resp, err = action.Execute()

	// Report the result back to the limiter, capturing the end timestamp once.
	// Success is defined as err == nil.
	l.Done(start, time.Now().UnixNano(), err == nil)

	return resp, err
}
