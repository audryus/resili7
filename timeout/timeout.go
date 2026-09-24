package timeout

import (
	"errors"
	"time"
)

var ErrTimeout = errors.New("request timeout")

// ResultAction represents a typed operation that returns a result.
// The generic return value enables pass-by-value semantics, avoiding heap allocations
// from pointer-based side-channels in the hot path.
type ResultAction[R any] interface {
	Execute() (R, error)
	Now() int64
	RequestDeadline() int64
}

func Execute[R any, A ResultAction[R]](d time.Duration, action A) (err error) {
	_, err = ExecuteWithResult(d, action)
	return err
}

// ExecuteWithResult executes the action with timeout enforcement.
// Returns (R, error) to support pass-by-value semantics for zero-allocation in the hot path.
//
// Connection-close semantics: when the deadline has passed the caller gets
// the zero value of R with ErrTimeout — never a partial response it might
// treat as usable (e.g. an HTTP Body it would fail to Close, leaking the
// connection).
func ExecuteWithResult[R any, A ResultAction[R]](d time.Duration, action A) (resp R, err error) {
	now := action.Now()
	requestDeadline := action.RequestDeadline()

	// Initialize the RequestDeadline if it hasn't been set by an outer middleware.
	if requestDeadline == 0 {
		requestDeadline = now + int64(d)
	}

	// Pre-execution check: fail immediately if the deadline has already passed.
	if now > requestDeadline {
		return resp, ErrTimeout
	}

	// Execute the rest of the pipeline.
	resp, err = action.Execute()

	// Post-execution check: even if the handler succeeded, if it finished after the deadline,
	// we return the zero value with ErrTimeout to ensure strict timing guarantees.
	// Note: We call time.Now() here to get the most accurate finish time.
	if time.Now().UnixNano() > requestDeadline {
		var zero R
		return zero, ErrTimeout
	}

	return resp, err
}
