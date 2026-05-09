package retry

import (
	"errors"
	"time"
)

var ErrTimeout = errors.New("request timeout")
var ErrRetry = errors.New("exausted retry attempts")

// ResultAction represents a typed operation that returns a result.
// The generic return value enables pass-by-value semantics, avoiding heap allocations
// from pointer-based side-channels in the hot path.
type ResultAction[R any] interface {
	Execute() (R, error)
	Now() int64
	RequestDeadline() int64
	IsSuccess(R, error) bool
	ShouldRetryDefault(R, error) bool
	Err(error) error
}

func Execute[R any, A ResultAction[R]](policy *RetryPolicy[R], action A) (err error) {
	_, err = ExecuteWithResult(policy, action)
	return err
}

// ExecuteWithResult executes the action with retry logic.
// Returns (R, error) to support pass-by-value semantics for zero-allocation in the hot path.
func ExecuteWithResult[R any, A ResultAction[R]](policy *RetryPolicy[R], action A) (resp R, err error) {
	maxAttempts := policy.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	backoffFn := policy.Backoff
	if backoffFn == nil {
		backoffFn = LinearBackoff
	}
	shouldRetryFn := policy.ShouldRetry
	if shouldRetryFn == nil {
		shouldRetryFn = action.ShouldRetryDefault
	}

	budget := policy.Budget
	perTryTimeout := int64(policy.TryDeadline)

	// Calculate the per-try deadline once per request start.
	var tryDeadline int64
	if perTryTimeout > 0 {
		tryDeadline = action.Now() + perTryTimeout
	}
	now := action.Now()

	for attempt := range maxAttempts {
		// Enforce per-try timeout before starting the attempt.
		if tryDeadline > 0 && now > tryDeadline {
			return resp, ErrTimeout
		}

		// Execute the next handler in the chain.
		resp, err = action.Execute()

		// Update cached time after a potentially slow network call.
		now = time.Now().UnixNano()

		// Success condition: No error and status code is successful (< 400).
		if action.IsSuccess(resp, err) {
			if budget != nil {
				budget.RecordSuccess()
			}
			return resp, nil
		}

		// Check if we should even attempt a retry based on the error/response.
		if !shouldRetryFn(resp, err) {
			return resp, err
		}

		// Verify the retry budget to prevent overloading the downstream service.
		if budget != nil && !budget.AllowRetry() {
			return resp, ErrRetry
		}

		// Calculate and wait for backoff delay.
		delay := backoffFn(attempt)

		// Respect global RequestDeadline during backoff sleep.
		if action.RequestDeadline() > 0 {
			remaining := action.RequestDeadline() - now
			if remaining <= 0 {
				return resp, ErrTimeout
			}
			if int64(delay) > remaining {
				delay = time.Duration(remaining)
			}
		}

		time.Sleep(delay)
		// Refresh time after sleeping.
		now = time.Now().UnixNano()
	}

	// If we exhausted all attempts:
	// Return the actual network error if present, otherwise return ErrRetry.
	return resp, action.Err(err)
}
