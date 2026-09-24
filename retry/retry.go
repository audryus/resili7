package retry

import (
	"errors"
	"time"

	"codeberg.org/audryus/resili7/timeout"
)

// ErrTimeout is an alias of timeout.ErrTimeout: a single sentinel so callers
// can match either layer with errors.Is.
var ErrTimeout = timeout.ErrTimeout

// ErrRetry signals that all retry attempts were exhausted. When the last
// attempt failed with a network error, it is joined with that error via
// errors.Join so both errors.Is(err, ErrRetry) and unwrapping of the causal
// error (e.g. status.FromError) keep working.
var ErrRetry = errors.New("exhausted retry attempts")

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

// DiscardAttempt is optionally implemented by a ResultAction to release
// resources held by a response the retry loop will not return to the caller
// (intermediate attempts, or attempts discarded on timeout/budget/exhaustion).
// The HTTP adapter implements it to close response bodies and avoid leaking
// connections; adapters without closeable resources omit it.
type DiscardAttempt[R any] interface {
	DiscardAttempt(R)
}

func discard[R any, A ResultAction[R]](action A, resp R) {
	if d, ok := any(action).(DiscardAttempt[R]); ok {
		d.DiscardAttempt(resp)
	}
}

// ExecuteWithResult executes the action with retry logic.
// Returns (R, error) to support pass-by-value semantics for zero-allocation in the hot path.
//
// A nil policy is treated as a single attempt with default backoff. Backoff
// sleeps are deadline-aware: when RequestDeadline passes during the wait, no
// further attempt is executed and ErrTimeout is returned.
func ExecuteWithResult[R any, A ResultAction[R]](policy *RetryPolicy[R], action A) (resp R, err error) {
	maxAttempts := 1
	var backoffFn func(int) time.Duration
	var shouldRetryFn func(R, error) bool
	var budget *Budget
	var perTryTimeout int64

	if policy != nil {
		maxAttempts = policy.MaxAttempts
		backoffFn = policy.Backoff
		shouldRetryFn = policy.ShouldRetry
		budget = policy.Budget
		perTryTimeout = int64(policy.TryDeadline)
	}

	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if backoffFn == nil {
		backoffFn = LinearBackoff
	}
	if shouldRetryFn == nil {
		shouldRetryFn = action.ShouldRetryDefault
	}

	now := action.Now()

	for attempt := range maxAttempts {
		// Global deadline pre-check: never start an attempt that is already expired.
		if dl := action.RequestDeadline(); dl > 0 && now > dl {
			discard(action, resp)
			var zero R
			return zero, ErrTimeout
		}

		// Calculate the per-try deadline for this specific attempt.
		var tryDeadline int64
		if perTryTimeout > 0 {
			tryDeadline = now + perTryTimeout
		}

		// Execute the next handler in the chain.
		resp, err = action.Execute()

		// Update cached time after a potentially slow network call.
		now = time.Now().UnixNano()

		// Enforce per-try timeout before starting the attempt.
		if tryDeadline > 0 && now > tryDeadline {
			discard(action, resp)
			var zero R
			return zero, ErrTimeout
		}

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
			discard(action, resp)
			var zero R
			return zero, ErrRetry
		}

		// The current response will not be returned (another attempt follows):
		// let the action release it (e.g. close an HTTP response body).
		discard(action, resp)
		var discarded R
		resp = discarded

		// Calculate and wait for backoff delay, deadline-aware.
		delay := backoffFn(attempt)

		// Respect global RequestDeadline during backoff sleep.
		if dl := action.RequestDeadline(); dl > 0 {
			remaining := dl - now
			if remaining <= 0 {
				var zero R
				return zero, ErrTimeout
			}
			if int64(delay) > remaining {
				delay = time.Duration(remaining)
			}
		}

		time.Sleep(delay)
		// Refresh time after sleeping.
		now = time.Now().UnixNano()
	}

	// If we exhausted all attempts: discard the last response (it is not
	// returned) and report exhaustion, preserving the last network error.
	discard(action, resp)
	var zero R
	return zero, action.Err(err)
}
