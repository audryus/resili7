package hedge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// result wraps a response or an error.
// It is used to communicate results from parallel hedge attempts back to the main goroutine.
type result struct {
	resp any
	err  error
}

// hedgeState manages the lifecycle and coordination of a single hedged request.
type hedgeState struct {
	resCh  chan result  // Buffered channel to receive the first available result.
	active atomic.Int32 // Reference counter to track pending goroutines for recycling.
}

// statePool provides a pool of hedgeState objects to avoid per-request heap allocations.
var statePool = sync.Pool{
	New: func() any {
		return &hedgeState{
			resCh: make(chan result, 1),
		}
	},
}

// ResultAction represents a typed operation that returns a result.
// The generic return value enables pass-by-value semantics, avoiding heap allocations
// from pointer-based side-channels in the hot path.
type ResultAction[R any] interface {
	Execute() (R, error)
}

// timerPool provides a pool of *time.Timer objects to reduce allocation pressure.
var timerPool sync.Pool

// getTimer retrieves a timer from the pool and resets it to the specified duration.
func getTimer(d time.Duration) *time.Timer {
	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		t.Reset(d)
		return t
	}
	return time.NewTimer(d)
}

// putTimer stops the timer and returns it to the pool for reuse.
func putTimer(t *time.Timer) {
	if !t.Stop() {
		// Drain the timer channel if it already fired to avoid interference in reuse.
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}

func Execute[R any, A ResultAction[R]](delay time.Duration, maxAttempts int, action A) (err error) {
	_, err = ExecuteWithResult(delay, maxAttempts, action)
	return err
}

// ExecuteWithResult executes the action with hedged request logic.
// Returns (R, error) to support pass-by-value semantics for zero-allocation in the hot path.
//
// This is the fire-and-forget variant: attempts that lose the race run to
// completion because they receive no cancellation signal. Prefer
// ExecuteWithResultCtx with a context-aware attempt function so losers abort
// early (at the cost of one context allocation per hedged request).
func ExecuteWithResult[R any, A ResultAction[R]](delay time.Duration, maxAttempts int, action A) (resp R, err error) {
	return ExecuteWithResultCtx(context.Background(), delay, maxAttempts, func(context.Context) (R, error) {
		return action.Execute()
	})
}

func ExecuteCtx[R any, A ResultAction[R]](ctx context.Context, delay time.Duration, maxAttempts int, action A) (err error) {
	_, err = ExecuteWithResultCtx(ctx, delay, maxAttempts, func(context.Context) (R, error) {
		return action.Execute()
	})
	return err
}

// ExecuteWithResultCtx executes fn with hedged request logic and cancellation.
//
// Each attempt runs fn with a child context derived from ctx. When the first
// attempt reports back, the child context is cancelled so losing attempts
// that respect ctx abort early instead of running to completion. If ctx
// itself is cancelled while waiting, the wait aborts with ctx.Err().
//
// The first result received wins — even if it is an error. Callers that want
// error-tolerant hedging should encode success in R or wrap fn.
func ExecuteWithResultCtx[R any](ctx context.Context, delay time.Duration, maxAttempts int, fn func(context.Context) (R, error)) (resp R, err error) {
	// Fast path: if hedging is disabled or invalid params are provided.
	if maxAttempts <= 1 || delay <= 0 {
		if ctx == nil {
			ctx = context.Background()
		}
		return fn(ctx)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Acquire state from pool to maintain zero-allocation goal.
	state := statePool.Get().(*hedgeState)
	state.active.Store(1) // Initial count for the main coordinating loop.

	// Start the first (primary) attempt.
	state.active.Add(1)
	go hedgeAttemptCtx(childCtx, fn, state)

	var finalRes result
	gotResult := false

	// Loop to start additional attempts if the primary one is slow.
loop:
	for i := 1; i < maxAttempts; i++ {
		timer := getTimer(delay)

		select {
		case finalRes = <-state.resCh:
			// A result was received before the delay; stop the timer and exit.
			putTimer(timer)
			gotResult = true
			break loop
		case <-timer.C:
			// Delay exceeded; fire another attempt.
			putTimer(timer)

			state.active.Add(1)
			go hedgeAttemptCtx(childCtx, fn, state)
		case <-ctx.Done():
			// Caller gave up while waiting: stop the timer and abort.
			putTimer(timer)
			cancel()
			drainPending(state)
			if state.active.Add(-1) == 0 {
				recycleState(state)
			}
			var zero R
			return zero, ctx.Err()
		}
	}

	if !gotResult {
		// All attempts fired; wait for the first result from any of them,
		// or for caller cancellation.
		select {
		case finalRes = <-state.resCh:
		case <-ctx.Done():
			cancel()
			drainPending(state)
			if state.active.Add(-1) == 0 {
				recycleState(state)
			}
			var zero R
			return zero, ctx.Err()
		}
	}

	// Winner known: cancel losers, then wait only for state recycling
	// bookkeeping (losers decrement active on exit; recycle happens when the
	// last one finishes). Do NOT block the caller on losers.
	cancel()

	// Cleanup: the coordinator decrements the reference count only after it has consumed
	// the winning result, so the state cannot be recycled while it is still in use.
	// Each hedged attempt also decrements the counter once, so recycling only occurs
	// after the coordinator and all outstanding attempt goroutines have finished.
	if state.active.Add(-1) == 0 {
		recycleState(state)
	}

	if finalRes.resp != nil {
		resp = finalRes.resp.(R)
	}
	return resp, finalRes.err
}

// hedgeAttempt executes a single attempt and reports the result back to the state channel.
func hedgeAttempt[R any, A ResultAction[R]](action A, state *hedgeState) {
	resp, err := action.Execute()

	// Attempt to send the result. If resCh is full, another goroutine already won.
	select {
	case state.resCh <- result{resp: resp, err: err}:
	default:
	}

	// Decrement the active count. Exactly one owner (the last to decrement)
	// observes zero and recycles the state.
	if state.active.Add(-1) == 0 {
		recycleState(state)
	}
}

// hedgeAttemptCtx executes a single context-aware attempt and reports the
// result back unless the race is already decided or ctx was cancelled.
func hedgeAttemptCtx[R any](ctx context.Context, fn func(context.Context) (R, error), state *hedgeState) {
	resp, err := fn(ctx)

	// Attempt to send the result. If resCh is full, another goroutine already won.
	// If ctx was cancelled first, drop the result: the coordinator has moved on.
	select {
	case state.resCh <- result{resp: resp, err: err}:
	default:
	}

	if state.active.Add(-1) == 0 {
		recycleState(state)
	}
}

// drainPending discards a result that may already sit in the channel so a
// recycled state never leaks a stale winner into the next request.
func drainPending(state *hedgeState) {
	select {
	case <-state.resCh:
	default:
	}
}

// recycleState prepares the hedgeState for reuse by clearing the channel and returning to pool.
func recycleState(s *hedgeState) {
	select {
	case <-s.resCh:
	default:
	}
	statePool.Put(s)
}
