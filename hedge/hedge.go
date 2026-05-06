package hedge

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/audryus/resili7"
)

// result wraps an HTTP response or an error.
// It is used to communicate results from parallel hedge attempts back to the main goroutine.
type result struct {
	resp *http.Response
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

// NewHedgeMiddleware creates a middleware that implements the Hedged Requests pattern.
// If the primary request is slow (takes longer than 'delay'), additional parallel attempts
// are fired. The first successful response (or the last failure) is returned.
func NewHedgeMiddleware(delay time.Duration, maxAttempts int) resili7.Middleware {
	return func(next resili7.Handler) resili7.Handler {
		return func(req resili7.Request) (*http.Response, error) {
			// Fast path: if hedging is disabled or invalid params are provided.
			if maxAttempts <= 1 || delay <= 0 {
				return next(req)
			}

			// Acquire state from pool to maintain zero-allocation goal.
			state := statePool.Get().(*hedgeState)
			state.active.Store(1) // Initial count for the main coordinating loop.

			// Start the first (primary) attempt.
			state.active.Add(1)
			go hedgeAttempt(next, req, state)

			var finalRes result
			var timer *time.Timer

			// Loop to start additional attempts if the primary one is slow.
			for i := 1; i < maxAttempts; i++ {
				timer = getTimer(delay)

				select {
				case finalRes = <-state.resCh:
					// A result was received before the delay; stop the timer and exit.
					putTimer(timer)
					goto finish
				case <-timer.C:
					// Delay exceeded; fire another attempt.
					putTimer(timer)
					timer = nil

					newReq := req // Request is passed by value (safe copy).
					state.active.Add(1)
					go hedgeAttempt(next, newReq, state)
				}
			}

			// All attempts fired; wait for the first result from any of them.
			finalRes = <-state.resCh

		finish:
			// Cleanup: decrement the reference count. If zero, all goroutines finished; recycle state.
			if state.active.Add(-1) == 0 {
				recycleState(state)
			}
			return finalRes.resp, finalRes.err
		}
	}
}

// hedgeAttempt executes a single attempt and reports the result back to the state channel.
func hedgeAttempt(next resili7.Handler, req resili7.Request, state *hedgeState) {
	resp, err := next(req)

	// Attempt to send the result. If resCh is full, another goroutine already won.
	select {
	case state.resCh <- result{resp, err}:
	default:
	}

	// Decrement active count. If this was the last goroutine, recycle the state.
	if state.active.Add(-1) == 0 {
		recycleState(state)
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
