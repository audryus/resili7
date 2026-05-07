package hedge

import (
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

// ResultAction representa uma operação que devolve um resultado tipado.
// Elimina a necessidade de ponteiros side-channel (que causam escape para o Heap).
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

// ExecuteWithResult é idêntico ao ExecuteAction mas retorna (R, error).
// Isso permite que o caller receba o resultado por valor, sem alocação no Heap.
func ExecuteWithResult[R any, A ResultAction[R]](delay time.Duration, maxAttempts int, action A) (resp R, err error) {
	// Fast path: if hedging is disabled or invalid params are provided.
	if maxAttempts <= 1 || delay <= 0 {
		return action.Execute()
	}

	// Acquire state from pool to maintain zero-allocation goal.
	state := statePool.Get().(*hedgeState)
	state.active.Store(1) // Initial count for the main coordinating loop.

	// Start the first (primary) attempt.
	state.active.Add(1)
	go hedgeAttempt(action, state)

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

			state.active.Add(1)
			go hedgeAttempt(action, state)
		}
	}

	// All attempts fired; wait for the first result from any of them.
	finalRes = <-state.resCh

finish:
	// Cleanup: decrement the reference count. If zero, all goroutines finished; recycle state.
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
