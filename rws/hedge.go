package rws

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// DialConn is a function type that dials a WebSocket connection with cancellation.
// It matches the signature of websocket.Dialer.DialContext.
type DialConn func(ctx context.Context, url string, headers http.Header) (*websocket.Conn, *http.Response, error)

// dialResult holds the result of a hedged dial attempt.
// The first successful connection wins.
type dialResult struct {
	conn *websocket.Conn
	resp *http.Response
	err  error
}

// dialState coordinates simultaneous dial attempts for hedging.
// It uses a buffered channel to receive the first winner.
type dialState struct {
	resCh  chan dialResult
	active atomic.Int32 // Reference counter for active goroutines.
}

// dialStatePool provides a pool of dialState objects to avoid per-request heap allocations.
var dialStatePool = sync.Pool{
	New: func() any {
		return &dialState{
			resCh: make(chan dialResult, 1),
		}
	},
}

// timerPool provides a pool of *time.Timer objects to reduce allocation pressure.
var timerPool sync.Pool

// getTimer retrieves a timer from the pool or creates a new one.
func getTimer(d time.Duration) *time.Timer {
	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		t.Reset(d)
		return t
	}
	return time.NewTimer(d)
}

// putTimer stops the timer, drains if needed, and returns it to the pool.
func putTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	timerPool.Put(t)
}

// closeHandshakeBody releases the HTTP handshake response body. Hedge dialing
// only needs the connection; the handshake response is never returned to the
// caller, so every attempt (winner and losers) must release it.
func closeHandshakeBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}

// NewHedgeMiddleware creates a middleware that dials multiple endpoints with a delay between attempts.
// This is the WebSocket-native hedging strategy: hedge at the connection level, not per-message.
// The first successful connection is used; others are cleanly closed.
//
// Dials run with a child context that is cancelled once a winner is known, so
// losing handshakes abort early instead of running to the dial timeout.
func NewHedgeMiddleware(dialer DialConn, delay time.Duration, maxAttempts int) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*Response, error) {
			// Fast path: skip hedging if already have connection or invalid params.
			if maxAttempts <= 1 || delay <= 0 || req.URL == "" || req.Conn != nil {
				return next(req)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			state := dialStatePool.Get().(*dialState)
			state.active.Store(1) // Main goroutine reference.

			// Start primary dial attempt.
			state.active.Add(1)
			go hedgeDialAttempt(ctx, dialer, req.URL, req.Headers, state)

			var winner dialResult
			var timer *time.Timer

			// Loop to fire additional attempts after the delay.
			for i := 1; i < maxAttempts; i++ {
				timer = getTimer(delay)
				select {
				case winner = <-state.resCh:
					// Received result before delay; stop timer and finish.
					putTimer(timer)
					goto finish
				case <-timer.C:
					// Delay elapsed; fire another dial attempt.
					putTimer(timer)
					state.active.Add(1)
					go hedgeDialAttempt(ctx, dialer, req.URL, req.Headers, state)
				}
			}

			// Wait for the first winner from any fired attempt.
			winner = <-state.resCh

		finish:
			// Winner known: abort losing handshakes, then recycle state once
			// every attempt has exited (each decrements active on completion).
			cancel()
			if state.active.Add(-1) == 0 {
				recycleDialState(state)
			}

			if winner.err != nil {
				closeHandshakeBody(winner.resp)
				return nil, winner.err
			}

			closeHandshakeBody(winner.resp)
			req.Conn = winner.conn
			return next(req)
		}
	}
}

// hedgeDialAttempt attempts to dial a single endpoint.
// If it wins (first to send), the connection is used; otherwise it's closed
// along with its handshake response body.
func hedgeDialAttempt(ctx context.Context, dialer DialConn, url string, headers http.Header, state *dialState) {
	conn, resp, err := dialer(ctx, url, headers)

	select {
	case state.resCh <- dialResult{conn: conn, resp: resp, err: err}:
		// Won! Result sent to main goroutine (it owns conn and resp now).
	default:
		// Lost or already finished. Close loser connection and body.
		if conn != nil {
			conn.Close()
		}
		closeHandshakeBody(resp)
	}

	if state.active.Add(-1) == 0 {
		recycleDialState(state)
	}
}

// recycleDialState prepares the dialState for reuse by clearing the channel and returning to pool.
func recycleDialState(s *dialState) {
	select {
	case <-s.resCh:
	default:
	}
	dialStatePool.Put(s)
}
