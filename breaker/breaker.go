package breaker

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/audryus/resili7"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

// NewBreakerMiddleware creates a middleware that protects the handler using a circuit breaker.
// It tracks failure rates and opens the circuit if a threshold is reached, 
// preventing further requests from reaching failing downstream services.
func NewBreakerMiddleware(cb *CircuitBreaker) resili7.Middleware {
	return func(next resili7.Handler) resili7.Handler {
		return func(req resili7.Request) (*http.Response, error) {

			var err error
			var resp *http.Response

			// FAST PATH — CLOSED
			// Optimized for the common case where the circuit is closed.
			if State(cb.state.Load()) == StateClosed {
				cb.rotateWindow(req.Now)
				cb.requests.Add(1)

				resp, err = next(req)

				if cb.isFailure(err) {
					cb.failures.Add(1)
				}

				total := cb.requests.Load()
				fail := cb.failures.Load()

				// Check if the failure threshold has been exceeded.
				if total >= cb.minRequests {
					// Using fixed-point comparison: (fail / total) >= threshold
					// Equivalent to: fail * 1024 >= threshold * total
					if int64(fail)*1024 >= cb.errorThreshold*int64(total) {

						cb.mu.Lock()
						if State(cb.state.Load()) == StateClosed {
							cb.state.Store(uint32(StateOpen))
							cb.lastFailTime = time.Now().UnixNano()
						}
						cb.mu.Unlock()
					}
				}

				return resp, err
			}

			// SLOW PATH — OPEN or HALF-OPEN
			// Handles state transitions and recovery testing.
			cb.mu.Lock()

			state := State(cb.state.Load())
			now := time.Now().UnixNano()

			if state == StateOpen {
				// Check if the cooldown period has passed to move to Half-Open.
				if now-cb.lastFailTime >= int64(cb.openTimeout) {
					cb.state.Store(uint32(StateHalfOpen))
					state = StateHalfOpen
				} else {
					cb.mu.Unlock()
					return resp, ErrCircuitOpen
				}
			}

			if state == StateHalfOpen {
				// Only allow one request at a time during Half-Open testing.
				if cb.halfOpenInFlight {
					cb.mu.Unlock()
					return resp, ErrCircuitOpen
				}
				cb.halfOpenInFlight = true
			}

			cb.mu.Unlock()

			resp, err = next(req)

			cb.mu.Lock()
			defer cb.mu.Unlock()

			if State(cb.state.Load()) == StateHalfOpen {
				cb.halfOpenInFlight = false

				if cb.isFailure(err) {
					// If the trial request fails, move back to Open.
					cb.state.Store(uint32(StateOpen))
					cb.lastFailTime = time.Now().UnixNano()
				} else {
					// If the trial request succeeds, reset to Closed.
					cb.state.Store(uint32(StateClosed))
					cb.requests.Store(0)
					cb.failures.Store(0)
					cb.windowStart.Store(time.Now().UnixNano())
				}
			}

			return resp, err
		}
	}
}

// State represents the current operational mode of the circuit breaker.
type State uint32

const (
	StateClosed   State = iota // Normal operation: requests are allowed.
	StateOpen                  // Failing state: requests are rejected immediately.
	StateHalfOpen              // Recovery testing: limited requests are allowed to test downstream health.
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrorClassifier is a function that determines if an error should count as a failure.
type ErrorClassifier func(error) bool

// Option defines a functional configuration for a CircuitBreaker.
type Option func(*CircuitBreaker)

// WithTimeout sets the duration to stay in Open state before testing for recovery.
func WithTimeout(d time.Duration) Option {
	return func(cb *CircuitBreaker) {
		cb.openTimeout = d
	}
}

// WithWindow sets the statistical window duration for failure rate calculation.
func WithWindow(d time.Duration) Option {
	return func(cb *CircuitBreaker) {
		cb.windowDuration = d
	}
}

// WithMinRequests sets the minimum number of requests required before opening the circuit.
func WithMinRequests(n uint64) Option {
	return func(cb *CircuitBreaker) {
		cb.minRequests = n
	}
}

// WithErrorThreshold sets the failure rate (0.0 to 1.0) that triggers the circuit opening.
func WithErrorThreshold(v float64) Option {
	return func(cb *CircuitBreaker) {
		cb.errorThreshold = int64(v * 1024)
	}
}

// WithErrorClassifier sets a custom function to determine which errors count as failures.
func WithErrorClassifier(fn ErrorClassifier) Option {
	return func(cb *CircuitBreaker) {
		cb.errorClassifier = fn
	}
}

func defaultClassifier(err error) bool {
	return err != nil
}

// CircuitBreaker implements the Circuit Breaker pattern for fault tolerance.
// It uses atomic operations for the hot path and a mutex for state transitions.
type CircuitBreaker struct {
	lastFailTime     int64           // Absolute Unix Nano timestamp of the last failure in Open state.
	errorClassifier  ErrorClassifier // Function to identify retryable failures.
	openTimeout      time.Duration   // Cooldown period before trying recovery.
	windowDuration   time.Duration   // Duration of the sliding window for statistics.
	minRequests      uint64          // Minimum traffic required before evaluating threshold.
	errorThreshold   int64           // Failure rate threshold scaled by 1024 (fixed-point).
	requests         atomic.Uint64   // Counter for total requests in the current window.
	failures         atomic.Uint64   // Counter for failed requests in the current window.
	windowStart      atomic.Int64    // Unix Nano timestamp of the current window start.
	mu               sync.Mutex      // Protects state transitions and lastFailTime.
	state            atomic.Uint32   // Current State (Closed, Open, Half-Open).
	halfOpenInFlight bool            // Ensures only one request is tested during Half-Open.
}

// NewBreaker creates a new CircuitBreaker with default or custom options.
func NewBreaker(opts ...Option) *CircuitBreaker {
	cb := &CircuitBreaker{
		openTimeout:     5 * time.Second,
		windowDuration:  10 * time.Second,
		minRequests:     20,
		errorThreshold:  512, // Default: 50% failure rate (0.5 * 1024).
		errorClassifier: defaultClassifier,
	}

	for _, opt := range opts {
		opt(cb)
	}

	cb.state.Store(uint32(StateClosed))
	cb.windowStart.Store(time.Now().UnixNano())

	return cb
}

// rotateWindow resets the statistical window if the duration has expired.
// Uses Atomic Compare-and-Swap (CAS) to avoid locking during statistics collection.
func (cb *CircuitBreaker) rotateWindow(now int64) {
	start := cb.windowStart.Load()

	if now-start < int64(cb.windowDuration) {
		return
	}

	if cb.windowStart.CompareAndSwap(start, now) {
		cb.requests.Store(0)
		cb.failures.Store(0)
	}
}

func (cb *CircuitBreaker) isFailure(err error) bool {
	return cb.errorClassifier(err)
}
