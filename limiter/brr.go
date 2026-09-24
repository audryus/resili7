package limiter

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Limiter implements an adaptive concurrency limiting algorithm inspired by TCP BBR.
// It estimates the optimal concurrency limit by tracking Minimum RTT and bandwidth,
// adjusting dynamically to prevent queue build-up and congestion.
type Limiter struct {
	opts       Options
	limit      atomic.Int64  // Current allowed concurrency limit.
	inflight   atomic.Int64  // Number of requests currently being processed.
	rttMin     atomic.Int64  // Lowest observed RTT in the current window, in nanoseconds.
	rttEMA     atomic.Int64  // Exponential moving average of RTT, in nanoseconds.
	cycleIndex atomic.Uint32 // Current position in the gain cycling loop.
	stopCh     chan struct{} // Signal channel used to stop the background control loop.
	done       chan struct{} // Closed when the background control loop exits.
	closeOnce  sync.Once     // Ensures shutdown runs only once.
}

// NewLimiter initializes a new adaptive limiter.
func NewLimiter(opts ...Option) *Limiter {
	o := defaultOptions()

	for _, fn := range opts {
		fn(&o)
	}

	// Guard against misconfiguration that would panic in adjust():
	// an empty gain cycle would divide by zero, non-positive limits or
	// intervals would wedge the control loop.
	if len(o.GainCycle) == 0 {
		o.GainCycle = defaultOptions().GainCycle
	}
	if o.InitialLimit <= 0 {
		o.InitialLimit = 1
	}
	if o.MinLimit <= 0 {
		o.MinLimit = 1
	}
	if o.MaxLimit < o.MinLimit {
		o.MaxLimit = o.MinLimit
	}
	if o.UpdateInterval <= 0 {
		o.UpdateInterval = 200 * time.Millisecond
	}

	l := &Limiter{
		opts:   o,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}

	l.limit.Store(o.InitialLimit)
	l.rttMin.Store(int64(time.Hour)) // Initialize with a high value.

	// Start the background control loop to adjust limits periodically.
	// A finalizer is set as a safety net so a forgotten Close does not leak
	// the goroutine forever; explicit Close remains the supported path.
	go l.controlLoop()
	runtime.SetFinalizer(l, func(l *Limiter) { _ = l.Close() })

	return l
}

// Close stops the limiter's background control loop and waits for it to finish.
func (l *Limiter) Close() error {
	l.closeOnce.Do(func() {
		close(l.stopCh)
	})
	<-l.done
	return nil
}

// IsStopped reports whether the background control loop has exited.
func (l *Limiter) IsStopped() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// Acquire attempts to gain a permit to proceed with a request.
// It returns the start timestamp and a boolean indicating success.
// If the current inflight requests exceed the limit, it returns (0, false).
func (l *Limiter) Acquire(now int64) (int64, bool) {
	for {
		in := l.inflight.Load()
		lim := l.limit.Load()

		if in >= lim {
			return 0, false
		}

		// Atomically increment inflight count.
		if l.inflight.CompareAndSwap(in, in+1) {
			return now, true
		}
	}
}

// Done releases a permit and records the request latency (RTT).
// It should be called after the request finishes. end is the completion
// timestamp (UnixNano); passing it explicitly honors the clock-optimized
// contract — callers capture time once and reuse it instead of paying for an
// extra time.Now() call here.
//
// A Done without a matching Acquire (or a double Done) is ignored instead of
// driving inflight negative.
func (l *Limiter) Done(start, end int64, success bool) {
	for {
		in := l.inflight.Load()
		if in <= 0 {
			return
		}
		if l.inflight.CompareAndSwap(in, in-1) {
			break
		}
	}

	if !success {
		// We only adjust based on successful samples to avoid poisoning metrics
		// with fast failure errors.
		return
	}

	// Calculate RTT and update statistical models.
	l.observeRTT(end - start)
}

// observeRTT updates the Minimum RTT and the Exponential Moving Average.
// Uses fixed-point arithmetic (shift by 10) for the EMA calculation.
func (l *Limiter) observeRTT(ns int64) {
	// Update Min RTT using Atomic CAS.
	for {
		min := l.rttMin.Load()
		if ns >= min {
			break
		}
		if l.rttMin.CompareAndSwap(min, ns) {
			break
		}
	}

	// Update RTT EMA using Atomic CAS and fixed-point math.
	for {
		old := l.rttEMA.Load()

		if old == 0 {
			if l.rttEMA.CompareAndSwap(0, ns) {
				return
			}
			continue
		}

		smoothing := l.opts.Smoothing
		// EMA formula: old * (1 - smoothing) + ns * smoothing
		ema := (old*(1024-smoothing) + ns*smoothing) >> 10

		if l.rttEMA.CompareAndSwap(old, ema) {
			return
		}
	}
}

// controlLoop periodically triggers the limit adjustment logic.
func (l *Limiter) controlLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("limiter control loop recovered from panic: %v", r)
		}
		close(l.done)
	}()

	t := time.NewTicker(l.opts.UpdateInterval)
	defer t.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-t.C:
			l.adjust()
		}
	}
}

// adjust recalculates the concurrency limit based on observed RTT and gain cycles.
// It detects congestion when current EMA RTT significantly exceeds the Minimum RTT.
func (l *Limiter) adjust() {
	minRTT := l.rttMin.Load()
	emaRTT := l.rttEMA.Load()

	if emaRTT == 0 {
		return
	}

	queue := emaRTT - minRTT
	limit := l.limit.Load()

	// GAIN CYCLING (BBR Probing Strategy)
	// We rotate through different gain values to discover extra capacity or drain queues.
	idx := l.cycleIndex.Add(1)
	gain := l.opts.GainCycle[int(idx)%len(l.opts.GainCycle)]

	// target = current_limit * gain
	target := (limit * gain) >> 10

	if queue > 0 {
		// Congestion detected: reduce target proportionally to the RTT increase.
		// factor = (minRTT / emaRTT) * 1024
		factor := (minRTT << 10) / emaRTT
		target = (target * factor) >> 10
	}

	// Enforce min/max boundaries.
	if target < l.opts.MinLimit {
		target = l.opts.MinLimit
	}
	if target > l.opts.MaxLimit {
		target = l.opts.MaxLimit
	}

	l.limit.Store(target)
}
