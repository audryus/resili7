package limiter_test

import (
	"testing"
	"time"

	"codeberg.org/audryus/resili7/limiter"
)

// M2: empty GainCycle must not panic with div-by-zero in adjust().
func TestLimiterEmptyGainCycleNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("empty GainCycle panicked: %v", r)
		}
	}()
	l := limiter.NewLimiter(
		limiter.WithInitialLimit(10),
		limiter.WithGainCycle([]int64{}),
		limiter.WithUpdateInterval(time.Millisecond),
	)
	defer l.Close()
	start, ok := l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	l.Done(start, time.Now().UnixNano(), true)
	time.Sleep(10 * time.Millisecond) // let at least one adjust() run
}

// N6: Done without a matching Acquire (double-Done) must not drive inflight
// negative and wedge the limiter open.
func TestLimiterDoubleDoneNoUnderflow(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1), limiter.WithLimits(1, 1))
	defer l.Close()
	start, ok := l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	l.Done(start, time.Now().UnixNano(), true)
	l.Done(start, time.Now().UnixNano(), true) // spurious second Done

	// Limit is 1: only one outstanding permit may exist.
	s1, ok := l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	if _, ok := l.Acquire(time.Now().UnixNano()); ok {
		t.Fatal("limiter over-admitted after double Done (inflight underflow)")
	}
	l.Done(s1, time.Now().UnixNano(), true)
}
