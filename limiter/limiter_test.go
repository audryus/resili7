package limiter_test

import (
	"testing"
	"time"

	"github.com/audryus/resili7/limiter"
)

// Acquire honors the initial limit and Done releases permits with an
// explicit end timestamp (clock-optimized: no hidden time.Now in Done).
func TestLimiterAcquireReleaseWithTimestamps(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1), limiter.WithLimits(1, 1))
	defer l.Close()

	start, ok := l.Acquire(1000)
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	if start != 1000 {
		t.Fatalf("Acquire must echo caller timestamp, got %d", start)
	}
	if _, ok := l.Acquire(1001); ok {
		t.Fatal("expected second acquire to fail at limit 1")
	}
	l.Done(start, 2000, true)
	if _, ok := l.Acquire(2001); !ok {
		t.Fatal("expected acquire to succeed after Done")
	}
}

// Close is idempotent and stops the control loop.
func TestLimiterCloseIdempotent(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithUpdateInterval(time.Millisecond))
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !l.IsStopped() {
		t.Fatal("control loop should be stopped")
	}
}
