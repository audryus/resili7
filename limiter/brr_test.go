package limiter_test

import (
	"testing"
	"time"

	"github.com/audryus/resili7/limiter"
)

// TestLimiter_AcquireExhaust verifies that the limiter correctly tracks permits
// and denies requests once the InitialLimit is reached.
func TestLimiter_AcquireExhaust(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(2), limiter.WithLimits(1, 10))

	// Acquire Permit 1
	start1, ok := l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatalf("Expected to acquire first permit")
	}

	// Acquire Permit 2
	_, ok = l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatalf("Expected to acquire second permit")
	}

	// Acquire Permit 3 - should fail as limit is 2.
	_, ok = l.Acquire(time.Now().UnixNano())
	if ok {
		t.Fatalf("Expected to fail acquiring third permit")
	}

	// Release Permit 1.
	l.Done(start1, time.Now().UnixNano(), true)

	// Acquire again - should succeed now that a permit was released.
	_, ok = l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatalf("Expected to acquire permit after Done")
	}
}

// TestLimiter_DoneFails ensures that permits are correctly released
// even when the request itself was reported as a failure.
func TestLimiter_DoneFails(t *testing.T) {
	l := limiter.NewLimiter(limiter.WithInitialLimit(1))

	start, ok := l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatalf("Expected to acquire permit")
	}

	// Report failure.
	l.Done(start, time.Now().UnixNano(), false)

	// The permit should still be freed.
	_, ok = l.Acquire(time.Now().UnixNano())
	if !ok {
		t.Fatalf("Expected to acquire permit after Done(false)")
	}
}
