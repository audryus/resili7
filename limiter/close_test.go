package limiter

import (
	"testing"
	"time"
)

func TestLimiterCloseStopsControlLoop(t *testing.T) {
	l := NewLimiter(WithUpdateInterval(10 * time.Millisecond))

	if err := l.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	select {
	case <-l.done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("control loop did not stop after Close")
	}
}
