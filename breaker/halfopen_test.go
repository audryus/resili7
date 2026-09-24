package breaker_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/breaker"
)

type probeAction struct {
	now   int64
	block chan struct{}
	err   error
}

func (a probeAction) Execute() (int, error) {
	if a.block != nil {
		<-a.block
	}
	return 0, a.err
}
func (a probeAction) Now() int64 { return a.now }

// Only one probe may be in flight during half-open: concurrent probes beyond
// the first are rejected with ErrCircuitOpen.
func TestBreakerHalfOpenSingleFlight(t *testing.T) {
	cb := breaker.NewBreaker(
		breaker.WithTimeout(20*time.Millisecond),
		breaker.WithWindow(time.Hour),
		breaker.WithMinRequests(1),
		breaker.WithErrorThreshold(0.5),
	)

	// Trip the breaker: one failing request with minRequests=1 and 100% errors.
	_, _ = breaker.ExecuteWithResult(cb, probeAction{now: time.Now().UnixNano(), err: errors.New("boom")})

	// Wait for the open timeout to elapse so the next request half-opens.
	time.Sleep(50 * time.Millisecond)

	release := make(chan struct{})
	var entered atomic.Int64
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := breaker.ExecuteWithResult(cb, countingProbe{
				now:     time.Now().UnixNano(),
				release: release,
				entered: &entered,
			})
			if errors.Is(err, breaker.ErrCircuitOpen) {
				rejected.Add(1)
			}
		}()
	}
	// Let all goroutines reach the probe, then release the winner.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if entered.Load() != 1 {
		t.Fatalf("expected exactly 1 half-open probe, got %d", entered.Load())
	}
	if rejected.Load() != 7 {
		t.Fatalf("expected 7 rejections, got %d", rejected.Load())
	}
}

type countingProbe struct {
	now     int64
	release chan struct{}
	entered *atomic.Int64
}

func (a countingProbe) Execute() (int, error) {
	a.entered.Add(1)
	<-a.release
	return 0, nil
}
func (a countingProbe) Now() int64 { return a.now }
