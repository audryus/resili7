package breaker

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBreakerConcurrentWindowRotation(t *testing.T) {
	cb := NewBreaker(WithWindow(10*time.Millisecond), WithMinRequests(1), WithErrorThreshold(0.5))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = ExecuteWithResult(cb, actionStub{now: time.Now().UnixNano(), err: errors.New("boom")})
		}()
	}
	wg.Wait()

	if got := cb.requests.Load(); got == 0 {
		t.Fatal("expected requests to be counted")
	}
}

type actionStub struct {
	now int64
	err error
}

func (a actionStub) Execute() (struct{}, error) {
	return struct{}{}, a.err
}

func (a actionStub) Now() int64 {
	return a.now
}
