package retry_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/retry"
)

// C3: concurrent AllowRetry must not overshoot the budget.
// With 100 successes at ratio 0.2, allowed = 20. Hammering from 64
// goroutines must grant at most 20 (+1 for the winning CAS race margin).
func TestBudgetConcurrentOvershoot(t *testing.T) {
	b := retry.NewBudget(0.2)
	for range 100 {
		b.RecordSuccess()
	}
	var granted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.AllowRetry() {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := granted.Load(); got > 21 {
		t.Fatalf("budget overshoot: granted %d retries, want <= 21", got)
	}
}

// N1: Decay racing with AllowRetry/RecordSuccess must not lose updates
// or panic under -race.
func TestBudgetDecayConcurrent(t *testing.T) {
	b := retry.NewBudget(0.5)
	for range 100 {
		b.RecordSuccess()
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				b.RecordSuccess()
				b.AllowRetry()
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				b.Decay()
			}
		}()
	}
	wg.Wait()
}

// N2: ratio outside (0,1] must be clamped to the default, not silently
// produce a >100% quota.
func TestBudgetRatioClamped(t *testing.T) {
	b := retry.NewBudget(5.0)
	for range 10 {
		b.RecordSuccess()
	}
	allowed := 0
	for range 20 {
		if b.AllowRetry() {
			allowed++
		}
	}
	// Default 0.2 => 10 successes allow 2 retries. 5.0 unclamped would allow 50.
	if allowed > 3 {
		t.Fatalf("ratio not clamped: %d retries granted for 10 successes", allowed)
	}
}

// retryAction is a stub ResultAction for deadline tests.
type retryAction struct {
	now      int64
	deadline int64
	calls    *atomic.Int64
	block    time.Duration
}

func (a retryAction) Execute() (int, error) {
	if a.calls != nil {
		a.calls.Add(1)
	}
	if a.block > 0 {
		time.Sleep(a.block)
	}
	return 0, errors.New("boom")
}
func (a retryAction) Now() int64                      { return a.now }
func (a retryAction) RequestDeadline() int64          { return a.deadline }
func (a retryAction) IsSuccess(_ int, _ error) bool   { return false }
func (a retryAction) ShouldRetryDefault(_ int, _ error) bool { return true }
func (a retryAction) Err(err error) error {
	if err == nil {
		return retry.ErrRetry
	}
	return err
}

// H3: backoff sleep must respect the global deadline — no extra Execute
// after the deadline has passed, and return is prompt.
func TestRetryRespectsDeadlineDuringBackoff(t *testing.T) {
	var calls atomic.Int64
	now := time.Now().UnixNano()
	policy := &retry.RetryPolicy[int]{
		MaxAttempts: 10,
		Backoff:     func(_ int) time.Duration { return 5 * time.Second },
		ShouldRetry: func(_ int, _ error) bool { return true },
		TryDeadline: 0,
	}
	start := time.Now()
	_, err := retry.ExecuteWithResult(policy, retryAction{
		now:      now,
		deadline: now + int64(100*time.Millisecond),
		calls:    &calls,
	})
	elapsed := time.Since(start)
	if !errors.Is(err, retry.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if got := calls.Load(); got > 2 {
		t.Fatalf("executed %d attempts past deadline, want <= 2", got)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("deadline ignored during backoff: took %v", elapsed)
	}
}

// M2: nil policy must not panic.
func TestRetryNilPolicyNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil policy panicked: %v", r)
		}
	}()
	var calls atomic.Int64
	_, _ = retry.ExecuteWithResult[int](nil, retryAction{
		now:      time.Now().UnixNano(),
		deadline: 0,
		calls:    &calls,
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("nil policy: expected exactly 1 attempt, got %d", got)
	}
}
