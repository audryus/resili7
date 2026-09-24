package retry_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/audryus/resili7/retry"
)

// loopAction is a controllable ResultAction for exercising the retry loop.
type loopAction struct {
	now      int64
	deadline int64
	calls    *atomic.Int64
	failWith error
	succeed  bool
}

func (a loopAction) Execute() (int, error) {
	if a.calls != nil {
		a.calls.Add(1)
	}
	if a.succeed {
		return 42, nil
	}
	return 0, a.failWith
}
func (a loopAction) Now() int64                          { return a.now }
func (a loopAction) RequestDeadline() int64              { return a.deadline }
func (a loopAction) IsSuccess(_ int, err error) bool     { return err == nil }
func (a loopAction) ShouldRetryDefault(_ int, _ error) bool { return true }
func (a loopAction) Err(err error) error {
	if err == nil {
		return retry.ErrRetry
	}
	return errors.Join(retry.ErrRetry, err)
}

func testPolicy() *retry.RetryPolicy[int] {
	return &retry.RetryPolicy[int]{
		MaxAttempts: 3,
		Backoff:     func(_ int) time.Duration { return 0 },
		ShouldRetry: func(_ int, _ error) bool { return true },
	}
}

// Success on the first attempt records budget and returns the value.
func TestRetryExecuteWrapper(t *testing.T) {
	if err := retry.Execute(testPolicy(), loopAction{
		now:     time.Now().UnixNano(),
		succeed: true,
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

func TestRetrySuccessFirstAttempt(t *testing.T) {	b := retry.NewBudget(0.5)
	for range 10 {
		b.RecordSuccess()
	}
	p := testPolicy()
	p.Budget = b
	var calls atomic.Int64
	resp, err := retry.ExecuteWithResult(p, loopAction{
		now:     time.Now().UnixNano(),
		calls:   &calls,
		succeed: true,
	})
	if err != nil || resp != 42 {
		t.Fatalf("resp=%d err=%v", resp, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 attempt, got %d", calls.Load())
	}
}

// A non-retryable outcome returns the response and error untouched.
func TestRetryNonRetryablePassthrough(t *testing.T) {
	p := testPolicy()
	p.ShouldRetry = func(_ int, _ error) bool { return false }
	sentinel := errors.New("fatal")
	var calls atomic.Int64
	_, err := retry.ExecuteWithResult(p, loopAction{
		now:      time.Now().UnixNano(),
		calls:    &calls,
		failWith: sentinel,
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel passthrough, got %v", err)
	}
	if errors.Is(err, retry.ErrRetry) {
		t.Fatalf("non-retryable error must not be marked exhausted: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 attempt, got %d", calls.Load())
	}
}

// Exhausted attempts join ErrRetry with the last error.
func TestRetryExhaustionJoinsLastError(t *testing.T) {
	p := testPolicy()
	sentinel := errors.New("downstream down")
	var calls atomic.Int64
	_, err := retry.ExecuteWithResult(p, loopAction{
		now:      time.Now().UnixNano(),
		calls:    &calls,
		failWith: sentinel,
	})
	if !errors.Is(err, retry.ErrRetry) {
		t.Fatalf("expected ErrRetry, got %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected causal error preserved, got %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls.Load())
	}
}

// A denied budget discards the response and reports ErrRetry without sleeping.
func TestRetryBudgetDenied(t *testing.T) {
	b := retry.NewBudget(0.1) // no successes: no retries allowed
	p := testPolicy()
	p.Budget = b
	var calls atomic.Int64
	_, err := retry.ExecuteWithResult(p, loopAction{
		now:      time.Now().UnixNano(),
		calls:    &calls,
		failWith: errors.New("boom"),
	})
	if !errors.Is(err, retry.ErrRetry) {
		t.Fatalf("expected ErrRetry, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 attempt (budget denied), got %d", calls.Load())
	}
}

// Per-try deadline exceeded by a slow attempt reports ErrTimeout.
func TestRetryPerTryDeadline(t *testing.T) {
	p := testPolicy()
	p.TryDeadline = 20 * time.Millisecond
	slow := loopAction{
		now:      time.Now().UnixNano(),
		failWith: errors.New("boom"),
	}
	// Wrap Execute with a sleep to simulate a slow handler.
	_, err := retry.ExecuteWithResult(p, slowWrapper{loopAction: slow, block: 50 * time.Millisecond})
	if !errors.Is(err, retry.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

type slowWrapper struct {
	loopAction
	block time.Duration
}

func (a slowWrapper) Execute() (int, error) {
	time.Sleep(a.block)
	return a.loopAction.Execute()
}

// Backoff is clamped to the remaining global deadline.
func TestRetryBackoffClampedToDeadline(t *testing.T) {
	p := testPolicy()
	p.MaxAttempts = 2
	p.Backoff = func(_ int) time.Duration { return time.Hour }
	now := time.Now().UnixNano()
	start := time.Now()
	_, err := retry.ExecuteWithResult(p, loopAction{
		now:      now,
		deadline: now + int64(50*time.Millisecond),
		failWith: errors.New("boom"),
	})
	if !errors.Is(err, retry.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("backoff not clamped: took %v", elapsed)
	}
}
