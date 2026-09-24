package hedge_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/hedge"
)

// Winner result is returned; slow losers must observe cancellation so they
// do not run to completion (user accepted alloc cost of context threading).
func TestHedgeLoserCancelled(t *testing.T) {
	var calls atomic.Int64
	var loserAborted atomic.Bool

	fn := func(ctx context.Context) (int, error) {
		n := calls.Add(1)
		if n == 1 {
			// Primary: slow enough to trigger the hedge, then succeeds.
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		}
		// Loser: must abort on cancel, not sleep to completion.
		select {
		case <-ctx.Done():
			loserAborted.Store(true)
			return 0, ctx.Err()
		case <-time.After(5 * time.Second):
			return 99, nil
		}
	}

	resp, err := hedge.ExecuteWithResultCtx(context.Background(), 10*time.Millisecond, 2, fn)
	if err != nil {
		t.Fatalf("expected winner result, got %v", err)
	}
	if resp != 1 {
		t.Fatalf("expected winner resp 1, got %d", resp)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !loserAborted.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !loserAborted.Load() {
		t.Fatal("loser attempt was not cancelled after winner returned")
	}
}

// Fast path (single attempt / non-positive delay) executes exactly once.
func TestHedgeFastPathSingleAttempt(t *testing.T) {
	var calls atomic.Int64
	fn := func(context.Context) (int, error) {
		calls.Add(1)
		return 7, nil
	}
	resp, err := hedge.ExecuteWithResultCtx(context.Background(), 0, 1, fn)
	if err != nil || resp != 7 || calls.Load() != 1 {
		t.Fatalf("fast path: resp=%d err=%v calls=%d", resp, err, calls.Load())
	}
}

// Caller context cancellation aborts the wait.
func TestHedgeCallerContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	fn := func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(5 * time.Second):
			return 1, nil
		}
	}
	_, err := hedge.ExecuteWithResultCtx(ctx, 5*time.Millisecond, 3, fn)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// Legacy fire-and-forget variant still works for non-context-aware actions.
func TestHedgeLegacyExecute(t *testing.T) {
	var calls atomic.Int64
	a := legacyAction{fn: func() (int, error) {
		calls.Add(1)
		return 3, nil
	}}
	resp, err := hedge.ExecuteWithResult(50*time.Millisecond, 2, a)
	if err != nil || resp != 3 {
		t.Fatalf("legacy: resp=%d err=%v", resp, err)
	}
	if calls.Load() < 1 {
		t.Fatal("legacy: action never executed")
	}
	if err := hedge.Execute(50*time.Millisecond, 2, legacyAction{fn: func() (int, error) {
		return 0, nil
	}}); err != nil {
		t.Fatalf("legacy Execute: %v", err)
	}
	if err := hedge.ExecuteCtx(context.Background(), 50*time.Millisecond, 2, a); err != nil {
		t.Fatalf("legacy ExecuteCtx: %v", err)
	}
}

type legacyAction struct{ fn func() (int, error) }

func (a legacyAction) Execute() (int, error) { return a.fn() }
