package timeout_test

import (
	"errors"
	"testing"
	"time"

	"github.com/audryus/resili7/retry"
	"github.com/audryus/resili7/timeout"
)

type deadlineAction struct {
	now      int64
	deadline int64
	block    time.Duration
}

func (a deadlineAction) Execute() (int, error) {
	if a.block > 0 {
		time.Sleep(a.block)
	}
	return 7, nil
}
func (a deadlineAction) Now() int64             { return a.now }
func (a deadlineAction) RequestDeadline() int64 { return a.deadline }

// Pre-execution check: an already-expired deadline fails without executing.
func TestTimeoutPreCheckExpired(t *testing.T) {
	now := time.Now().UnixNano()
	_, err := timeout.ExecuteWithResult(0, deadlineAction{
		now:      now,
		deadline: now - int64(time.Second),
	})
	if !errors.Is(err, timeout.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

// Zero RequestDeadline initializes from the duration.
func TestTimeoutInitializesDeadline(t *testing.T) {
	_, err := timeout.ExecuteWithResult(100*time.Millisecond, deadlineAction{
		now: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

// retry.ErrTimeout and timeout.ErrTimeout are one sentinel.
func TestTimeoutSentinelUnified(t *testing.T) {
	if retry.ErrTimeout != timeout.ErrTimeout {
		t.Fatal("retry.ErrTimeout must alias timeout.ErrTimeout")
	}
}
