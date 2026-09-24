package timeout_test

import (
	"errors"
	"testing"
	"time"

	"github.com/audryus/resili7/timeout"
)

type stubAction struct {
	now int64
	err error
}

func (a stubAction) Execute() (stubAction, error) {
	return a, a.err
}

func (a stubAction) Now() int64 {
	return a.now
}

func (a stubAction) RequestDeadline() int64 {
	return a.now + int64(10*time.Millisecond)
}

func TestTimeoutExecutesWithoutErrorWhenWithinDeadline(t *testing.T) {
	a := stubAction{now: time.Now().UnixNano()}
	_, err := timeout.ExecuteWithResult(100*time.Millisecond, a)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestTimeoutReturnsErrorWhenDeadlineHasPassed(t *testing.T) {
	a := stubAction{now: time.Now().UnixNano() - int64(20*time.Millisecond)}
	_, err := timeout.ExecuteWithResult(10*time.Millisecond, a)
	if !errors.Is(err, timeout.ErrTimeout) {
		t.Fatalf("expected timeout error, got %v", err)
	}
}
