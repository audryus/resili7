package timeout_test

import (
	"errors"
	"testing"
	"time"

	"github.com/audryus/resili7/timeout"
)

type slowAction struct {
	now      int64
	deadline int64
}

func (a slowAction) Execute() (string, error) {
	time.Sleep(20 * time.Millisecond)
	return "late-partial", nil
}
func (a slowAction) Now() int64             { return a.now }
func (a slowAction) RequestDeadline() int64 { return a.deadline }

// N4 (connection-close semantics): when the handler finishes after the
// deadline, the caller must get the zero value — never a partial response
// it might treat as usable (e.g. an HTTP Body it won't Close).
func TestTimeoutReturnsZeroValueAfterDeadline(t *testing.T) {
	now := time.Now().UnixNano()
	resp, err := timeout.ExecuteWithResult(5*time.Millisecond, slowAction{
		now:      now,
		deadline: now + int64(5*time.Millisecond),
	})
	if !errors.Is(err, timeout.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if resp != "" {
		t.Fatalf("expected zero response on timeout, got %q", resp)
	}
}
