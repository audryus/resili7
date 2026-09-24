package rws

import (
	"errors"
	"io"
	"net"

	"codeberg.org/audryus/resili7/retry"
	"github.com/gorilla/websocket"
)

// NewRetryMiddleware creates a middleware that implements sequential retries for WebSocket.
// It uses a provided RetryPolicy to manage backoff, budgets, and per-try deadlines.
// For WebSocket, retry semantics mean reconnection rather than message resend.
func NewRetryMiddleware(policy *retry.RetryPolicy[*Response]) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*Response, error) {
			return retry.ExecuteWithResult(policy, action{
				h:   next,
				req: req,
			})
		}
	}
}

// IsSuccess returns true if the response is considered successful.
// For WebSocket, success means no error occurred.
func (a action) IsSuccess(resp *Response, err error) bool {
	return err == nil
}

// ShouldRetryDefault delegates to the package-level ShouldRetryDefault function.
func (a action) ShouldRetryDefault(resp *Response, err error) bool {
	return ShouldRetryDefault(resp, err)
}

// Err wraps the error for retry handling.
// It joins retry.ErrRetry with the last error so exhaustion is signalled
// while the causal error stays unwrappable. A nil error yields ErrRetry.
func (a action) Err(err error) error {
	if err == nil {
		return retry.ErrRetry
	}
	return errors.Join(retry.ErrRetry, err)
}

// ShouldRetryDefault returns true if the error warrants a retry.
// Retries on WebSocket close codes indicating temporary failures, network errors,
// and unexpected connection closures.
var ShouldRetryDefault = func(response *Response, err error) bool {
	if err == nil {
		return false
	}

	// Retry on close codes indicating temporary server-side issues.
	if websocket.IsCloseError(err,
		websocket.CloseServiceRestart,    // 1012: Server restarting
		websocket.CloseTryAgainLater,     // 1013: Temporary failure / Rate limit
		websocket.CloseInternalServerErr, // 1011: Internal server error
	) {
		return true
	}

	// Retry on unexpected closures, except for normal termination.
	if websocket.IsUnexpectedCloseError(err,
		websocket.CloseNormalClosure, // 1000
		websocket.CloseGoingAway,     // 1001
	) {
		return true
	}

	// Retry on network and I/O errors.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Retry when connection is in an invalid state.
	if errors.Is(err, websocket.ErrCloseSent) || errors.Is(err, websocket.ErrBadHandshake) {
		return true
	}

	return false
}
