package rws

import (
	"errors"
	"net"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// Error definitions for WebSocket-specific timeout scenarios.
var (
	ErrDialTimeout    = errors.New("dial timeout")
	ErrReadTimeout    = errors.New("read timeout")
	ErrSessionTimeout = errors.New("session timeout")
)

// TimeoutConfig defines three levels of WebSocket timeouts.
// WebSocket resilience operates at the connection level, not per-message like HTTP/gRPC.
type TimeoutConfig struct {
	Dial    time.Duration // Maximum time to establish a connection.
	Read    time.Duration // Maximum time to wait for a message read.
	Session time.Duration // Maximum total session duration.
}

// NewTimeoutMiddleware creates a middleware that enforces WebSocket-native timeouts.
// It uses SetReadDeadline/SetWriteDeadline on the connection for I/O-level timeout,
// distinct from HTTP/gRPC logic-level timeout enforcement.
//
// The session deadline is propagated to req.RequestDeadline so downstream layers
// (notably retry backoff) can observe the global deadline. Connection-close
// semantics: on session expiry the connection is closed and the caller gets a
// nil response with ErrSessionTimeout — never a partial response.
func NewTimeoutMiddleware(cfg TimeoutConfig) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*Response, error) {
			now := time.Now().UnixNano()

			var sessionDeadline int64
			if cfg.Session > 0 {
				if req.RequestDeadline > 0 && req.RequestDeadline < now+int64(cfg.Session) {
					// An outer layer already imposed a tighter deadline: honor it.
					sessionDeadline = req.RequestDeadline
				} else {
					sessionDeadline = now + int64(cfg.Session)
				}
				// Propagate so the retry layer's deadline clamp can fire.
				req.RequestDeadline = sessionDeadline
			} else {
				sessionDeadline = req.RequestDeadline
			}

			// Pre-execution check: fail immediately if the session already expired.
			if sessionDeadline > 0 && now > sessionDeadline {
				return nil, ErrSessionTimeout
			}

			// Set per-request read/write deadlines if configured.
			if cfg.Read > 0 && req.Conn != nil {
				deadline := time.Now().Add(cfg.Read)
				req.Conn.SetReadDeadline(deadline)
				req.Conn.SetWriteDeadline(deadline)
				defer req.Conn.SetReadDeadline(time.Time{})
				defer req.Conn.SetWriteDeadline(time.Time{})
			}

			// Propagate timeout config to request for handler use.
			req.TimeoutConfig = cfg

			resp, err := next(req)

			// Convert network timeout errors to ErrReadTimeout.
			if isTimeoutError(err) {
				return nil, ErrReadTimeout
			}

			// Post-execution check: a response that finished after the session
			// deadline is discarded (connection closed) regardless of error.
			if sessionDeadline > 0 && time.Now().UnixNano() > sessionDeadline {
				if req.Conn != nil {
					req.Conn.Close()
				}
				return nil, ErrSessionTimeout
			}

			return resp, err
		}
	}
}

// isTimeoutError checks if the error is a WebSocket or network timeout.
// It prefers errors.As with net.Error (robust against wrapping) and falls
// back to message matching for gorilla-specific errors.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if err == websocket.ErrCloseSent {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "read tcp") && strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "websocket: read limit exceeded")
}
