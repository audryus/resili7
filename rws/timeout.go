package rws

import (
	"errors"
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
func NewTimeoutMiddleware(cfg TimeoutConfig) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*Response, error) {
			now := time.Now().UnixNano()
			sessionDeadline := now + int64(cfg.Session)

			// Check session deadline before executing.
			if cfg.Session > 0 && now > sessionDeadline {
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

			// Check session deadline after execution.
			if err == nil && cfg.Session > 0 && time.Now().UnixNano() > sessionDeadline {
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
// This handles both gorilla/websocket errors and underlying TCP errors.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if err == websocket.ErrCloseSent {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "read tcp") && strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "websocket: read limit exceeded")
}
