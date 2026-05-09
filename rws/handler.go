package rws

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Response wraps the WebSocket message with its type and payload.
type Response struct {
	Data        []byte
	MessageType int
}

// Request represents a single WebSocket operation.
// For connection-level resilience (timeout, hedge, retry), it holds connection parameters.
// For message-level resilience, it holds the active connection and message data.
type Request struct {
	Headers         http.Header     // Optional headers for connection.
	Conn            *websocket.Conn // Active WebSocket connection.
	URL             string          // Target WebSocket URL (for dial/hedge).
	Data            []byte          // Message payload.
	TimeoutConfig   TimeoutConfig   // Per-request timeout configuration.
	MessageType     int             // WebSocket message type (TextMessage, BinaryMessage, etc.).
	Now             int64           // Cached Unix Nano timestamp (set once at pipeline entry).
	RequestDeadline int64           // Absolute Unix Nano timestamp for session deadline.
}

// Handler is the functional interface for executing a Request.
// Middlewares wrap these handlers to inject resilience logic.
type Handler func(Request) (*Response, error)

// Middleware is a higher-order function that takes a Handler and returns a decorated Handler.
type Middleware func(Handler) Handler

// Client is the primary entry point for the WebSocket resilience library.
// It holds the compiled middleware chain and executes requests through the pipeline.
type Client struct {
	chain Handler
}

// Send executes the given Request through the pre-configured resilience pipeline.
// It initializes the Request context, including the initial timestamp (Now).
func (c *Client) Send(r Request) (*Response, error) {
	if r.Now == 0 {
		r.Now = time.Now().UnixNano()
	}
	return c.chain(r)
}

// action implements the retry.ResultAction interface for WebSocket operations.
// It bridges the generic action pattern with the concrete Request/Response types.
type action struct {
	h   Handler
	req Request
}

// Execute runs the next handler in the chain with the current request.
func (a action) Execute() (*Response, error) {
	return a.h(a.req)
}

// Now returns the cached timestamp from the request.
func (a action) Now() int64 {
	return a.req.Now
}

// RequestDeadline returns the session deadline from the request.
func (a action) RequestDeadline() int64 {
	return a.req.RequestDeadline
}
