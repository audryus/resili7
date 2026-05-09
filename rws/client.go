package rws

import (
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var ErrClientCreation = errors.New("client creation failed: requires a Handler")

// WsHandler wraps a websocket.Conn into a Handler.
// It performs a single write-read roundtrip: sends the request message and returns the response.
func WsHandler(c *websocket.Conn) Handler {
	return func(r Request) (*Response, error) {
		// Apply per-request read timeout if configured.
		if r.TimeoutConfig.Read > 0 && r.Conn != nil {
			deadline := time.Now().Add(r.TimeoutConfig.Read)
			r.Conn.SetReadDeadline(deadline)
			r.Conn.SetWriteDeadline(deadline)
			defer r.Conn.SetReadDeadline(time.Time{})
			defer r.Conn.SetWriteDeadline(time.Time{})
		}

		// Write message if payload is provided.
		if r.Data != nil {
			if err := c.WriteMessage(r.MessageType, r.Data); err != nil {
				return nil, err
			}
		}

		mt, data, err := c.ReadMessage()
		if err != nil {
			return nil, err
		}

		return &Response{MessageType: mt, Data: data}, nil
	}
}

// Connector is a function type that establishes a new WebSocket connection.
type Connector func() (*websocket.Conn, error)

// PersistentHandler manages a long-lived WebSocket connection.
// It reuses the connection for multiple requests and re-establishes it
// only when an error occurs. This works in tandem with the Retry middleware
// to provide transparent re-connection.
func PersistentHandler(connector Connector) Handler {
	var mu sync.Mutex
	var conn *websocket.Conn

	return func(r Request) (*Response, error) {
		mu.Lock()
		// Adopt connection from hedge middleware if available, otherwise create new.
		if conn == nil {
			if r.Conn != nil {
				conn = r.Conn
			} else {
				c, err := connector()
				if err != nil {
					mu.Unlock()
					return nil, err
				}
				conn = c
			}
		}
		activeConn := conn
		mu.Unlock()

		h := WsHandler(activeConn)
		r.Conn = activeConn
		resp, err := h(r)

		if err != nil {
			mu.Lock()
			// On error, invalidate the connection so next request triggers reconnection.
			if conn == activeConn {
				activeConn.Close()
				conn = nil
			}
			mu.Unlock()
			return nil, err
		}

		return resp, nil
	}
}

// Pipeline defines the configuration for building a WebSocket resilience chain.
// The order of execution follows: Limiter -> CircuitBreaker -> Timeout -> Retry -> Hedge -> Handler.
type Pipeline struct {
	Connector      Connector  // Function to establish new connections.
	WsHandler      Handler    // An optional custom handler (useful for testing).
	Timeout        Middleware // Session/read/dial timeout enforcement.
	Retry          Middleware // Reconnection logic with backoff.
	Hedge          Middleware // Parallel dial hedging.
	Limiter        Middleware // Concurrency control.
	CircuitBreaker Middleware // Fault isolation.
	MessageType    int       // Default WebSocket message type for writes.
}

// NewClient assembles a resilience pipeline into a functional Client.
// It chains middlewares in the correct order to ensure optimal protection and performance.
func NewClient(pipeline Pipeline) (*Client, error) {
	var h Handler

	if pipeline.WsHandler != nil {
		h = pipeline.WsHandler
	} else if pipeline.Connector != nil {
		h = PersistentHandler(pipeline.Connector)
	} else {
		return nil, ErrClientCreation
	}

	// Chain order: Limiter -> CircuitBreaker -> Timeout -> Retry -> Hedge -> Handler.
	// Last added middleware becomes the outermost layer in the call stack.

	if pipeline.Hedge != nil {
		h = pipeline.Hedge(h)
	}

	if pipeline.Retry != nil {
		h = pipeline.Retry(h)
	}

	if pipeline.Timeout != nil {
		h = pipeline.Timeout(h)
	}

	if pipeline.CircuitBreaker != nil {
		h = pipeline.CircuitBreaker(h)
	}

	if pipeline.Limiter != nil {
		h = pipeline.Limiter(h)
	}

	return &Client{chain: h}, nil
}
