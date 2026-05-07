package rhttp

import (
	"net/http"
	"time"
)

// Request represents a single logical execution within the resilience pipeline.
// It carries the original http.Request along with deadlines and cached timestamps
// to minimize system calls and facilitate cross-middleware coordination.
type Request struct {
	Req             *http.Request // The original HTTP request to be executed.
	RequestDeadline int64         // Absolute Unix Nano timestamp for the global request deadline.
	TryDeadline     int64         // Absolute Unix Nano timestamp for a single attempt deadline.
	Now             int64         // Cached Unix Nano timestamp to avoid redundant time.Now() calls.
}

// Handler is the functional interface for executing a Request.
// Middlewares wrap these handlers to inject resilience logic.
type Handler func(Request) (*http.Response, error)

// Middleware is a higher-order function that takes a Handler and returns a decorated Handler.
type Middleware func(Handler) Handler

// Client is the primary entry point for the resilience library.
// It holds the compiled middleware chain.
type Client struct {
	chain Handler
}

// Do executes the given http.Request through the pre-configured resilience pipeline.
// It initializes the Request context, including the initial timestamp (Now).
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	r := Request{
		Req: req,
		Now: time.Now().UnixNano(), // Capture time once at the entry point.
	}

	return c.chain(r)
}

type action struct {
	h   Handler
	req Request
}

func (a action) Execute() (resp *http.Response, err error) {
	return a.h(a.req)
}

func (a action) Now() int64 {
	return a.req.Now
}

func (a action) RequestDeadline() int64 {
	return a.req.RequestDeadline
}
