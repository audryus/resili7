package resili7

import (
	"errors"
	"net/http"
	"time"
)

var ErrClientCreation = errors.New("client creation failed: requires an http.Client or a custom Handler")

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

// HTTPHandler wraps a standard http.Client.Do call into the resilience Handler interface.
func HTTPHandler(c *http.Client) Handler {
	return func(r Request) (*http.Response, error) {
		return c.Do(r.Req)
	}
}

// Pipeline defines the configuration for building a resilience chain.
// The order of execution follows: Limiter -> CircuitBreaker -> Timeout -> Retry -> Hedge -> Handler.
type Pipeline struct {
	HttpCient      *http.Client // The underlying HTTP client to use.
	HttpHandler    Handler      // An optional custom handler (useful for testing or non-HTTP protocols).
	Timeout        Middleware   // Global timeout enforcement.
	Retry          Middleware   // Sequential retry logic with backoff.
	Hedge          Middleware   // Parallel hedging strategy.
	Limiter        Middleware   // Rate limiting or concurrency control.
	CircuitBreaker Middleware   // Fault tolerance and failure isolation.
}

// NewClient assembles a resilience pipeline into a functional Client.
// It chains middlewares in the correct order to ensure optimal protection and performance.
func NewClient(pipeline Pipeline) (*Client, error) {
	var h Handler
	if pipeline.HttpHandler != nil {
		h = pipeline.HttpHandler
	} else if pipeline.HttpCient != nil {
		h = HTTPHandler(pipeline.HttpCient)
	} else {
		return nil, ErrClientCreation
	}

	// Chain order: Handler -> Hedge -> Retry -> Timeout -> CircuitBreaker -> Limiter.
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

	return &Client{
		chain: h,
	}, nil
}
