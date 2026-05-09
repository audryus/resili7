package rhttp

import (
	"errors"
	"net/http"
)

var ErrClientCreation = errors.New("client creation failed: requires an http.Client or a custom Handler")

// HttPHandler wraps a standard http.Client.Do call into the resilience Handler interface.
func HttpHandler(c *http.Client) Handler {
	return func(r Request) (*http.Response, error) {
		return c.Do(r.Req)
	}
}

// Pipeline defines the configuration for building a resilience chain.
// The order of execution follows: Limiter -> CircuitBreaker -> Timeout -> Retry -> Hedge -> Handler.
type Pipeline struct {
	HttpClient     *http.Client // The underlying HTTP client to use.
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
	} else if pipeline.HttpClient != nil {
		h = HttpHandler(pipeline.HttpClient)
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
