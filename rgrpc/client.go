package rgrpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// Pipeline defines the configuration for building a gRPC resilience chain.
// The order of execution follows: Limiter -> CircuitBreaker -> Timeout -> Retry -> Hedge -> Handler.
type Pipeline struct {
	Timeout        Middleware // Global timeout enforcement.
	Retry          Middleware // Sequential retry logic with backoff.
	Hedge          Middleware // Parallel hedging strategy.
	Limiter        Middleware // Rate limiting or concurrency control.
	CircuitBreaker Middleware // Fault tolerance and failure isolation.
}

// UnaryClientInterceptor assembles a resilience pipeline into a gRPC UnaryClientInterceptor.
// It chains middlewares in the correct order to ensure optimal protection and performance.
func NewUnaryClientInterceptor(pipeline Pipeline) grpc.UnaryClientInterceptor {
	// Base handler that finally performs the gRPC call.
	var h Handler = func(r Request) error {
		return r.Invoker(r.Ctx, r.Method, r.Req, r.Reply, r.Cc, r.Opts...)
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

	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		r := Request{
			Ctx:     ctx,
			Method:  method,
			Req:     req,
			Reply:   reply,
			Cc:      cc,
			Opts:    opts,
			Invoker: invoker,
			Now:     time.Now().UnixNano(), // Capture time once at the entry point.
		}

		return h(r)
	}
}
