package rgrpc

import (
	"context"

	"google.golang.org/grpc"
)

// Request represents a single logical execution within the gRPC resilience pipeline.
// It carries the gRPC call parameters along with deadlines and cached timestamps
// to minimize system calls and facilitate cross-middleware coordination.
type Request struct {
	Ctx             context.Context
	Req             any
	Reply           any
	Cc              *grpc.ClientConn
	Invoker         grpc.UnaryInvoker
	Method          string
	Opts            []grpc.CallOption
	RequestDeadline int64
	TryDeadline     int64
	Now             int64
}

// Handler is the functional interface for executing a Request.
// Middlewares wrap these handlers to inject resilience logic.
type Handler func(Request) error

// Middleware is a higher-order function that takes a Handler and returns a decorated Handler.
type Middleware func(Handler) Handler

// action implements the timeout.ResultAction interface for gRPC calls.
// Using a struct and generics in timeout.ExecuteWithResult helps keep allocations near zero.
type action struct {
	h   Handler
	req Request
}

func (a action) Execute() (Request, error) {
	return a.req, a.h(a.req)
}

func (a action) Now() int64 {
	return a.req.Now
}

func (a action) RequestDeadline() int64 {
	return a.req.RequestDeadline
}
