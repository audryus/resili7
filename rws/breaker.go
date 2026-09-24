package rws

import (
	"github.com/audryus/resili7/breaker"
)

// NewBreakerMiddleware creates a middleware that protects the handler using a circuit breaker.
// It tracks failure rates and opens the circuit if a threshold is reached,
// preventing further requests from reaching failing downstream services.
func NewBreakerMiddleware(cb *breaker.CircuitBreaker) Middleware {
	return func(next Handler) Handler {
		return func(req Request) (*Response, error) {
			return breaker.ExecuteWithResult(cb, action{
				h:   next,
				req: req,
			})
		}
	}
}
