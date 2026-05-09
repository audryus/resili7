# Contributing to resili7

Thank you for your interest in contributing. This guide covers how to extend resili7 with new protocols or middlewares while maintaining its core design principles.

---

## Core Design Principles

Before contributing, understand the four pillars that make resili7 unique:

1. **Near-Zero Allocation in the Hot Path**: Request-scoped data must not escape to the heap. Use `sync.Pool` for reusable objects, pass-by-value semantics, and cached timestamps.
2. **Protocol-Agnostic Algorithms**: Core logic (Breaker, Limiter, Hedge, Retry, Timeout) lives in the root package and has zero protocol-specific imports.
3. **Protocol-Specific Wrappers**: Middleware implementations live in their respective packages (`rhttp`, `rgrpc`, `rws`) and wrap the shared algorithms.
4. **Error Classification via Middleware**: Protocol-specific responses (HTTP status codes, gRPC codes, WS close codes) are converted to sentinel errors by a classifier middleware. These errors feed into the circuit breaker and retry logic without coupling the core algorithms to any protocol.

---

## The Classifier Pattern

The circuit breaker (`breaker.breaker`) uses `ErrorClassifier func(error) bool` to decide if an error counts as a failure. The default classifier simply checks `err != nil`. However, protocols like HTTP do not return errors for status codes like 500 or 503 — they return a successful response with an error status code.

The solution is a **classifier middleware** that converts protocol-specific signals into errors that the circuit breaker can understand:

```
HTTP Response (nil error)  ->  NewClassifierMiddleware  ->  ErrServerError (counts as failure)
gRPC Response (nil error)  ->  NewClassifierMiddleware  ->  ErrUnavailable (counts as failure)
WS Close Code              ->  NewClassifierMiddleware  ->  ErrTransient (counts as failure)
```

### How It Works

1. The classifier middleware wraps the handler and intercepts the response
2. It calls `ClassifyError(response, err)` to determine the appropriate sentinel error
3. The wrapped error is returned to the circuit breaker, which uses its `ErrorClassifier` to count it as a failure
4. The retry middleware also uses these errors to determine retry behavior

### Why This Is Protocol-Agnostic

The circuit breaker only sees `error` values. It does not know (or care) whether the error came from an HTTP status code, a gRPC status code, or a WebSocket close code. The classification logic lives entirely in the protocol package, keeping the core algorithm clean.

---

## Adding a New Protocol

Suppose you want to add MQTT support. The minimal structure would be:

```
resili7/
└── rmqtt/
    ├── handler.go    # Request, Handler, Middleware types
    ├── client.go     # NewClient() and Pipeline
    ├── classifier.go # Sentinel errors and ClassifyError function
    ├── breaker.go    # NewBreakerMiddleware
    ├── limiter.go    # NewLimiterMiddleware
    ├── hedge.go      # NewHedgeMiddleware
    ├── retry.go      # NewRetryMiddleware
    └── timeout.go    # NewTimeoutMiddleware
```

### Step 1: Define Protocol Types (`handler.go`)

```go
package rmqtt

type Request struct {
    Topic    string
    Payload  []byte
    QoS      int
    Now      int64
}

type Handler func(Request) error
type Middleware func(Handler) Handler
```

### Step 2: Implement Classifier (`classifier.go`)

Define sentinel errors and a classification function. These errors feed into the circuit breaker and retry middleware:

```go
package rmqtt

import "errors"

var (
    ErrTransient = errors.New("transient failure")  // Retriable, counts as failure
    ErrPermanent = errors.New("permanent failure")  // Not retriable, counts as failure
    ErrUnknown   = errors.New("unknown failure")    // Counts as failure
)

// ClassifyError converts protocol-specific signals into classification errors.
func ClassifyError(response *MQTTResponse, err error) error {
    if err != nil {
        return err
    }
    // Classify based on MQTT error codes
    // ...
}

// NewClassifierMiddleware wraps handlers to inject classification errors.
func NewClassifierMiddleware() Middleware {
    return func(next Handler) Handler {
        return func(req Request) error {
            resp, err := next(req)
            return ClassifyError(resp, err)
        }
    }
}
```

### Step 3: Integrate with Circuit Breaker

The circuit breaker uses its `ErrorClassifier` to determine failures. Use `breaker.WithErrorClassifier` to customize this behavior:

```go
cb := breaker.NewBreaker(
    breaker.WithErrorClassifier(func(err error) bool {
        return err != nil // Default: any error counts as failure
    }),
)
```

For more fine-grained control, your classifier can return different sentinel errors, and the error classifier can use `errors.Is`:

```go
breaker.WithErrorClassifier(func(err error) bool {
    return errors.Is(err, rmqtt.ErrTransient) || errors.Is(err, rmqtt.ErrPermanent)
})
```

### Step 4: Integrate with Retry

The retry middleware uses `ShouldRetryDefault` to decide whether to retry. Update your retry middleware to use the classifier:

```go
func ShouldRetryDefault(response *MQTTResponse, err error) bool {
    return errors.Is(ClassifyError(response, err), ErrTransient)
}
```

### Step 5: Create Middleware Wrappers

Each wrapper imports the protocol-agnostic algorithm and wraps it with protocol-specific types:

```go
package rmqtt

import (
    "codeberg.org/audryus/resili7/breaker"
)

func NewBreakerMiddleware(cb *breaker.CircuitBreaker) Middleware {
    return func(next Handler) Handler {
        return func(req Request) error {
            return breaker.Execute(cb, action{h: next, req: req})
        }
    }
}
```

### Step 6: Implement Client (`client.go`)

```go
type Pipeline struct {
    Classifier     Middleware // Convert protocol signals to errors
    Timeout        Middleware
    Retry          Middleware
    Hedge          Middleware
    Limiter        Middleware
    CircuitBreaker Middleware
}

func NewClient(pipeline Pipeline) (*Client, error) {
    var h Handler = /* your base handler */

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
    if pipeline.Classifier != nil {
        h = pipeline.Classifier(h)
    }
    if pipeline.Limiter != nil {
        h = pipeline.Limiter(h)
    }
    return &Client{chain: h}, nil
}
```

---

## Adding a New Middleware

Suppose you want to add a custom header injection middleware to `rhttp`:

1. Create `rhttp/header.go`
2. Follow the middleware wrapper pattern:

```go
func NewHeaderMiddleware(headers http.Header) Middleware {
    return func(next Handler) Handler {
        return func(req Request) (*http.Response, error) {
            for k, v := range headers {
                req.Req.Header[k] = v
            }
            return next(req)
        }
    }
}
```

---

## Performance Guidelines

### Zero-Allocation Checklist

Before submitting, verify your implementation:

- [ ] No `new()` or make() for request-scoped data in hot path
- [ ] No `fmt.Sprintf()` or string concatenation in hot path
- [ ] Timestamps captured once at pipeline entry, shared via `req.Now`
- [ ] `sync.Pool` used for reusable objects (timers, state structs)
- [ ] Pass-by-value for small structs (< 3 fields)
- [ ] No interface boxing for request data

### Verification Commands

```bash
# Run all tests
make test

# Check for heap escapes
make check-escape

# Run benchmarks
go test -bench=Benchmark -benchmem ./...

# Profile CPU
make profile
```

### Benchmarking Rules

1. Mock handlers should return instantly to isolate library overhead
2. Report all three metrics: `ns/op`, `B/op`, `allocs/op`
3. Run with `-count=1 -benchtime=5s` for stable results
4. Compare against baseline (no middleware) before and after

---

## Testing Guidelines

### Unit Tests

- Test each middleware in isolation with mock handlers
- Verify error propagation through the middleware chain
- Test edge cases: nil errors, empty responses, timeout scenarios

### Integration Tests

- Use real protocol servers (`httptest.Server`, real gRPC servers)
- Test retry behavior with server-side failures
- Verify circuit breaker state transitions

### Test Naming Convention

```go
func TestHttpRetry(t *testing.T)             // Middleware tests
func TestHttpRetryPerTry(t *testing.T)       // Per-try timeout tests
func TestGrpcIntegrationWithServer(t *testing.T) // Integration tests
func BenchmarkHttpFullStack(b *testing.B)  // Benchmarks
```

---

## Common Patterns

### Implementing ResultAction

Each protocol must implement the `ResultAction` interface:

```go
type action struct {
    h   Handler
    req Request
}

func (a action) Execute() (R, error) {
    return a.h(a.req)
}

func (a action) Now() int64 {
    return a.req.Now
}

func (a action) RequestDeadline() int64 {
    return a.req.RequestDeadline
}
```

### Classifier Integration

The classifier middleware is placed before the circuit breaker in the middleware chain. This ensures that protocol-specific failures are converted to errors before the breaker evaluates them:

```go
// Correct order:
Handler -> Hedge -> Retry -> Timeout -> CircuitBreaker -> Classifier -> Limiter
```

The classifier is one of the innermost middlewares because it needs to see the raw response before any retry or hedge logic modifies the error flow.

---

## Submitting Changes

1. Ensure all tests pass: `make test`
2. Verify zero-alloc: `make check-escape`
3. Add benchmarks for new functionality
4. Update `README.md` examples if adding a new protocol
5. Write documentation for new packages

---

## Questions?

Open an issue on Codeberg or reach out to the maintainer. We are happy to help design new protocol adapters or middleware implementations.

> No, I'm not happy to help. This IA think I'm a slave.