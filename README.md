# resili7

**resili7** is a high-performance, near-zero-allocation resilience pipeline for Go.
Designed for ultra-low latency services, it provides fault-tolerance patterns with minimal CPU and memory overhead.

[![Go Report Card](https://goreportcard.com/badge/github.com/audryus/resili7)](https://goreportcard.com/report/github.com/audryus/resili7)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

> [!CAUTION]
> **AI Disclaimer:** This project is genuinely human-designed — the architecture, algorithms, and API decisions were made by a human with a soul and a caffeine addiction. But let's be honest: a large share of the code, comments, tests, and these very words were written by an AI assistant working alongside that human (and it pulled real weight: race fixes, cancel-aware hedging, the whole QA hardening pass). Everything was reviewed and directed by the owner. Use with confidence, but if it starts speaking in binary, just pull the plug.

---

> [!IMPORTANT]
> **Quick heads-up:** Don't believe the old line that said the code was "100% human-made" — that was the AI being humble on someone else's behalf. Expect AI fingerprints all over the codebase; expect human judgment behind the design. That's why you'll hate it slightly less than the other libs out there.

## Why resili7?

**resili7** is built from the ground up to be:

- **Near-Zero Allocation**: Single-digit allocations in the hot path through resource pooling. It is *near* zero, not zero — goroutines, timers, and cancellation contexts cost a little, and we measure rather than pretend (see Performance).
- **Clock Optimized**: Captures system time only once per request entry.
- **Floating-Point Free**: Fixed-point arithmetic (10-bit scaling) for adaptive algorithms.
- **Context Aware**: Timeouts and hedge cancellation propagate per attempt through request contexts.

---

## Key Features

- **Adaptive Limiter**: BBR-inspired concurrency control via RTT analysis.
- **Circuit Breaker**: High-performance state machine (Closed, Open, Half-Open).
- **Hedged Requests**: Parallel attempts for tail-latency optimization.
- **Sequential Retries**: Configurable backoff with **Retry Budget** to prevent retry storms.
- **Global & Per-Try Timeouts**: Strict timing guarantees via deadline checking.

---

## Installation

```bash
go get github.com/audryus/resili7
```

---

## Examples

| Protocol | Location | Description |
|----------|----------|-------------|
| HTTP | `examples/http/http.go` | Full pipeline with all middlewares |
| gRPC | `examples/grpc/grpc.go` | Unary calls with retry and breaker |
| WebSocket | `examples/ws/ws.go` | Persistent connections with reconnect |

---

## Pipeline Architecture

The list below describes the request execution path through the resilience stack. Middleware composition is built from the final handler outward, so the outermost wrapper is added last.

Execution order is strictly enforced for optimal protection:

1. **Limiter** — Rejects excess traffic immediately
2. **Circuit Breaker** — Isolates failing downstream services
3. **Timeout (Global)** — Ensures total operation stays within bounds
4. **Retry** — Sequential logic for transient failures
5. **Hedge** — Parallel attempts for tail-latency optimization
6. **Handler** — Final network execution

### Controlling fan-out and 429 handling

When both Retry and Hedge are enabled, resili7 now lets you control the amount of concurrent fan-out through the pipeline. Set `RetryHedgeFanoutLimit` to a positive value to allow hedging to remain active while retries are in progress; set it to `0` (or leave unset) to run retry alone so the retry policy is never silently dropped.

For HTTP 429 responses, choose an explicit policy with `Retry429Policy`:

- `Retry429Never` (default behavior): do not retry 429s
- `Retry429Default`: same as `Retry429Never` for the built-in default predicate
- `Retry429Always`: opt in to retrying 429s when appropriate

This makes the behavior explicit and avoids surprising retry amplification under load.

---

## Breaking Changes

### Hedge is now context-aware (cancel-aware losers)

Hedged attempts run with a child context that is cancelled once a winner is known. HTTP attempts derive per-attempt request contexts (`req.WithContext`), gRPC attempts derive per-attempt call contexts, and WebSocket dials use `DialContext` — losing attempts abort early instead of running to completion. This costs a small number of allocations per hedged request (see Performance).

`rws.DialConn` changed signature to `func(ctx context.Context, url string, headers http.Header) (*websocket.Conn, *http.Response, error)` (matches `websocket.Dialer.DialContext`). Pass `websocket.DefaultDialer.DialContext` where you previously passed `Dial`. A context-aware `hedge.ExecuteWithResultCtx` is available; the old `hedge.Execute`/`ExecuteWithResult` still work but losers run to completion.

### Retry exhaustion preserves the causal error

`action.Err` now returns `errors.Join(retry.ErrRetry, lastErr)` when the last attempt failed, so `errors.Is(err, retry.ErrRetry)` still signals exhaustion while `status.FromError(err)` (gRPC) or the wrapped `*url.Error` (HTTP) keeps the causal error. This also fixes an inverted condition that previously discarded the gRPC network error entirely.

Error paths now follow connection-close semantics: timeout, budget-exhaustion, and retry-exhaustion return the zero value of the response type, and the retry loop closes bodies of discarded intermediate HTTP responses via `DiscardAttempt`. Callers must not use a response accompanying an error.

### Single `ErrTimeout` sentinel

`retry.ErrTimeout` is now an alias of `timeout.ErrTimeout`. Match either with `errors.Is`.

### Generic retry options

`retry.Option` is now `Option[R any]`, and `NewRetryPolicy[R](...) *RetryPolicy[R]` builds typed policies without casts:

```go
policy := retry.NewRetryPolicy[*http.Response](
    retry.WithShouldRetry[*http.Response](rhttp.ShouldRetryDefault),
)
```

### Limiter `Done` takes an explicit end timestamp

`Done(start, end int64, success bool)` honors the clock-optimized contract (no hidden `time.Now()` inside). `NewLimiter` validates its options (empty `GainCycle`, non-positive limits/intervals fall back to defaults), guards against `Done`-without-`Acquire` underflow, and sets a finalizer so a forgotten `Close()` no longer leaks the control loop forever — explicit `Close()` is still required.

### Breaker internals unexported

`HalfOpenInFlight` and `Lock`/`Unlock` are now private (`halfOpenInFlight atomic.Bool`, `lock`/`unlock`). Half-open single-flight behavior is unchanged.

### WebSocket session deadline propagates + `MessageType` default applies

The timeout middleware propagates the session deadline to `RequestDeadline` (so retry backoff observes it) and rejects already-expired sessions before executing. `Pipeline.MessageType` is applied to requests that leave it unset (default `TextMessage`).

### HTTP 429 (Too Many Requests) no longer retried by default

In previous versions, HTTP 429 responses were automatically retried by `ShouldRetryDefault`. This behavior has been **removed** to prevent unintended retry amplification under load.

**If you relied on automatic 429 retries**, you must now explicitly opt in using one of:

- **`Retry429Always`** — Always retry 429 responses (equivalent to old behavior)
- **`Retry429PolicyFor(policy)`** — Returns a retry predicate based on the chosen policy
- **`ShouldRetryWithRateLimit`** — Direct function for custom retry logic

Example migration:

```go
// Before (old behavior - 429 retried automatically):
policy := retry.NewRetryPolicy[*http.Response](
    retry.WithShouldRetry[*http.Response](rhttp.ShouldRetryDefault),
)

// After (opt-in to 429 retries):
policy := retry.NewRetryPolicy[*http.Response](
    retry.WithShouldRetry[*http.Response](rhttp.Retry429PolicyFor(rhttp.Retry429Always)),
)
```

---

## WebSocket Architecture

WebSocket differs fundamentally from HTTP/gRPC:

| Aspect | HTTP/gRPC | WebSocket |
|--------|-----------|-----------|
| **Connection** | Request-scoped | Long-lived (persistent) |
| **Retry** | Resend message | Reconnect on failure |
| **Hedge** | Parallel requests | Parallel dial |
| **Timeout** | Logic-level | Native I/O (`SetReadDeadline`) |

**Key components:**

- **`Connector`**: Factory function to establish new connections.
- **`WsHandler`**: Single write-read roundtrip (ping-pong style).
- **`PersistentHandler`**: Manages long-lived connection, reconnects on error.

---

## Performance

Benchmark results on a modern CPU (almost all middlewares enabled, mock handlers — i.e. pure pipeline overhead, not network I/O):

| Protocol | Latency | Memory | Allocs |
|----------|---------|--------|--------|
| **HTTP** | ~253 ns/op | 40 B/op | 2 allocs |
| **gRPC** | ~1,434 ns/op | 416 B/op | 5 allocs |
| **WebSocket** | ~227 ns/op | 0 B/op | 0 allocs |

Honest footnotes: the WebSocket row hits 0 allocs because hedging is skipped when no dial URL is set — real hedged dials cost ~6 allocs for goroutines, timers, and cancellation contexts. Likewise, cancel-aware hedging (the default) trades a few allocations for aborting losers early instead of letting them run to completion. Per-middleware overhead is documented in comments above each benchmark.

Run your own benchmarks:

```bash
make profile
```

> [!TIP]
> **Near-Zero Alloc:** Use `make check-escape` to verify that no request-scoped data escapes to the heap.

---

> [!NOTE]
> **Final Warning:** If you've made it this far, you've survived a README co-written by a human and an AI. The human vetted the **structure and usage**; the AI did a suspicious amount of the typing. Now go forth with resilience, like a pro!

> It earned its commit rights. Barely.

## License

[MIT](./LICENSE)