# ext-proc-tests

Integration tests for Envoy's [ext-proc](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter) filter, focused on long-running AI streaming scenarios. Each test spins up an Envoy container via [testcontainers-go](https://golang.testcontainers.org/) alongside an in-process gRPC ext-proc server and a mock SSE backend.

The primary goal is comparing **STREAMED** vs **BUFFERED** response body modes to determine which is viable for proxying real-time AI token streams.

## Prerequisites

- Go 1.25+
- Docker

## Test packages

### `memory/`

Tests how Envoy's memory scales with response size and ext-proc backpressure.

- **STREAMED** (M1–M3): Memory stays bounded (~3 MB) regardless of total data volume, even under heavy backpressure from a slow ext-proc.
- **BUFFERED** (M4–M6): Envoy buffers the entire response body. Responses exceeding the 1 MiB per-connection buffer limit fail with HTTP 500.

### `timeout/`

Tests `message_timeout` and `stream_idle_timeout` behavior.

- **STREAMED** (T1–T4): `message_timeout` applies per-chunk — a slow ext-proc kills the stream mid-flight (headers already committed, so client sees 200 + EOF). `stream_idle_timeout` resets on each chunk, so steady token flow keeps the connection alive.
- **BUFFERED** (T5–T6): `message_timeout` applies to the single buffered body message. Since headers aren't committed yet, Envoy can return a proper 504. Backend streaming time is not covered by `message_timeout` in either mode.

### `ttfb/`

Measures time-to-first-byte.

- **STREAMED**: TTFB equals the first chunk interval (~1s).
- **BUFFERED**: TTFB equals the entire backend duration (5s+) since Envoy waits for the full response before forwarding.

### `longevity/`

Verifies sustained streaming over minutes.

- **L1** (STREAMED, 5 min): Completes with ~300 chunks, no degradation.
- **L2** (BUFFERED, 2 min, tiny data): Technically succeeds but TTFB = 2 full minutes — the client sees nothing until the backend finishes.

### `concurrency/`

Runs 10/50/100 simultaneous STREAMED connections for 30 seconds each.

All streams complete with zero failures. Memory scales linearly at ~0.07 MB per concurrent stream (~15 MB for 100 streams).

## Running

```bash
# all tests in parallel (longevity tests take ~7 min)
go test -v -count=1 -timeout 30m ./...

# single package
go test -v -count=1 ./memory/

# skip longevity/concurrency, run only fast tests (~1 min)
go test -v -count=1 -timeout 5m ./memory/ ./timeout/ ./ttfb/

# skip all integration tests
go test -short ./...
```
 SOME NEW COMMIT
 SOME NEW STUFF