// Inference integrity tests: POST /inference with request body + SSE response.
// Tests both request body integrity (SHA256 match) and response body integrity (checksum).
//
//	CLIENT  --POST /inference-->  ENVOY  --N chained FD_STREAMED ext-proc-->  BACKEND
//	         <---SSE stream----          <------StreamedBodyResponse------
package fullduplexstreamed_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/crstrn13/ext-proc-tests"
	fds "github.com/crstrn13/ext-proc-tests/full_duplex_streamed"
)

func TestInferenceIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, depth := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			t.Parallel()
			env := fds.SetupChainedEnvN(t, depth)

			// Single-shot inference — one request per sub-test
			singleTests := []struct {
				name       string
				bodySize   int
				chunkSize  int
				intervalMs int
				holdSecs   int
			}{
				{"single_2KB_small_chunks", 2 * 1024, 1024, 10, 1},
				{"single_50KB_small_chunks", 50 * 1024, 1024, 10, 5},
				{"single_500KB_large_chunks", 500 * 1024, 100 * 1024, 50, 5},
				{"single_2MB_small_chunks", 2 * 1024 * 1024, 10 * 1024, 5, 5},
				{"single_2MB_large_chunks", 2 * 1024 * 1024, 100 * 1024, 25, 5},
			}
			for _, tc := range singleTests {
				t.Run(tc.name, func(t *testing.T) {
					body := makeInferenceBody("test-model-7b", tc.bodySize)
					headers := map[string]string{
						"x-hold-seconds":      strconv.Itoa(tc.holdSecs),
						"x-chunk-interval-ms": strconv.Itoa(tc.intervalMs),
						"x-chunk-size":        strconv.Itoa(tc.chunkSize),
						"x-full-duplex":       "true",
					}
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tc.holdSecs+30)*time.Second)
					defer cancel()

					r := DoInferenceRequest(ctx, env.EnvoyURL, body, headers)

					t.Logf("depth=%d %s status=%d ttfb=%v chunks=%d reqMatch=%v respMatch=%v",
						depth, tc.name, r.StatusCode, r.TTFB.Round(time.Millisecond),
						r.Chunks, r.RequestBodyMatch,
						r.ResponseHash == r.ExpectedHash)

					require.Equal(t, 200, r.StatusCode)
					require.True(t, r.Complete, "stream did not complete")
					require.NoError(t, r.Err)
					assert.True(t, r.RequestBodyMatch, "request body corrupted")
					assert.Equal(t, r.ExpectedHash, r.ResponseHash, "response body corrupted")
				})
			}

			// Rapid-fire inference — sequential back-to-back
			rapidTests := []struct {
				name       string
				bodySize   int
				chunkSize  int
				intervalMs int
				holdSecs   int
				iterations int
			}{
				{"rapid_2KB", 2 * 1024, 1024, 10, 1, 50},
				{"rapid_50KB", 50 * 1024, 1024, 10, 1, 50},
				{"rapid_500KB", 500 * 1024, 1024, 5, 1, 30},
				{"rapid_2MB", 2 * 1024 * 1024, 10 * 1024, 5, 1, 20},
			}
			for _, tc := range rapidTests {
				t.Run(tc.name, func(t *testing.T) {
					body := makeInferenceBody("test-model-7b", tc.bodySize)
					headers := map[string]string{
						"x-hold-seconds":      strconv.Itoa(tc.holdSecs),
						"x-chunk-interval-ms": strconv.Itoa(tc.intervalMs),
						"x-chunk-size":        strconv.Itoa(tc.chunkSize),
						"x-full-duplex":       "true",
					}
					before := make([]int64, len(env.Procs))
					for i, p := range env.Procs {
						before[i] = p.RequestBodyMismatches()
					}
					var transport, reqCorrupt, respCorrupt int
					for range tc.iterations {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tc.holdSecs+30)*time.Second)
						r := DoInferenceRequest(ctx, env.EnvoyURL, body, headers)
						cancel()
						if r.Err != nil || r.StatusCode != 200 || !r.Complete {
							transport++
						} else {
							if !r.RequestBodyMatch {
								reqCorrupt++
							}
							if r.ResponseHash != r.ExpectedHash {
								respCorrupt++
							}
						}
					}
					for i, p := range env.Procs {
						t.Logf("  filter[%d] reqBodyMismatches=%d", i, p.RequestBodyMismatches()-before[i])
					}
					t.Logf("depth=%d %s iters=%d transport=%d reqCorrupt=%d respCorrupt=%d",
						depth, tc.name, tc.iterations, transport, reqCorrupt, respCorrupt)
					assert.Equal(t, 0, transport, "transport errors")
					assert.Equal(t, 0, reqCorrupt, "request body corruption")
					assert.Equal(t, 0, respCorrupt, "response body corruption")
				})
			}

			// Concurrent inference — parallel requests
			concTests := []struct {
				name        string
				bodySize    int
				chunkSize   int
				intervalMs  int
				holdSecs    int
				concurrency int
			}{
				{"conc_2KB_x20", 2 * 1024, 1024, 10, 1, 20},
				{"conc_50KB_x20", 50 * 1024, 1024, 10, 1, 20},
				{"conc_500KB_x20", 500 * 1024, 1024, 5, 1, 20},
				{"conc_2MB_x10", 2 * 1024 * 1024, 10 * 1024, 5, 1, 10},
				{"conc_50KB_x50", 50 * 1024, 1024, 10, 1, 50},
				{"conc_500KB_x50", 500 * 1024, 1024, 5, 1, 50},
			}
			for _, tc := range concTests {
				t.Run(tc.name, func(t *testing.T) {
					body := makeInferenceBody("test-model-7b", tc.bodySize)
					headers := map[string]string{
						"x-hold-seconds":      strconv.Itoa(tc.holdSecs),
						"x-chunk-interval-ms": strconv.Itoa(tc.intervalMs),
						"x-chunk-size":        strconv.Itoa(tc.chunkSize),
						"x-full-duplex":       "true",
					}
					before := make([]int64, len(env.Procs))
					for i, p := range env.Procs {
						before[i] = p.RequestBodyMismatches()
					}
					var (
						wg                                sync.WaitGroup
						mu                                sync.Mutex
						transport, reqCorrupt, respCorrupt int
					)
					for i := range tc.concurrency {
						wg.Add(1)
						go func(id int) {
							defer wg.Done()
							ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tc.holdSecs+30)*time.Second)
							defer cancel()
							r := DoInferenceRequest(ctx, env.EnvoyURL, body, headers)
							mu.Lock()
							defer mu.Unlock()
							if r.Err != nil || r.StatusCode != 200 || !r.Complete {
								transport++
							} else {
								if !r.RequestBodyMatch {
									reqCorrupt++
								}
								if r.ResponseHash != r.ExpectedHash {
									respCorrupt++
								}
							}
						}(i)
					}
					wg.Wait()
					for i, p := range env.Procs {
						t.Logf("  filter[%d] reqBodyMismatches=%d", i, p.RequestBodyMismatches()-before[i])
					}
					t.Logf("depth=%d %s conc=%d transport=%d reqCorrupt=%d respCorrupt=%d",
						depth, tc.name, tc.concurrency, transport, reqCorrupt, respCorrupt)
					assert.Equal(t, 0, transport, "transport errors")
					assert.Equal(t, 0, reqCorrupt, "request body corruption")
					assert.Equal(t, 0, respCorrupt, "response body corruption")
				})
			}
		})
	}
}

// --- Longevity tests ---

func TestLongevity10m(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	runLongevity(t, 2, 10*time.Minute)
}

func TestLongevity30m(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	runLongevity(t, 2, 30*time.Minute)
}

func TestLongevity1h(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	runLongevity(t, 2, 1*time.Hour)
}

func runLongevity(t *testing.T, depth int, duration time.Duration) {
	t.Helper()

	env := fds.SetupChainedEnvN(t, depth, func(cfg *EnvoyConfig) {
		cfg.StreamIdleTimeoutSec = int(duration.Seconds()) + 120
	})

	type payloadConfig struct {
		name       string
		size       int
		chunkSize  int
		intervalMs int
		holdSecs   int
	}
	payloads := []payloadConfig{
		{"2KB", 2 * 1024, 1024, 50, 3},
		{"50KB", 50 * 1024, 1024, 50, 3},
		{"500KB", 500 * 1024, 10 * 1024, 25, 3},
		{"2MB", 2 * 1024 * 1024, 100 * 1024, 25, 3},
	}

	deadline := time.Now().Add(duration)
	var total, transportErrors, reqIntegrity, respIntegrity int

	for time.Now().Before(deadline) {
		pc := payloads[total%len(payloads)]
		body := makeInferenceBody("test-model-7b", pc.size)
		headers := map[string]string{
			"x-hold-seconds":      strconv.Itoa(pc.holdSecs),
			"x-chunk-interval-ms": strconv.Itoa(pc.intervalMs),
			"x-chunk-size":        strconv.Itoa(pc.chunkSize),
			"x-full-duplex":       "true",
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pc.holdSecs+30)*time.Second)
		r := DoInferenceRequest(ctx, env.EnvoyURL, body, headers)
		cancel()

		total++
		if r.Err != nil || r.StatusCode != 200 || !r.Complete {
			transportErrors++
		} else {
			if !r.RequestBodyMatch {
				reqIntegrity++
			}
			if r.ResponseHash != r.ExpectedHash {
				respIntegrity++
			}
		}

		if total%100 == 0 {
			elapsed := time.Since(deadline.Add(-duration))
			var eosDups int64
			for _, p := range env.Procs {
				eosDups += p.RequestEosDups()
			}
			t.Logf("progress: %d in %v | payload=%s transport=%d reqInteg=%d respInteg=%d eosDups=%d",
				total, elapsed.Round(time.Second), pc.name, transportErrors, reqIntegrity, respIntegrity, eosDups)
		}

		time.Sleep(100 * time.Millisecond)
	}

	var eosDups int64
	for _, p := range env.Procs {
		eosDups += p.RequestEosDups()
	}
	t.Logf("FINAL: depth=%d %d requests in %v | transport=%d reqInteg=%d respInteg=%d eosDups=%d",
		depth, total, duration, transportErrors, reqIntegrity, respIntegrity, eosDups)

	assert.Equal(t, 0, transportErrors, "transport errors")
	assert.Equal(t, 0, reqIntegrity, "request body integrity errors")
	assert.Equal(t, 0, respIntegrity, "response body integrity errors")
}

func makeInferenceBody(model string, targetSize int) []byte {
	envelope := fmt.Sprintf(`{"model":"%s","messages":[{"role":"user","content":"`, model)
	suffix := `"}]}`
	paddingSize := targetSize - len(envelope) - len(suffix)
	if paddingSize < 0 {
		paddingSize = 0
	}
	return []byte(envelope + strings.Repeat("x", paddingSize) + suffix)
}
