// Streaming integrity tests: GET / with SSE response only (no request body).
// Tests response body integrity via checksum. Also covers concurrency and disconnect recovery.
//
//	CLIENT  --GET /-->  ENVOY  --N chained FD_STREAMED ext-proc-->  BACKEND
//	        <--SSE---          <------StreamedBodyResponse------
package fullduplexstreamed_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/crstrn13/ext-proc-tests"
	fds "github.com/crstrn13/ext-proc-tests/full_duplex_streamed"
)

func TestStreamingIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, depth := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			t.Parallel()
			env := fds.SetupChainedEnvN(t, depth)

			// Single-shot streaming — one request per sub-test
			singleTests := []struct {
				name       string
				chunks     int
				chunkSize  int
				intervalMs int
				holdSecs   int
			}{
				{"single_1KB_chunks_x1000", 1000, 1024, 10, 10},
				{"single_100KB_chunks_x200", 200, 100 * 1024, 50, 10},
				{"single_512B_burst_x2000", 2000, 512, 1, 2},
				{"single_512KB_chunks_x100", 100, 512 * 1024, 100, 10},
			}
			for _, tc := range singleTests {
				t.Run(tc.name, func(t *testing.T) {
					headers := map[string]string{
						"x-hold-seconds":      strconv.Itoa(tc.holdSecs),
						"x-chunk-interval-ms": strconv.Itoa(tc.intervalMs),
						"x-chunk-size":        strconv.Itoa(tc.chunkSize),
						"x-checksum":          "true",
						"x-full-duplex":       "true",
					}
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tc.holdSecs+30)*time.Second)
					defer cancel()

					r := DoStreamingRequest(ctx, env.EnvoyURL, headers)

					t.Logf("depth=%d %s status=%d complete=%v chunks=%d hashMatch=%v",
						depth, tc.name, r.StatusCode, r.Complete, r.Chunks,
						r.ExpectedHash == r.ReceivedHash)

					require.Equal(t, 200, r.StatusCode)
					require.True(t, r.Complete, "stream did not complete")
					require.NoError(t, r.Err)
					assert.Equal(t, tc.chunks, r.Chunks, "chunk count mismatch")
					assert.Equal(t, r.ExpectedHash, r.ReceivedHash, "response body integrity failed")
				})
			}

			// Rapid-fire streaming — sequential back-to-back
			rapidTests := []struct {
				name       string
				chunkSize  int
				intervalMs int
				holdSecs   int
				iterations int
			}{
				{"rapid_1KB", 1024, 10, 1, 50},
				{"rapid_100KB", 100 * 1024, 10, 1, 30},
			}
			for _, tc := range rapidTests {
				t.Run(tc.name, func(t *testing.T) {
					headers := map[string]string{
						"x-hold-seconds":      strconv.Itoa(tc.holdSecs),
						"x-chunk-interval-ms": strconv.Itoa(tc.intervalMs),
						"x-chunk-size":        strconv.Itoa(tc.chunkSize),
						"x-checksum":          "true",
						"x-full-duplex":       "true",
					}
					var transport, integrity int
					for range tc.iterations {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tc.holdSecs+30)*time.Second)
						r := DoStreamingRequest(ctx, env.EnvoyURL, headers)
						cancel()
						if r.Err != nil || r.StatusCode != 200 || !r.Complete {
							transport++
						} else if r.ExpectedHash != r.ReceivedHash {
							integrity++
						}
					}
					t.Logf("depth=%d %s iters=%d transport=%d integrity=%d",
						depth, tc.name, tc.iterations, transport, integrity)
					assert.Equal(t, 0, transport, "transport errors")
					assert.Equal(t, 0, integrity, "response body integrity errors")
				})
			}
		})
	}
}

func TestStreamingConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	env := fds.SetupChainedEnv(t)

	const (
		concurrency = 50
		holdSecs    = 10
		intervalMs  = 100
		chunkSize   = 1024
	)

	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		succeeded       int64
		failed          int64
		integrityFailed int64
		failures        []string
	)

	for i := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			headers := map[string]string{
				"x-hold-seconds":      strconv.Itoa(holdSecs),
				"x-chunk-interval-ms": strconv.Itoa(intervalMs),
				"x-chunk-size":        strconv.Itoa(chunkSize),
				"x-checksum":          "true",
				"x-full-duplex":       "true",
			}

			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(holdSecs+30)*time.Second)
			defer cancel()

			result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

			mu.Lock()
			defer mu.Unlock()
			if result.StatusCode != 200 || !result.Complete || result.Err != nil {
				failed++
				failures = append(failures, fmt.Sprintf("%d: status=%d", id, result.StatusCode))
			} else if result.ExpectedHash != result.ReceivedHash {
				integrityFailed++
				failures = append(failures, fmt.Sprintf("%d: hash mismatch", id))
			} else {
				succeeded++
			}
		}(i)
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()

	t.Logf("concurrency=%d succeeded=%d failed=%d integrityFailed=%d",
		concurrency, succeeded, failed, integrityFailed)
	for _, f := range failures {
		t.Logf("  failure: %s", f)
	}

	assert.Equal(t, int64(0), failed, "transport failures")
	assert.Equal(t, int64(0), integrityFailed, "integrity failures")
	assert.Equal(t, int64(concurrency), succeeded)
}

func TestStreamingDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	env := fds.SetupChainedEnv(t)

	const rounds = 10

	for i := range rounds {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result := DoStreamingRequest(ctx, env.EnvoyURL, map[string]string{
			"x-hold-seconds":      "30",
			"x-chunk-interval-ms": "50",
			"x-chunk-size":        "1024",
			"x-full-duplex":       "true",
		})
		cancel()

		require.Greater(t, result.Chunks, 0, "round %d: no chunks before disconnect", i)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
		recovery := DoStreamingRequest(ctx2, env.EnvoyURL, map[string]string{
			"x-hold-seconds":      "3",
			"x-chunk-interval-ms": "50",
			"x-chunk-size":        "1024",
			"x-checksum":          "true",
			"x-full-duplex":       "true",
		})
		cancel2()

		require.Equal(t, 200, recovery.StatusCode, "round %d: recovery failed", i)
		require.True(t, recovery.Complete, "round %d: recovery stream incomplete", i)
		assert.Equal(t, recovery.ExpectedHash, recovery.ReceivedHash,
			"round %d: recovery integrity failed", i)
	}

	t.Logf("disconnect/recovery rounds=%d all passed", rounds)
}
