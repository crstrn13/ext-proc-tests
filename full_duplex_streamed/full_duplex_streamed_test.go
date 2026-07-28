package fullduplexstreamed_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/crstrn13/ext-proc-tests"
	fds "github.com/crstrn13/ext-proc-tests/full_duplex_streamed"
)

func TestChainIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	tests := []struct {
		name       string
		chained    bool
		chunks     int
		chunkSize  int
		holdSecs   int
		intervalMs int
	}{
		{
			name:       "CH1_single_extproc",
			chained:    false,
			chunks:     1000,
			chunkSize:  1024,
			holdSecs:   10,
			intervalMs: 10,
		},
		{
			name:       "CH2_chained_extprocs",
			chained:    true,
			chunks:     1000,
			chunkSize:  1024,
			holdSecs:   10,
			intervalMs: 10,
		},
		{
			name:       "CH3_chained_large_chunks",
			chained:    true,
			chunks:     500,
			chunkSize:  102400,
			holdSecs:   25,
			intervalMs: 50,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setupFn := func(cfg *EnvoyConfig) {
				cfg.ResponseBodyMode = "FULL_DUPLEX_STREAMED"
			}

			if tc.chained {
				_, port2 := fds.StartServer(t)
				setupFn = func(cfg *EnvoyConfig) {
					cfg.ResponseBodyMode = "FULL_DUPLEX_STREAMED"
					cfg.ExtProc2Port = port2
					cfg.ExtProc2ResponseBodyMode = "FULL_DUPLEX_STREAMED"
					cfg.ExtraHostPorts = []int{port2}
				}
			}

			env := SetupEnv(t, setupFn)

			headers := map[string]string{
				"x-hold-seconds":      strconv.Itoa(tc.holdSecs),
				"x-chunk-interval-ms": strconv.Itoa(tc.intervalMs),
				"x-chunk-size":        strconv.Itoa(tc.chunkSize),
				"x-checksum":          "true",
				"x-full-duplex":       "true",
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tc.holdSecs+30)*time.Second)
			defer cancel()

			result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

			t.Logf("status=%d complete=%v chunks=%d bodySize=%d err=%v expectedHash=%s receivedHash=%s",
				result.StatusCode, result.Complete, result.Chunks, result.BodySize, result.Err,
				result.ExpectedHash, result.ReceivedHash)
			require.Equal(t, 200, result.StatusCode, "expected 200 OK")
			require.True(t, result.Complete, "stream did not complete")
			require.NoError(t, result.Err)

			assert.Equal(t, tc.chunks, result.Chunks, "chunk count mismatch")
			assert.NotEmpty(t, result.ExpectedHash, "backend did not send checksum")
			assert.NotEmpty(t, result.ReceivedHash, "client did not compute hash")
			assert.Equal(t, result.ExpectedHash, result.ReceivedHash,
				"body integrity check failed: data was corrupted through the ext-proc chain")

			t.Logf("chunks=%d bodySize=%d hashMatch=%v",
				result.Chunks, result.BodySize, result.ExpectedHash == result.ReceivedHash)
		})
	}
}

func TestChainConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	_, port2 := fds.StartServer(t)

	env := SetupEnv(t, func(cfg *EnvoyConfig) {
		cfg.ResponseBodyMode = "FULL_DUPLEX_STREAMED"
		cfg.ExtProc2Port = port2
		cfg.ExtProc2ResponseBodyMode = "FULL_DUPLEX_STREAMED"
		cfg.ExtraHostPorts = []int{port2}
	})

	const (
		concurrency = 50
		holdSecs    = 10
		intervalMs  = 100
		chunkSize   = 1024
	)

	var (
		wg              sync.WaitGroup
		succeeded       int64
		failed          int64
		integrityFailed int64
		mu              sync.Mutex
		failures        []string
	)

	for i := 0; i < concurrency; i++ {
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

			if result.StatusCode != 200 || !result.Complete || result.Err != nil {
				mu.Lock()
				failed++
				failures = append(failures, strconv.Itoa(id)+": status="+strconv.Itoa(result.StatusCode))
				mu.Unlock()
				return
			}

			if result.ExpectedHash != result.ReceivedHash {
				mu.Lock()
				integrityFailed++
				failures = append(failures, strconv.Itoa(id)+": hash mismatch")
				mu.Unlock()
				return
			}

			mu.Lock()
			succeeded++
			mu.Unlock()
		}(i)
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()

	t.Logf("concurrency=%d succeeded=%d failed=%d integrityFailed=%d",
		concurrency, succeeded, failed, integrityFailed)

	for _, f := range failures {
		t.Logf("  failure: %s", f)
	}

	assert.Equal(t, int64(0), failed, "expected zero transport failures")
	assert.Equal(t, int64(0), integrityFailed, "expected zero integrity failures")
	assert.Equal(t, int64(concurrency), succeeded, "not all streams succeeded")
}
