package memory_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/crstrn13/ext-proc-tests"
)

func TestMemoryStreamed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	env := SetupEnv(t, func(cfg *EnvoyConfig) {
		cfg.ResponseBodyMode = "STREAMED"
	})

	tests := []struct {
		name             string
		holdSeconds      int
		chunkIntervalMs  int
		chunkSize        int
		procDelayMs      int
		maxMemoryDeltaMB float64
	}{
		{
			name:             "M1_baseline_no_delay",
			holdSeconds:      10,
			chunkIntervalMs:  10,
			chunkSize:        10240,
			procDelayMs:      0,
			maxMemoryDeltaMB: 5.0,
		},
		{
			name:             "M2_backpressure",
			holdSeconds:      10,
			chunkIntervalMs:  10,
			chunkSize:        10240,
			procDelayMs:      100,
			maxMemoryDeltaMB: 10.0,
		},
		{
			name:             "M3_large_chunks_coalescing",
			holdSeconds:      5,
			chunkIntervalMs:  50,
			chunkSize:        102400,
			procDelayMs:      500,
			maxMemoryDeltaMB: 10.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{
				"x-hold-seconds":      strconv.Itoa(tc.holdSeconds),
				"x-chunk-interval-ms": strconv.Itoa(tc.chunkIntervalMs),
				"x-chunk-size":        strconv.Itoa(tc.chunkSize),
			}
			if tc.procDelayMs > 0 {
				headers["x-proc-delay-ms"] = strconv.Itoa(tc.procDelayMs)
			}

			var result StreamResult
			baseline, peak := SampleMemoryDuring(t, env.AdminURL, time.Second, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
				defer cancel()
				result = DoStreamingRequest(ctx, env.EnvoyURL, headers)
			})

			deltaMB := float64(peak-baseline) / (1024 * 1024)
			t.Logf("baseline=%d peak=%d delta=%.1f MB", baseline, peak, deltaMB)
			t.Logf("status=%d chunks=%d ttfb=%v total=%v complete=%v err=%v",
				result.StatusCode, result.Chunks, result.TTFB, result.TotalTime, result.Complete, result.Err)

			assert.Equal(t, 200, result.StatusCode)
			assert.True(t, result.Complete, "expected data: [DONE]")
			assert.LessOrEqual(t, deltaMB, tc.maxMemoryDeltaMB,
				"memory delta %.1f MB exceeded max %.1f MB", deltaMB, tc.maxMemoryDeltaMB)
		})
	}
}

func TestMemoryBuffered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	env := SetupEnv(t, func(cfg *EnvoyConfig) {
		cfg.ResponseBodyMode = "BUFFERED"
	})

	t.Run("M4_exceeds_buffer_limit", func(t *testing.T) {
		headers := map[string]string{
			"x-hold-seconds":      "10",
			"x-chunk-interval-ms": "10",
			"x-chunk-size":        "10240",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d total=%v err=%v", result.StatusCode, result.Chunks, result.TotalTime, result.Err)
		assert.Equal(t, 500, result.StatusCode, "expected 500 from buffer overflow")
	})

	t.Run("M5_under_buffer_limit", func(t *testing.T) {
		headers := map[string]string{
			"x-hold-seconds":      "5",
			"x-chunk-interval-ms": "100",
			"x-chunk-size":        "10240",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d ttfb=%v total=%v complete=%v",
			result.StatusCode, result.Chunks, result.TTFB, result.TotalTime, result.Complete)
		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
	})

	t.Run("M6_under_limit_slow_extproc", func(t *testing.T) {
		headers := map[string]string{
			"x-hold-seconds":      "5",
			"x-chunk-interval-ms": "100",
			"x-chunk-size":        "10240",
			"x-proc-delay-ms":     "500",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d ttfb=%v total=%v complete=%v",
			result.StatusCode, result.Chunks, result.TTFB, result.TotalTime, result.Complete)
		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
	})
}
