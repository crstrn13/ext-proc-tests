package longevity_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/crstrn13/ext-proc-tests"
)

func TestLongevity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("L1_STREAMED_5min", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "STREAMED"
			cfg.StreamIdleTimeoutSec = 300
		})

		headers := map[string]string{
			"x-hold-seconds":  "300",
			"x-chunk-interval": "1",
			"x-chunk-size":    "1024",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
		assert.NoError(t, result.Err)
		assert.InDelta(t, 300, result.Chunks, 5)
	})

	t.Run("L2_BUFFERED_2min_tiny_data", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "BUFFERED"
			cfg.StreamIdleTimeoutSec = 300
		})

		headers := map[string]string{
			"x-hold-seconds":  "120",
			"x-chunk-interval": "1",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d ttfb=%v total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TTFB, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
		assert.Greater(t, result.TTFB, 110*time.Second)
	})
}
