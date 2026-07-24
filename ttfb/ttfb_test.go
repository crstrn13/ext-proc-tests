package ttfb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/crstrn13/ext-proc-tests"
)

func TestTTFB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("STREAMED_immediate_TTFB", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "STREAMED"
		})

		headers := map[string]string{
			"x-hold-seconds":  "5",
			"x-chunk-interval": "1",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d ttfb=%v total=%v complete=%v",
			result.StatusCode, result.TTFB, result.TotalTime, result.Complete)

		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
		assert.Less(t, result.TTFB, 3*time.Second, "STREAMED TTFB should be near-instant")
	})

	t.Run("BUFFERED_delayed_TTFB", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "BUFFERED"
		})

		headers := map[string]string{
			"x-hold-seconds":      "5",
			"x-chunk-interval-ms": "100",
			"x-chunk-size":        "10240",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d ttfb=%v total=%v complete=%v",
			result.StatusCode, result.TTFB, result.TotalTime, result.Complete)

		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
		assert.Greater(t, result.TTFB, 4*time.Second, "BUFFERED TTFB should be >= backend duration")
	})
}
