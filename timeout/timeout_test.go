package timeout_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/crstrn13/ext-proc-tests"
)

func TestTimeoutStreamed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("T1_message_timeout_kills_slow_extproc", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "STREAMED"
			cfg.MessageTimeoutSec = 1
		})

		headers := map[string]string{
			"x-hold-seconds":  "30",
			"x-chunk-interval": "1",
			"x-chunk-size":    "100",
			"x-proc-delay-ms": "5000",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 200, result.StatusCode)
		assert.False(t, result.Complete, "stream should have been reset before completion")
		assert.Less(t, result.Chunks, 30, "should not receive all 30 chunks")
	})

	t.Run("T2_message_timeout_spares_fast_extproc", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "STREAMED"
			cfg.MessageTimeoutSec = 1
		})

		headers := map[string]string{
			"x-hold-seconds":  "5",
			"x-chunk-interval": "1",
			"x-proc-delay-ms": "500",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
		assert.NoError(t, result.Err)
	})

	t.Run("T3_stream_idle_timeout_fires", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "STREAMED"
			cfg.StreamIdleTimeoutSec = 3
		})

		headers := map[string]string{
			"x-hold-seconds":  "60",
			"x-chunk-interval": "10",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 200, result.StatusCode)
		assert.False(t, result.Complete, "stream should have been reset by idle timeout")
		assert.Less(t, result.TotalTime, 8*time.Second, "should die around 3s idle timeout")
	})

	t.Run("T4_stream_idle_resets_on_tokens", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "STREAMED"
			cfg.StreamIdleTimeoutSec = 5
		})

		headers := map[string]string{
			"x-hold-seconds":  "15",
			"x-chunk-interval": "1",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
		assert.NoError(t, result.Err)
		assert.Greater(t, result.TotalTime, 14*time.Second)
		assert.Less(t, result.TotalTime, 25*time.Second)
	})
}

func TestTimeoutBuffered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("T5_message_timeout_504", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "BUFFERED"
			cfg.MessageTimeoutSec = 1
		})

		headers := map[string]string{
			"x-hold-seconds":      "5",
			"x-chunk-interval-ms": "100",
			"x-chunk-size":        "10240",
			"x-proc-delay-ms":     "2000",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 504, result.StatusCode)
	})

	t.Run("T6_backend_time_not_covered_by_message_timeout", func(t *testing.T) {
		env := SetupEnv(t, func(cfg *EnvoyConfig) {
			cfg.ResponseBodyMode = "BUFFERED"
			cfg.MessageTimeoutSec = 1
		})

		headers := map[string]string{
			"x-hold-seconds":      "5",
			"x-chunk-interval-ms": "100",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result := DoStreamingRequest(ctx, env.EnvoyURL, headers)

		t.Logf("status=%d chunks=%d ttfb=%v total=%v complete=%v err=%v",
			result.StatusCode, result.Chunks, result.TTFB, result.TotalTime, result.Complete, result.Err)

		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.Complete)
		assert.Greater(t, result.TotalTime, 4*time.Second)
	})
}
