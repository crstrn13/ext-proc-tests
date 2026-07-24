package concurrency_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/crstrn13/ext-proc-tests"
)

func TestConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	tests := []struct {
		name        string
		concurrency int
		holdSeconds int
		maxMemoryMB float64
	}{
		{name: "C10", concurrency: 10, holdSeconds: 30, maxMemoryMB: 30},
		{name: "C50", concurrency: 50, holdSeconds: 30, maxMemoryMB: 50},
		{name: "C100", concurrency: 100, holdSeconds: 30, maxMemoryMB: 80},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := SetupEnv(t, func(cfg *EnvoyConfig) {
				cfg.ResponseBodyMode = "STREAMED"
			})

			var (
				wg        sync.WaitGroup
				succeeded atomic.Int64
				failed    atomic.Int64
			)

			done := make(chan struct{})
			var peakMem int64

			go func() {
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						if mem, err := GetEnvoyMemoryBytes(env.AdminURL); err == nil {
							for {
								cur := atomic.LoadInt64(&peakMem)
								if mem <= cur || atomic.CompareAndSwapInt64(&peakMem, cur, mem) {
									break
								}
							}
						}
					}
				}
			}()

			for i := 0; i < tc.concurrency; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					headers := map[string]string{
						"x-hold-seconds":  strconv.Itoa(tc.holdSeconds),
						"x-chunk-interval": "1",
					}

					ctx, cancel := context.WithTimeout(context.Background(),
						time.Duration(tc.holdSeconds+60)*time.Second)
					defer cancel()

					result := DoStreamingRequest(ctx, env.EnvoyURL, headers)
					if result.StatusCode == 200 && result.Complete && result.Err == nil {
						succeeded.Add(1)
					} else {
						failed.Add(1)
						t.Logf("[conn %d] failed: status=%d complete=%v err=%v",
							id, result.StatusCode, result.Complete, result.Err)
					}
				}(i)
				time.Sleep(5 * time.Millisecond)
			}

			wg.Wait()
			close(done)

			peakMB := float64(atomic.LoadInt64(&peakMem)) / (1024 * 1024)
			t.Logf("concurrency=%d succeeded=%d failed=%d peakMemory=%.1f MB",
				tc.concurrency, succeeded.Load(), failed.Load(), peakMB)

			assert.Equal(t, int64(tc.concurrency), succeeded.Load()+failed.Load(),
				"not all goroutines completed")
			assert.Equal(t, int64(0), failed.Load(), "expected zero failures")

			if tc.maxMemoryMB > 0 && peakMB > 0 {
				assert.LessOrEqual(t, peakMB, tc.maxMemoryMB,
					"peak memory %.1f MB exceeded max %.1f MB", peakMB, tc.maxMemoryMB)
			}
		})
	}
}
