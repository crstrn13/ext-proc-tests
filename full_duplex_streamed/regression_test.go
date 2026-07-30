// Regression test: confirms chained FD_STREAMED body truncation on older Envoy versions
// and verifies the fix on v1.37+. Uses bulk /echo endpoint because the original bug
// (envoy#44605) was found with simple body echo.
package fullduplexstreamed_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/crstrn13/ext-proc-tests"
)

func TestChainIntegrityRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	versions := []struct {
		image    string
		expectOK bool
	}{
		{"envoyproxy/envoy:v1.35.4", false},
		{"envoyproxy/envoy:v1.37-latest", true},
	}

	payload := []byte(strings.Repeat("x", 50*1024))

	for _, ver := range versions {
		t.Run(ver.image, func(t *testing.T) {
			env := SetupEnvWithExtProcDelay(t, 1000, func(cfg *EnvoyConfig) {
				cfg.EnvoyImage = ver.image
				cfg.Filters = []ExtProcFilterConfig{
					{
						Port:              cfg.ExtProcPort,
						RequestHeaderMode: "SKIP",
						RequestBodyMode:   "NONE",
						ResponseBodyMode:  "NONE",
						AllowModeOverride: true,
					},
					{
						Port:              cfg.ExtProcPort,
						RequestHeaderMode: "SKIP",
						RequestBodyMode:   "NONE",
						ResponseBodyMode:  "FULL_DUPLEX_STREAMED",
						AllowModeOverride: true,
					},
				}
			})

			const iterations = 10
			var truncated, errored int

			for i := range iterations {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				result := DoBulkRequest(ctx, env.EnvoyURL, payload)
				cancel()
				if result.Err != nil {
					errored++
					t.Logf("ERROR iter=%d envoy=%s status=%d: %v",
						i, ver.image, result.StatusCode, result.Err)
				} else if !result.Match {
					truncated++
					t.Logf("TRUNCATION iter=%d envoy=%s sent=%d received=%d",
						i, ver.image, result.SentSize, result.ReceivedSize)
				}
			}

			t.Logf("envoy=%s iterations=%d truncated=%d errored=%d",
				ver.image, iterations, truncated, errored)

			if ver.expectOK {
				assert.Equal(t, 0, truncated, "truncation on fixed version")
				assert.Equal(t, 0, errored, "errors on fixed version")
			} else {
				assert.Greater(t, truncated+errored, 0,
					"expected truncation on buggy version (envoy#44605)")
			}
		})
	}
}
