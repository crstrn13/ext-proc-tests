package fullduplexstreamed

import (
	"testing"

	envoytest "github.com/crstrn13/ext-proc-tests"
)

type ChainedEnv struct {
	envoytest.TestEnv
	Procs []*envoytest.ExtProcServer
}

func SetupChainedEnv(t *testing.T, configFn ...func(*envoytest.EnvoyConfig)) ChainedEnv {
	return SetupChainedEnvN(t, 2, configFn...)
}

func SetupChainedEnvN(t *testing.T, n int, configFn ...func(*envoytest.EnvoyConfig)) ChainedEnv {
	t.Helper()

	procs := make([]*envoytest.ExtProcServer, n)
	filters := make([]envoytest.ExtProcFilterConfig, n)

	for i := range n {
		proc, port := envoytest.StartExtProcServer(t)
		proc.AlwaysStreamRequestBody = true
		proc.AlwaysStreamResponseBody = true
		procs[i] = proc
		filters[i] = envoytest.ExtProcFilterConfig{
			Port:             port,
			RequestBodyMode:  "FULL_DUPLEX_STREAMED",
			ResponseBodyMode: "FULL_DUPLEX_STREAMED",
		}
	}

	env := envoytest.SetupEnv(t, func(cfg *envoytest.EnvoyConfig) {
		cfg.Filters = filters
		for _, fn := range configFn {
			fn(cfg)
		}
	})

	return ChainedEnv{TestEnv: env, Procs: procs}
}
