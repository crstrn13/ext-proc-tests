package envoytest

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ext_proc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
)

type TestEnv struct {
	EnvoyURL string
	AdminURL string
}

func StartExtProcServer(t *testing.T) (*ExtProcServer, int) {
	return startExtProc(t, &ExtProcServer{})
}

func startExtProc(t *testing.T, srv *ExtProcServer) (*ExtProcServer, int) {
	t.Helper()
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port

	s := grpc.NewServer()
	ext_proc.RegisterExternalProcessorServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Printf("ext-proc server stopped: %v", err)
		}
	}()
	t.Cleanup(s.GracefulStop)

	return srv, port
}


func StartBackend(t *testing.T) int {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", EchoHandler)
	mux.HandleFunc("/inference", InferenceHandler)
	mux.HandleFunc("/", BackendHandler)

	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port

	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Printf("backend stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})

	return port
}

func SetupEnvWithExtProcDelay(t *testing.T, bodyDelayMs int, configFn func(*EnvoyConfig)) TestEnv {
	t.Helper()
	_, extProcPort := startExtProc(t, &ExtProcServer{
		BodyDelayMs:      bodyDelayMs,
		AlwaysStreamBody: bodyDelayMs > 0,
	})
	return setupEnvoy(t, extProcPort, configFn)
}

func SetupEnv(t *testing.T, configFn func(*EnvoyConfig)) TestEnv {
	t.Helper()
	_, extProcPort := StartExtProcServer(t)
	return setupEnvoy(t, extProcPort, configFn)
}

func setupEnvoy(t *testing.T, extProcPort int, configFn func(*EnvoyConfig)) TestEnv {
	t.Helper()
	ctx := context.Background()

	backendPort := StartBackend(t)

	cfg := defaultEnvoyConfig()
	cfg.ExtProcPort = extProcPort
	cfg.BackendPort = backendPort
	if configFn != nil {
		configFn(&cfg)
	}

	seen := map[int]bool{extProcPort: true, backendPort: true}
	hostPorts := []int{extProcPort, backendPort}
	for _, f := range cfg.Filters {
		if f.Port > 0 && !seen[f.Port] {
			hostPorts = append(hostPorts, f.Port)
			seen[f.Port] = true
		}
	}
	for _, p := range cfg.ExtraHostPorts {
		if !seen[p] {
			hostPorts = append(hostPorts, p)
			seen[p] = true
		}
	}

	configBytes, err := renderEnvoyConfig(cfg)
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        cfg.EnvoyImage,
		ExposedPorts: []string{"10000/tcp", "9901/tcp"},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            bytes.NewReader(configBytes),
				ContainerFilePath: "/etc/envoy/envoy.yaml",
				FileMode:          0644,
			},
		},
		HostAccessPorts: hostPorts,
		WaitingFor: wait.ForAll(
			wait.ForHTTP("/ready").WithPort("9901/tcp"),
			wait.ForListeningPort("10000/tcp"),
		).WithDeadline(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate envoy: %v", err)
		}
	})

	envoyPort, err := container.MappedPort(ctx, "10000/tcp")
	require.NoError(t, err)
	adminPort, err := container.MappedPort(ctx, "9901/tcp")
	require.NoError(t, err)

	env := TestEnv{
		EnvoyURL: fmt.Sprintf("http://localhost:%d", envoyPort.Num()),
		AdminURL: fmt.Sprintf("http://localhost:%d", adminPort.Num()),
	}

	require.Eventually(t, func() bool {
		r, err := http.Get(env.AdminURL + "/ready")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	}, 15*time.Second, 500*time.Millisecond, "envoy did not become ready")

	return env
}

func SampleMemoryDuring(t *testing.T, adminURL string, interval time.Duration, fn func()) (baseline, peak int64) {
	t.Helper()
	baseline, err := GetEnvoyMemoryBytes(adminURL)
	require.NoError(t, err)
	peak = baseline

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if mem, err := GetEnvoyMemoryBytes(adminURL); err == nil && mem > peak {
					atomic.StoreInt64(&peak, mem)
				}
			}
		}
	}()

	fn()

	cancel()
	wg.Wait()
	if final, err := GetEnvoyMemoryBytes(adminURL); err == nil && final > peak {
		peak = final
	}
	return
}
