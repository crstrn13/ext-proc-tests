package envoytest

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"net/http"
	"strconv"
	"text/template"
)

//go:embed envoy.yaml.tmpl
var envoyConfigTemplate string

type EnvoyConfig struct {
	ListenerPort                  int
	PerConnectionBufferLimitBytes int
	StreamIdleTimeoutSec          int
	RouteTimeoutSec               int
	MessageTimeoutSec             int
	ResponseBodyMode              string
	RequestBodyMode               string
	ExtProcAddr                   string
	ExtProcPort                   int
	BackendAddr                   string
	BackendPort                   int
	AdminPort                     int
}

func defaultEnvoyConfig() EnvoyConfig {
	return EnvoyConfig{
		ListenerPort:         10000,
		StreamIdleTimeoutSec: 300,
		RouteTimeoutSec:      0,
		MessageTimeoutSec:    10,
		ResponseBodyMode:     "STREAMED",
		RequestBodyMode:      "BUFFERED",
		ExtProcAddr:          "host.testcontainers.internal",
		ExtProcPort:          50051,
		BackendAddr:          "host.testcontainers.internal",
		BackendPort:          3000,
		AdminPort:            9901,
	}
}

func renderEnvoyConfig(cfg EnvoyConfig) ([]byte, error) {
	tmpl, err := template.New("envoy").Parse(envoyConfigTemplate)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, cfg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GetEnvoyMemoryBytes(adminURL string) (int64, error) {
	resp, err := http.Get(adminURL + "/memory")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	return strconv.ParseInt(result["allocated"], 10, 64)
}
