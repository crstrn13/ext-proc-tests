// Standalone ext-proc gRPC server for manual testing.
// Runs a single FULL_DUPLEX_STREAMED passthrough filter with verbose logging.
//
// Usage:
//
//	go run ./cmd/extproc --port 50051 --name filter-0
//	go run ./cmd/extproc --port 50052 --name filter-1
package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	envoytest "github.com/crstrn13/ext-proc-tests"
	ext_proc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50051, "gRPC listen port")
	name := flag.String("name", "filter", "filter name shown in logs")
	logLevel := flag.String("log-level", "info", "log level: error, info, debug")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("[%s] failed to listen: %v", *name, err)
	}

	var level envoytest.LogLevel
	switch *logLevel {
	case "debug":
		level = envoytest.LogLevelDebug
	case "info":
		level = envoytest.LogLevelInfo
	default:
		level = envoytest.LogLevelError
	}

	srv := &envoytest.ExtProcServer{}
	srv.AlwaysStreamRequestBody = true
	srv.AlwaysStreamResponseBody = true
	srv.LogLevel = level
	srv.Name = *name

	grpcSrv := grpc.NewServer()
	ext_proc.RegisterExternalProcessorServer(grpcSrv, srv)

	log.Printf("[%s] listening on :%d (FULL_DUPLEX_STREAMED, req+resp body)", *name, *port)
	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("[%s] serve error: %v", *name, err)
	}
}
