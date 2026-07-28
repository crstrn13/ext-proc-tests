package fullduplexstreamed

import (
	"io"
	"log"
	"net"
	"sync/atomic"
	"testing"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	ext_proc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type Server struct {
	ext_proc.UnimplementedExternalProcessorServer
	activeStreams atomic.Int64
	totalStreams  atomic.Int64
	bytesSeen    atomic.Int64
}

func (s *Server) BytesSeen() int64 {
	return s.bytesSeen.Load()
}

func StartServer(t *testing.T) (*Server, int) {
	t.Helper()
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port

	srv := &Server{}
	s := grpc.NewServer()
	ext_proc.RegisterExternalProcessorServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Printf("full-duplex ext-proc server stopped: %v", err)
		}
	}()
	t.Cleanup(s.GracefulStop)

	return srv, port
}

func headerValue(headers []*core.HeaderValue, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.RawValue)
		}
	}
	return ""
}

func (s *Server) Process(stream ext_proc.ExternalProcessor_ProcessServer) error {
	s.totalStreams.Add(1)
	s.activeStreams.Add(1)
	defer s.activeStreams.Add(-1)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch v := req.Request.(type) {
		case *ext_proc.ProcessingRequest_RequestHeaders:
			if err := stream.Send(&ext_proc.ProcessingResponse{
				Response: &ext_proc.ProcessingResponse_RequestHeaders{
					RequestHeaders: &ext_proc.HeadersResponse{
						Response: &ext_proc.CommonResponse{Status: ext_proc.CommonResponse_CONTINUE},
					},
				},
			}); err != nil {
				return err
			}

		case *ext_proc.ProcessingRequest_RequestBody:
			if err := stream.Send(&ext_proc.ProcessingResponse{
				Response: &ext_proc.ProcessingResponse_RequestBody{
					RequestBody: &ext_proc.BodyResponse{
						Response: &ext_proc.CommonResponse{Status: ext_proc.CommonResponse_CONTINUE},
					},
				},
			}); err != nil {
				return err
			}

		case *ext_proc.ProcessingRequest_ResponseHeaders:
			if err := stream.Send(&ext_proc.ProcessingResponse{
				Response: &ext_proc.ProcessingResponse_ResponseHeaders{
					ResponseHeaders: &ext_proc.HeadersResponse{
						Response: &ext_proc.CommonResponse{Status: ext_proc.CommonResponse_CONTINUE},
					},
				},
			}); err != nil {
				return err
			}

		case *ext_proc.ProcessingRequest_ResponseBody:
			s.bytesSeen.Add(int64(len(v.ResponseBody.Body)))
			if err := stream.Send(&ext_proc.ProcessingResponse{
				Response: &ext_proc.ProcessingResponse_ResponseBody{
					ResponseBody: &ext_proc.BodyResponse{
						Response: &ext_proc.CommonResponse{
							Status: ext_proc.CommonResponse_CONTINUE,
							BodyMutation: &ext_proc.BodyMutation{
								Mutation: &ext_proc.BodyMutation_StreamedResponse{
									StreamedResponse: &ext_proc.StreamedBodyResponse{
										Body:        v.ResponseBody.Body,
										EndOfStream: v.ResponseBody.EndOfStream,
									},
								},
							},
						},
					},
				},
			}); err != nil {
				return err
			}

		case *ext_proc.ProcessingRequest_ResponseTrailers:
			if err := stream.Send(&ext_proc.ProcessingResponse{
				Response: &ext_proc.ProcessingResponse_ResponseTrailers{
					ResponseTrailers: &ext_proc.TrailersResponse{},
				},
			}); err != nil {
				return err
			}

		default:
			log.Printf("[full-duplex ext-proc] unhandled: %T", req.Request)
		}
	}
}
