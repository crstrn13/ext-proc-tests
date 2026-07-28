package envoytest

import (
	"io"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	ext_proc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

type extProcServer struct {
	ext_proc.UnimplementedExternalProcessorServer
	activeStreams atomic.Int64
	totalStreams  atomic.Int64
}

func headerValue(headers []*core.HeaderValue, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.RawValue)
		}
	}
	return ""
}

func (s *extProcServer) Process(stream ext_proc.ExternalProcessor_ProcessServer) error {
	id := s.totalStreams.Add(1)
	s.activeStreams.Add(1)
	defer s.activeStreams.Add(-1)

	var procDelayMs int
	var fullDuplex bool

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
			if v.RequestHeaders != nil && v.RequestHeaders.Headers != nil {
				if val := headerValue(v.RequestHeaders.Headers.Headers, "x-proc-delay-ms"); val != "" {
					if parsed, err := strconv.Atoi(val); err == nil {
						procDelayMs = parsed
					}
				}
				if headerValue(v.RequestHeaders.Headers.Headers, "x-full-duplex") == "true" {
					fullDuplex = true
				}
			}
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
			if procDelayMs > 0 {
				time.Sleep(time.Duration(procDelayMs) * time.Millisecond)
			}
			if fullDuplex {
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
			} else {
				if err := stream.Send(&ext_proc.ProcessingResponse{
					Response: &ext_proc.ProcessingResponse_ResponseBody{
						ResponseBody: &ext_proc.BodyResponse{
							Response: &ext_proc.CommonResponse{Status: ext_proc.CommonResponse_CONTINUE},
						},
					},
				}); err != nil {
					return err
				}
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
			log.Printf("[ext-proc stream %d] unhandled: %T", id, req.Request)
		}
	}
}

