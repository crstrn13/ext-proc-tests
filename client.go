package envoytest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

type StreamResult struct {
	StatusCode     int
	TTFB           time.Duration
	TotalTime      time.Duration
	Chunks         int
	Complete       bool
	BodySize       int64
	Err            error
	ReceivedHash   string
	ExpectedHash   string
}

func DoStreamingRequest(ctx context.Context, envoyURL string, headers map[string]string) StreamResult {
	req, err := http.NewRequestWithContext(ctx, "GET", envoyURL, nil)
	if err != nil {
		return StreamResult{Err: err}
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	start := time.Now()
	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return StreamResult{Err: err, TotalTime: time.Since(start)}
	}
	defer resp.Body.Close()

	result := StreamResult{StatusCode: resp.StatusCode}
	firstByte := false
	hasher := sha256.New()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !firstByte {
			result.TTFB = time.Since(start)
			firstByte = true
		}
		result.BodySize += int64(len(line)) + 1
		if strings.HasPrefix(line, "data: ") {
			if line == "data: [DONE]" {
				result.Complete = true
			} else if strings.Contains(line, "\"sha256\":") {
				var msg struct{ Sha256 string }
				if err := json.Unmarshal([]byte(line[6:]), &msg); err == nil {
					result.ExpectedHash = msg.Sha256
				}
			} else {
				result.Chunks++
				hasher.Write([]byte(line + "\n\n"))
			}
		}
	}
	if err := scanner.Err(); err != nil {
		result.Err = err
	}

	result.ReceivedHash = hex.EncodeToString(hasher.Sum(nil))
	result.TotalTime = time.Since(start)
	return result
}

type InferenceResult struct {
	StatusCode       int
	TTFB             time.Duration
	Chunks           int
	Complete         bool
	Err              error
	RequestBodyMatch bool
	ResponseHash     string
	ExpectedHash     string
	Model            string
}

func DoInferenceRequest(ctx context.Context, envoyURL string, body []byte, headers map[string]string) InferenceResult {
	req, err := http.NewRequestWithContext(ctx, "POST", envoyURL+"/inference", bytes.NewReader(body))
	if err != nil {
		return InferenceResult{Err: err}
	}
	bodyHash := hex.EncodeToString(sha256Sum(body))
	req.Header.Set("x-expected-body-hash", bodyHash)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	start := time.Now()
	resp, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		return InferenceResult{Err: err}
	}
	defer resp.Body.Close()

	result := InferenceResult{StatusCode: resp.StatusCode}
	firstByte := false
	hasher := sha256.New()
	expectedBodyHash := bodyHash

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !firstByte {
			result.TTFB = time.Since(start)
			firstByte = true
		}
		if strings.HasPrefix(line, "data: ") {
			if line == "data: [DONE]" {
				result.Complete = true
			} else if strings.Contains(line, "\"request_body_sha256\":") {
				var msg struct {
					RequestBodySha256 string `json:"request_body_sha256"`
					Model             string `json:"model"`
				}
				if err := json.Unmarshal([]byte(line[6:]), &msg); err == nil {
					result.RequestBodyMatch = msg.RequestBodySha256 == expectedBodyHash
					result.Model = msg.Model
				}
			} else if strings.Contains(line, "\"sha256\":") {
				var msg struct{ Sha256 string }
				if err := json.Unmarshal([]byte(line[6:]), &msg); err == nil {
					result.ExpectedHash = msg.Sha256
				}
			} else {
				result.Chunks++
				hasher.Write([]byte(line + "\n\n"))
			}
		}
	}
	if err := scanner.Err(); err != nil {
		result.Err = err
	}

	result.ResponseHash = hex.EncodeToString(hasher.Sum(nil))
	return result
}

func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

type BulkResult struct {
	StatusCode   int
	SentSize     int
	ReceivedSize int
	Match        bool
	Err          error
	Body         string
}

type chunkedReader struct {
	r         io.Reader
	chunkSize int
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if len(p) > c.chunkSize {
		p = p[:c.chunkSize]
	}
	return c.r.Read(p)
}

func DoBulkRequestChunked(ctx context.Context, envoyURL string, body []byte, chunkSize int) BulkResult {
	req, err := http.NewRequestWithContext(ctx, "POST", envoyURL+"/echo",
		&chunkedReader{r: bytes.NewReader(body), chunkSize: chunkSize})
	if err != nil {
		return BulkResult{Err: err}
	}
	req.ContentLength = -1

	resp, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		return BulkResult{Err: err}
	}
	defer resp.Body.Close()

	received, err := io.ReadAll(resp.Body)
	if err != nil {
		return BulkResult{StatusCode: resp.StatusCode, SentSize: len(body), Err: err}
	}
	r := BulkResult{
		StatusCode:   resp.StatusCode,
		SentSize:     len(body),
		ReceivedSize: len(received),
		Match:        bytes.Equal(body, received),
	}
	if resp.StatusCode >= 400 && len(received) < 4096 {
		r.Body = string(received)
	}
	return r
}

func DoBulkRequest(ctx context.Context, envoyURL string, body []byte) BulkResult {
	req, err := http.NewRequestWithContext(ctx, "POST", envoyURL+"/echo", bytes.NewReader(body))
	if err != nil {
		return BulkResult{Err: err}
	}

	resp, err := (&http.Client{Timeout: 0}).Do(req)
	if err != nil {
		return BulkResult{Err: err}
	}
	defer resp.Body.Close()

	received, err := io.ReadAll(resp.Body)
	if err != nil {
		return BulkResult{StatusCode: resp.StatusCode, SentSize: len(body), Err: err}
	}
	r := BulkResult{
		StatusCode:   resp.StatusCode,
		SentSize:     len(body),
		ReceivedSize: len(received),
		Match:        bytes.Equal(body, received),
	}
	if resp.StatusCode >= 400 && len(received) < 4096 {
		r.Body = string(received)
	}
	return r
}
