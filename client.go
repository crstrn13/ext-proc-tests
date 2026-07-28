package envoytest

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
