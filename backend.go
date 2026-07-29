package envoytest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func BackendHandler(w http.ResponseWriter, r *http.Request) {
	holdSecs := 30
	chunkInterval := 1
	chunkIntervalMs := 0
	chunkSize := 0

	if v := r.Header.Get("x-hold-seconds"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			holdSecs = p
		}
	}
	if v := r.Header.Get("x-chunk-interval"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			chunkInterval = p
		}
	}
	if v := r.Header.Get("x-chunk-interval-ms"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			chunkIntervalMs = p
		}
	}
	if v := r.Header.Get("x-chunk-size"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			chunkSize = p
		}
	}
	checksum := r.Header.Get("x-checksum") == "true"

	var interval time.Duration
	var totalChunks int
	if chunkIntervalMs > 0 {
		interval = time.Duration(chunkIntervalMs) * time.Millisecond
		totalChunks = (holdSecs * 1000) / chunkIntervalMs
	} else {
		interval = time.Duration(chunkInterval) * time.Second
		totalChunks = holdSecs / chunkInterval
	}
	if totalChunks < 1 {
		totalChunks = 1
	}

	payload := ""
	if chunkSize > 0 {
		payload = strings.Repeat("x", chunkSize)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	flusher.Flush()

	hasher := sha256.New()
	for i := 1; i <= totalChunks; i++ {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(interval):
		}
		var line string
		if chunkSize > 0 {
			line = fmt.Sprintf("data: {\"chunk\":%d,\"total\":%d,\"payload\":\"%s\"}\n\n", i, totalChunks, payload)
		} else {
			line = fmt.Sprintf("data: token_%d (chunk %d/%d)\n\n", i, i, totalChunks)
		}
		fmt.Fprint(w, line)
		hasher.Write([]byte(line))
		flusher.Flush()
	}

	if checksum {
		fmt.Fprintf(w, "data: {\"sha256\":\"%s\"}\n\n", hex.EncodeToString(hasher.Sum(nil)))
		flusher.Flush()
	}
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

func InferenceHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}
	bodyHash := sha256.Sum256(body)

	holdSecs := 10
	chunkIntervalMs := 100
	chunkSize := 1024

	if v := r.Header.Get("x-hold-seconds"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			holdSecs = p
		}
	}
	if v := r.Header.Get("x-chunk-interval-ms"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			chunkIntervalMs = p
		}
	}
	if v := r.Header.Get("x-chunk-size"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			chunkSize = p
		}
	}

	interval := time.Duration(chunkIntervalMs) * time.Millisecond
	totalChunks := (holdSecs * 1000) / chunkIntervalMs
	if totalChunks < 1 {
		totalChunks = 1
	}
	payload := strings.Repeat("x", chunkSize)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)

	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}

	modelName := r.Header.Get("x-model-name")
	fmt.Fprintf(w, "data: {\"request_body_sha256\":\"%s\",\"request_body_size\":%d,\"model\":\"%s\"}\n\n",
		hex.EncodeToString(bodyHash[:]), len(body), modelName)
	flusher.Flush()

	hasher := sha256.New()
	for i := 1; i <= totalChunks; i++ {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(interval):
		}
		line := fmt.Sprintf("data: {\"chunk\":%d,\"total\":%d,\"payload\":\"%s\"}\n\n", i, totalChunks, payload)
		fmt.Fprint(w, line)
		hasher.Write([]byte(line))
		flusher.Flush()
	}

	fmt.Fprintf(w, "data: {\"sha256\":\"%s\"}\n\n", hex.EncodeToString(hasher.Sum(nil)))
	flusher.Flush()
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

func EchoHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(200)
	w.Write(body)
}
