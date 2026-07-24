package envoytest

import (
	"fmt"
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

	for i := 1; i <= totalChunks; i++ {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(interval):
		}
		if chunkSize > 0 {
			fmt.Fprintf(w, "data: {\"chunk\":%d,\"total\":%d,\"payload\":\"%s\"}\n\n", i, totalChunks, payload)
		} else {
			fmt.Fprintf(w, "data: token_%d (chunk %d/%d)\n\n", i, i, totalChunks)
		}
		flusher.Flush()
	}

	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}
