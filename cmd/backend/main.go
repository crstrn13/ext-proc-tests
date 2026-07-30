// Standalone backend HTTP server for manual testing.
// Exposes /inference (POST, SSE) and /echo (POST, echo body).
//
// Usage:
//
//	go run ./cmd/backend --port 8080
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"net/http"

	envoytest "github.com/crstrn13/ext-proc-tests"
)

func main() {
	port := flag.String("port", "8080", "HTTP listen port")
	flag.Parse()

	logHandler := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil && r.Body != http.NoBody {
				body, err := io.ReadAll(r.Body)
				r.Body.Close()
				if err != nil {
					log.Printf("[backend] %s %s body read error: %v", r.Method, r.URL.Path, err)
				} else {
					hash := sha256.Sum256(body)
					log.Printf("[backend] %s %s received %d bytes sha256=%s",
						r.Method, r.URL.Path, len(body), hex.EncodeToString(hash[:]))
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
			} else {
				log.Printf("[backend] %s %s (no body)", r.Method, r.URL.Path)
			}
			h(w, r)
			log.Printf("[backend] %s %s done", r.Method, r.URL.Path)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/inference", logHandler(envoytest.InferenceHandler))
	mux.HandleFunc("/echo", logHandler(envoytest.EchoHandler))
	mux.HandleFunc("/", logHandler(envoytest.BackendHandler))

	log.Printf("[backend] listening on :%s", *port)
	log.Printf("[backend]   POST /inference  — reads body, SHA256s it, streams SSE tokens")
	log.Printf("[backend]   POST /echo       — echoes request body verbatim")
	log.Printf("[backend]   GET  /           — streams SSE tokens (no request body)")
	if err := http.ListenAndServe(":"+*port, mux); err != nil {
		log.Fatalf("[backend] serve error: %v", err)
	}
}
