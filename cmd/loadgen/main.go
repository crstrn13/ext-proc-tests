// Quick load generator to reproduce envoy#44605 manually.
// Fires N concurrent inference requests and reports mismatches.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	envoytest "github.com/crstrn13/ext-proc-tests"
)

func main() {
	url := flag.String("url", "http://localhost:18000", "Envoy URL")
	size := flag.Int("size", 500*1024, "request body size in bytes")
	conc := flag.Int("conc", 50, "concurrent requests")
	flag.Parse()

	body := []byte(fmt.Sprintf(`{"model":"test","messages":[{"role":"user","content":"%s"}]}`,
		strings.Repeat("x", *size)))

	headers := map[string]string{
		"x-hold-seconds":      "1",
		"x-chunk-interval-ms": "10",
		"x-chunk-size":        "1024",
		"x-full-duplex":       "true",
	}

	log.Printf("Firing %d concurrent requests, body=%d bytes to %s", *conc, len(body), *url)

	var (
		wg                                     sync.WaitGroup
		mu                                     sync.Mutex
		ok, transport, reqCorrupt, respCorrupt int
	)

	for i := range *conc {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			r := envoytest.DoInferenceRequest(ctx, *url, body, headers)
			mu.Lock()
			defer mu.Unlock()
			if r.Err != nil || r.StatusCode != 200 || !r.Complete {
				transport++
				log.Printf("  [%d] TRANSPORT ERROR status=%d err=%v", id, r.StatusCode, r.Err)
			} else {
				if !r.RequestBodyMatch {
					reqCorrupt++
					log.Printf("  [%d] REQUEST BODY MISMATCH", id)
				}
				if r.ResponseHash != r.ExpectedHash {
					respCorrupt++
					log.Printf("  [%d] RESPONSE BODY MISMATCH", id)
				}
				if r.RequestBodyMatch && r.ResponseHash == r.ExpectedHash {
					ok++
				}
			}
		}(i)
	}
	wg.Wait()

	log.Printf("RESULTS: ok=%d transport=%d reqCorrupt=%d respCorrupt=%d", ok, transport, reqCorrupt, respCorrupt)
	if reqCorrupt > 0 {
		log.Printf(">>> envoy#44605 CONFIRMED: %d/%d requests had their body silently dropped", reqCorrupt, *conc)
	} else {
		log.Printf(">>> No corruption detected this run (try larger --size or --conc)")
	}
}
