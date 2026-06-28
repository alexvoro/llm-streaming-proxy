package testutil

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/alexvoro/llm-stream-proxy/proxy"
)

// MockSSEServerConfig configures a mock SSE server.
type MockSSEServerConfig struct {
	InitialLatency time.Duration
	TokenDelay     time.Duration
	Tokens         []string
	StatusCode     int  // defaults to 200
	SimulateError  bool // if true, returns 500
	RateLimit      bool // if true, returns 429
}

// NewMockSSEServer creates an httptest.Server that serves SSE streams.
func NewMockSSEServer(cfg MockSSEServerConfig) *httptest.Server {
	if cfg.StatusCode == 0 {
		cfg.StatusCode = http.StatusOK
	}
	if len(cfg.Tokens) == 0 {
		cfg.Tokens = []string{"Hello", " from", " mock", " server", "!"}
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.RateLimit {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"rate limited"}`))
			return
		}

		if cfg.SimulateError {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"internal server error"}`))
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(cfg.StatusCode)
		flusher.Flush()

		time.Sleep(cfg.InitialLatency)

		for _, token := range cfg.Tokens {
			chunk := proxy.NewChunk("mock-model", token)
			data, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()

			time.Sleep(cfg.TokenDelay)
		}

		// Send done sentinel.
		doneChunk := proxy.NewDoneChunk("mock-model")
		data, _ := json.Marshal(doneChunk)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()

		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
}
