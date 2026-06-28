package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/alexvoro/llm-stream-proxy/metrics"
)

// Handler serves OpenAI-compatible chat completion requests.
type Handler struct {
	mux *Multiplexer
}

func NewHandler(mux *Multiplexer) *Handler {
	return &Handler{mux: mux}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req ChatCompletionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid JSON: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, `{"error":"messages array is required"}`, http.StatusBadRequest)
		return
	}

	start := time.Now()

	result, err := h.mux.Stream(r.Context(), body)
	if err != nil {
		slog.Error("multiplexer failed", "error", err)
		metrics.RecordRequest(string(h.mux.strategy), http.StatusBadGateway, time.Since(start))
		http.Error(w, fmt.Sprintf(`{"error":"all providers failed: %s"}`, err.Error()), http.StatusBadGateway)
		return
	}

	if !req.Stream {
		h.handleNonStreaming(w, result, start)
		return
	}

	h.handleStreaming(w, result, start)
}

func (h *Handler) handleStreaming(w http.ResponseWriter, result *StreamResult, start time.Time) {
	sw, err := NewSSEWriter(w)
	if err != nil {
		http.Error(w, `{"error":"streaming not supported"}`, http.StatusInternalServerError)
		return
	}

	firstToken := true
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			slog.Error("stream chunk error", "provider", result.Provider, "error", chunk.Err)
			break
		}

		if firstToken {
			metrics.RecordFirstTokenLatency(result.Provider, string(h.mux.strategy), time.Since(start))
			firstToken = false
		}

		// Write the chunk as an SSE event.
		sseData := fmt.Sprintf("data: %s\n\n", string(chunk.Data))
		if err := sw.WriteEvent([]byte(sseData)); err != nil {
			slog.Error("failed to write SSE event", "error", err)
			break
		}

		if chunk.Done {
			break
		}
	}

	if err := sw.WriteDone(); err != nil {
		slog.Error("failed to write SSE done", "error", err)
	}

	metrics.RecordRequest(string(h.mux.strategy), http.StatusOK, time.Since(start))
}

func (h *Handler) handleNonStreaming(w http.ResponseWriter, result *StreamResult, start time.Time) {
	// Buffer all chunks and return as a single response.
	var content string
	var model string

	for chunk := range result.Chunks {
		if chunk.Err != nil {
			slog.Error("stream chunk error", "provider", result.Provider, "error", chunk.Err)
			http.Error(w, `{"error":"provider streaming error"}`, http.StatusBadGateway)
			return
		}
		if chunk.Done {
			break
		}

		var cc ChatCompletionChunk
		if err := json.Unmarshal(chunk.Data, &cc); err != nil {
			continue
		}
		model = cc.Model
		if len(cc.Choices) > 0 {
			content += cc.Choices[0].Delta.Content
		}
	}

	resp := map[string]any{
		"id":      fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]string{
					"role":    "assistant",
					"content": content,
				},
				"finish_reason": "stop",
			},
		},
	}

	metrics.RecordRequest(string(h.mux.strategy), http.StatusOK, time.Since(start))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
