package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alexvoro/llm-stream-proxy/config"
)

func TestHandler_StreamingResponse(t *testing.T) {
	fast := &testProvider{name: "fast", latency: 5 * time.Millisecond, tokens: []string{"Hello", " world"}}
	mux := NewMultiplexer([]Provider{fast}, config.StrategyFallback)
	handler := NewHandler(mux)

	body := `{"messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "text/event-stream" {
		t.Errorf("content-type = %q, want %q", ct, "text/event-stream")
	}

	respBody, _ := io.ReadAll(resp.Body)
	respStr := string(respBody)

	if !strings.Contains(respStr, "data: ") {
		t.Error("response should contain SSE data lines")
	}
	if !strings.Contains(respStr, "[DONE]") {
		t.Error("response should contain [DONE] sentinel")
	}
}

func TestHandler_NonStreamingResponse(t *testing.T) {
	fast := &testProvider{name: "fast", latency: 5 * time.Millisecond, tokens: []string{"Hello"}}
	mux := NewMultiplexer([]Provider{fast}, config.StrategyFallback)
	handler := NewHandler(mux)

	body := `{"messages":[{"role":"user","content":"hi"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["object"] != "chat.completion" {
		t.Errorf("object = %v, want %q", result["object"], "chat.completion")
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	fast := &testProvider{name: "fast", latency: 5 * time.Millisecond, tokens: []string{"Hello"}}
	mux := NewMultiplexer([]Provider{fast}, config.StrategyFallback)
	handler := NewHandler(mux)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("not json"))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandler_EmptyMessages(t *testing.T) {
	fast := &testProvider{name: "fast", latency: 5 * time.Millisecond, tokens: []string{"Hello"}}
	mux := NewMultiplexer([]Provider{fast}, config.StrategyFallback)
	handler := NewHandler(mux)

	body := `{"messages":[],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	fast := &testProvider{name: "fast", latency: 5 * time.Millisecond, tokens: []string{"Hello"}}
	mux := NewMultiplexer([]Provider{fast}, config.StrategyFallback)
	handler := NewHandler(mux)

	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandler_AllProvidersFail(t *testing.T) {
	p := &testProvider{name: "broken", shouldFail: true}
	mux := NewMultiplexer([]Provider{p}, config.StrategyFallback)
	handler := NewHandler(mux)

	body := `{"messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req = req.WithContext(context.Background())
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadGateway)
	}
}
