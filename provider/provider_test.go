package provider

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alexvoro/llm-stream-proxy/proxy"
	"github.com/alexvoro/llm-stream-proxy/testutil"
)

func TestOpenAIProvider_StreamChat(t *testing.T) {
	srv := testutil.NewMockSSEServer(testutil.MockSSEServerConfig{
		InitialLatency: 10 * time.Millisecond,
		TokenDelay:     5 * time.Millisecond,
		Tokens:         []string{"Hello", " world"},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIProviderConfig{
		Name:     "test-openai",
		Endpoint: srv.URL,
		APIKey:   "test-key",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, _ := json.Marshal(proxy.ChatCompletionRequest{
		Messages: []proxy.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	})

	ch, err := p.StreamChat(ctx, body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var chunks []proxy.StreamChunk
	for chunk := range ch {
		chunks = append(chunks, chunk)
	}

	if len(chunks) < 2 {
		t.Errorf("expected at least 2 chunks, got %d", len(chunks))
	}

	// Verify first chunk has content.
	var cc proxy.ChatCompletionChunk
	if err := json.Unmarshal(chunks[0].Data, &cc); err != nil {
		t.Fatalf("failed to unmarshal first chunk: %v", err)
	}
	if len(cc.Choices) == 0 || cc.Choices[0].Delta.Content != "Hello" {
		t.Errorf("first chunk content = %q, want %q", cc.Choices[0].Delta.Content, "Hello")
	}
}

func TestOpenAIProvider_RateLimit(t *testing.T) {
	srv := testutil.NewMockSSEServer(testutil.MockSSEServerConfig{
		RateLimit: true,
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIProviderConfig{
		Name:     "test-openai",
		Endpoint: srv.URL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, _ := json.Marshal(proxy.ChatCompletionRequest{
		Messages: []proxy.Message{{Role: "user", Content: "hi"}},
	})

	_, err := p.StreamChat(ctx, body)
	if err == nil {
		t.Fatal("expected rate limit error")
	}

	var rle *proxy.RateLimitError
	if !errors.As(err, &rle) {
		t.Errorf("expected RateLimitError, got %T: %v", err, err)
	}
}

func TestOpenAIProvider_ServerError(t *testing.T) {
	srv := testutil.NewMockSSEServer(testutil.MockSSEServerConfig{
		SimulateError: true,
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIProviderConfig{
		Name:     "test-openai",
		Endpoint: srv.URL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, _ := json.Marshal(proxy.ChatCompletionRequest{
		Messages: []proxy.Message{{Role: "user", Content: "hi"}},
	})

	_, err := p.StreamChat(ctx, body)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOpenAIProvider_ContextCancellation(t *testing.T) {
	srv := testutil.NewMockSSEServer(testutil.MockSSEServerConfig{
		InitialLatency: 500 * time.Millisecond,
		Tokens:         []string{"slow", "response"},
	})
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIProviderConfig{
		Name:     "test-openai",
		Endpoint: srv.URL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	body, _ := json.Marshal(proxy.ChatCompletionRequest{
		Messages: []proxy.Message{{Role: "user", Content: "hi"}},
	})

	ch, err := p.StreamChat(ctx, body)
	if err != nil {
		// Context cancelled during HTTP request — acceptable.
		return
	}

	// The mock server sends 200 headers immediately, so StreamChat may
	// return a channel. The context cancellation then happens during
	// body reading — the channel should close quickly with no data tokens.
	var dataChunks int
	for chunk := range ch {
		if chunk.Err == nil && !chunk.Done {
			dataChunks++
		}
	}

	if dataChunks > 0 {
		t.Errorf("expected 0 data chunks (cancelled), got %d", dataChunks)
	}
}

func TestMockProvider_StreamChat(t *testing.T) {
	p := NewMockProvider(MockProviderConfig{
		Name:       "test-mock",
		Model:      "test-model",
		Latency:    5 * time.Millisecond,
		TokenDelay: 2 * time.Millisecond,
		Tokens:     []string{"one", "two", "three"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := p.StreamChat(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var chunks []proxy.StreamChunk
	for chunk := range ch {
		chunks = append(chunks, chunk)
	}

	// 3 tokens + 1 done chunk = 4
	if len(chunks) != 4 {
		t.Errorf("expected 4 chunks, got %d", len(chunks))
	}

	// Last chunk should be done.
	if !chunks[len(chunks)-1].Done {
		t.Error("last chunk should have Done=true")
	}
}

func TestMockProvider_ContextCancellation(t *testing.T) {
	p := NewMockProvider(MockProviderConfig{
		Name:       "test-mock",
		Latency:    500 * time.Millisecond,
		TokenDelay: 10 * time.Millisecond,
		Tokens:     []string{"never", "arrives"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	ch, err := p.StreamChat(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var chunks []proxy.StreamChunk
	for chunk := range ch {
		chunks = append(chunks, chunk)
	}

	// Should have been cancelled before any tokens arrived.
	if len(chunks) != 0 {
		t.Errorf("expected 0 chunks (cancelled), got %d", len(chunks))
	}
}

func TestOllamaProvider_StreamChat(t *testing.T) {
	srv := testutil.NewMockSSEServer(testutil.MockSSEServerConfig{
		InitialLatency: 5 * time.Millisecond,
		TokenDelay:     2 * time.Millisecond,
		Tokens:         []string{"ollama", "response"},
	})
	defer srv.Close()

	p := NewOllamaProvider(OllamaProviderConfig{
		Name:     "test-ollama",
		Endpoint: srv.URL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, _ := json.Marshal(proxy.ChatCompletionRequest{
		Messages: []proxy.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	})

	ch, err := p.StreamChat(ctx, body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count := 0
	for range ch {
		count++
	}

	if count < 2 {
		t.Errorf("expected at least 2 chunks, got %d", count)
	}
}
