package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/alexvoro/llm-stream-proxy/proxy"
)

// MockProvider generates fake streaming responses with configurable latency.
// It requires no network calls and is the default provider for demo/testing.
type MockProvider struct {
	name        string
	model       string
	latency     time.Duration // time-to-first-token
	tokenDelay  time.Duration // delay between tokens
	tokens      []string
}

// MockProviderConfig configures a MockProvider instance.
type MockProviderConfig struct {
	Name       string
	Model      string
	Latency    time.Duration
	TokenDelay time.Duration
	Tokens     []string
}

func NewMockProvider(cfg MockProviderConfig) *MockProvider {
	tokens := cfg.Tokens
	if len(tokens) == 0 {
		tokens = []string{
			"Hello", "!", " I", "'m", " a", " mock", " LLM",
			" response", " streaming", " token", " by", " token", ".",
		}
	}
	model := cfg.Model
	if model == "" {
		model = "mock-" + cfg.Name
	}
	return &MockProvider{
		name:       cfg.Name,
		model:      model,
		latency:    cfg.Latency,
		tokenDelay: cfg.TokenDelay,
		tokens:     tokens,
	}
}

func (p *MockProvider) Name() string { return p.name }

func (p *MockProvider) StreamChat(ctx context.Context, reqBody []byte) (<-chan proxy.StreamChunk, error) {
	ch := make(chan proxy.StreamChunk, len(p.tokens)+1)

	go func() {
		defer close(ch)

		// Simulate time-to-first-token latency.
		select {
		case <-ctx.Done():
			slog.Info("provider cancelled during initial latency", "provider", p.name)
			return
		case <-time.After(p.latency):
		}

		for _, token := range p.tokens {
			chunk := proxy.NewChunk(p.model, token)
			data, err := json.Marshal(chunk)
			if err != nil {
				ch <- proxy.StreamChunk{Err: fmt.Errorf("mock marshal: %w", err)}
				return
			}

			select {
			case <-ctx.Done():
				slog.Info("provider cancelled during streaming", "provider", p.name, "reason", ctx.Err())
				return
			case ch <- proxy.StreamChunk{Data: data}:
			}

			// Simulate inter-token delay.
			select {
			case <-ctx.Done():
				slog.Info("provider cancelled between tokens", "provider", p.name)
				return
			case <-time.After(p.tokenDelay):
			}
		}

		// Send final done chunk.
		doneChunk := proxy.NewDoneChunk(p.model)
		data, _ := json.Marshal(doneChunk)
		ch <- proxy.StreamChunk{Data: data, Done: true}
	}()

	return ch, nil
}
