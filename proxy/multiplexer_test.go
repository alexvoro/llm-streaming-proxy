package proxy

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexvoro/llm-stream-proxy/config"
)

// testProvider is a minimal Provider for testing multiplexer logic.
type testProvider struct {
	name       string
	latency    time.Duration
	tokens     []string
	shouldFail bool
	rateLimit  bool
	cancelled  atomic.Bool
}

func (p *testProvider) Name() string { return p.name }

func (p *testProvider) StreamChat(ctx context.Context, reqBody []byte) (<-chan StreamChunk, error) {
	if p.rateLimit {
		return nil, &RateLimitError{Provider: p.name, Err: fmt.Errorf("HTTP 429")}
	}
	if p.shouldFail {
		return nil, &ProviderError{Provider: p.name, Err: fmt.Errorf("connection refused")}
	}

	ch := make(chan StreamChunk, len(p.tokens)+1)
	go func() {
		defer close(ch)

		select {
		case <-ctx.Done():
			p.cancelled.Store(true)
			return
		case <-time.After(p.latency):
		}

		for _, token := range p.tokens {
			data := []byte(fmt.Sprintf(`{"choices":[{"delta":{"content":"%s"}}]}`, token))
			select {
			case <-ctx.Done():
				p.cancelled.Store(true)
				return
			case ch <- StreamChunk{Data: data}:
			}
		}
		ch <- StreamChunk{Data: []byte(`{"choices":[{"delta":{}}]}`), Done: true}
	}()

	return ch, nil
}

func TestRace_FastProviderWins(t *testing.T) {
	fast := &testProvider{name: "fast", latency: 5 * time.Millisecond, tokens: []string{"hello"}}
	slow := &testProvider{name: "slow", latency: 200 * time.Millisecond, tokens: []string{"world"}}

	mux := NewMultiplexer([]Provider{fast, slow}, config.StrategyRace)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := mux.Stream(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Provider != "fast" {
		t.Errorf("winner = %q, want %q", result.Provider, "fast")
	}

	// Drain chunks.
	for range result.Chunks {
	}

	// Give goroutines time to process cancellation.
	time.Sleep(50 * time.Millisecond)

	if !slow.cancelled.Load() {
		t.Error("slow provider should have been cancelled")
	}
}

func TestRace_AllProvidersFail(t *testing.T) {
	p1 := &testProvider{name: "p1", shouldFail: true}
	p2 := &testProvider{name: "p2", shouldFail: true}

	mux := NewMultiplexer([]Provider{p1, p2}, config.StrategyRace)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := mux.Stream(ctx, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error when all providers fail")
	}
}

func TestRace_OneFailsOneSucceeds(t *testing.T) {
	failing := &testProvider{name: "failing", shouldFail: true}
	working := &testProvider{name: "working", latency: 5 * time.Millisecond, tokens: []string{"ok"}}

	mux := NewMultiplexer([]Provider{failing, working}, config.StrategyRace)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := mux.Stream(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Provider != "working" {
		t.Errorf("winner = %q, want %q", result.Provider, "working")
	}

	for range result.Chunks {
	}
}

func TestFallback_PrimarySucceeds(t *testing.T) {
	primary := &testProvider{name: "primary", latency: 5 * time.Millisecond, tokens: []string{"hello"}}
	secondary := &testProvider{name: "secondary", latency: 5 * time.Millisecond, tokens: []string{"world"}}

	mux := NewMultiplexer([]Provider{primary, secondary}, config.StrategyFallback)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := mux.Stream(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Provider != "primary" {
		t.Errorf("provider = %q, want %q", result.Provider, "primary")
	}

	for range result.Chunks {
	}
}

func TestFallback_PrimaryRateLimited(t *testing.T) {
	primary := &testProvider{name: "primary", rateLimit: true}
	secondary := &testProvider{name: "secondary", latency: 5 * time.Millisecond, tokens: []string{"fallback"}}

	mux := NewMultiplexer([]Provider{primary, secondary}, config.StrategyFallback)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := mux.Stream(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Provider != "secondary" {
		t.Errorf("provider = %q, want %q", result.Provider, "secondary")
	}

	for range result.Chunks {
	}
}

func TestFallback_AllFail(t *testing.T) {
	p1 := &testProvider{name: "p1", shouldFail: true}
	p2 := &testProvider{name: "p2", rateLimit: true}

	mux := NewMultiplexer([]Provider{p1, p2}, config.StrategyFallback)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := mux.Stream(ctx, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error when all providers fail")
	}
}
