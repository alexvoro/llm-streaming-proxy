package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/alexvoro/llm-stream-proxy/config"
	"github.com/alexvoro/llm-stream-proxy/metrics"
)

// Multiplexer orchestrates requests across multiple providers using
// either a race or fallback strategy.
type Multiplexer struct {
	providers []Provider
	strategy  config.Strategy
}

func NewMultiplexer(providers []Provider, strategy config.Strategy) *Multiplexer {
	return &Multiplexer{
		providers: providers,
		strategy:  strategy,
	}
}

// StreamResult holds the outcome of a multiplexed streaming request.
type StreamResult struct {
	Provider string
	Chunks   <-chan StreamChunk
}

// Stream dispatches the request to providers according to the configured strategy.
func (m *Multiplexer) Stream(ctx context.Context, reqBody []byte) (*StreamResult, error) {
	switch m.strategy {
	case config.StrategyRace:
		return m.race(ctx, reqBody)
	case config.StrategyFallback:
		return m.fallback(ctx, reqBody)
	default:
		return nil, fmt.Errorf("unknown strategy: %s", m.strategy)
	}
}

// race launches all providers concurrently and returns the first to stream a chunk.
// The losing providers are cancelled immediately via context; the winner keeps its
// own independent context so it can continue streaming.
func (m *Multiplexer) race(ctx context.Context, reqBody []byte) (*StreamResult, error) {
	type result struct {
		provider string
		chunks   <-chan StreamChunk
		first    StreamChunk
		err      error
		idx      int
	}

	// Each provider gets its own cancellable context so we can cancel losers
	// without killing the winner.
	cancels := make([]context.CancelFunc, len(m.providers))
	results := make(chan result, len(m.providers))

	for i, p := range m.providers {
		provCtx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel

		go func(p Provider, idx int) {
			ch, err := p.StreamChat(provCtx, reqBody)
			if err != nil {
				results <- result{provider: p.Name(), err: err, idx: idx}
				return
			}

			// Wait for the first chunk to confirm this provider is streaming.
			select {
			case <-provCtx.Done():
				results <- result{provider: p.Name(), err: provCtx.Err(), idx: idx}
				return
			case chunk, ok := <-ch:
				if !ok {
					results <- result{provider: p.Name(), err: fmt.Errorf("channel closed without data"), idx: idx}
					return
				}
				if chunk.Err != nil {
					results <- result{provider: p.Name(), err: chunk.Err, idx: idx}
					return
				}
				results <- result{provider: p.Name(), chunks: ch, first: chunk, idx: idx}
			}
		}(p, i)
	}

	// Collect results until we find a winner or all providers fail.
	var errs []error
	for range m.providers {
		r := <-results
		if r.err != nil {
			slog.Warn("provider failed in race", "provider", r.provider, "error", r.err)
			metrics.RecordProviderError(r.provider, classifyError(r.err))
			errs = append(errs, fmt.Errorf("%s: %w", r.provider, r.err))
			continue
		}

		// Winner found — cancel all OTHER providers, keep the winner alive.
		slog.Info("race winner", "provider", r.provider)
		for i, cancel := range cancels {
			if i != r.idx {
				cancel()
			}
		}

		// Create a merged channel: emit the first chunk, then forward the rest.
		merged := make(chan StreamChunk, 16)
		go func() {
			defer close(merged)
			merged <- r.first
			for chunk := range r.chunks {
				merged <- chunk
			}
		}()

		return &StreamResult{
			Provider: r.provider,
			Chunks:   merged,
		}, nil
	}

	for _, cancel := range cancels {
		cancel()
	}
	return nil, fmt.Errorf("all providers failed: %w", errors.Join(errs...))
}

// fallback tries providers in order, falling back to the next on error or rate limit.
func (m *Multiplexer) fallback(ctx context.Context, reqBody []byte) (*StreamResult, error) {
	var errs []error
	var mu sync.Mutex

	for _, p := range m.providers {
		slog.Info("trying provider", "provider", p.Name(), "strategy", "fallback")

		ch, err := p.StreamChat(ctx, reqBody)
		if err != nil {
			slog.Warn("provider failed, trying next", "provider", p.Name(), "error", err)
			metrics.RecordProviderError(p.Name(), classifyError(err))
			mu.Lock()
			errs = append(errs, fmt.Errorf("%s: %w", p.Name(), err))
			mu.Unlock()
			continue
		}

		// Wait for the first chunk to confirm it's streaming.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case chunk, ok := <-ch:
			if !ok {
				errs = append(errs, fmt.Errorf("%s: channel closed without data", p.Name()))
				continue
			}
			if chunk.Err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", p.Name(), chunk.Err))
				continue
			}

			slog.Info("fallback provider streaming", "provider", p.Name())

			merged := make(chan StreamChunk, 16)
			go func() {
				defer close(merged)
				merged <- chunk
				for c := range ch {
					merged <- c
				}
			}()

			return &StreamResult{
				Provider: p.Name(),
				Chunks:   merged,
			}, nil
		}
	}

	return nil, fmt.Errorf("all providers failed: %w", errors.Join(errs...))
}

// classifyError returns a short error type label for metrics.
func classifyError(err error) string {
	var rle *RateLimitError
	if errors.As(err, &rle) {
		return "rate_limit"
	}
	var pe *ProviderError
	if errors.As(err, &pe) {
		return "upstream_error"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "connection"
}
