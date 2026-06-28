package proxy

import "context"

// StreamChunk represents a single chunk from an upstream provider.
// Data contains the raw SSE payload already in OpenAI-compatible format.
type StreamChunk struct {
	Data []byte
	Err  error
	Done bool
}

// Provider defines the interface that all LLM providers must implement.
// Implementations are responsible for translating their native API format
// into OpenAI-compatible SSE chunks.
type Provider interface {
	Name() string
	StreamChat(ctx context.Context, reqBody []byte) (<-chan StreamChunk, error)
}

// RateLimitError indicates the upstream provider returned HTTP 429.
type RateLimitError struct {
	Provider string
	Err      error
}

func (e *RateLimitError) Error() string {
	return e.Provider + ": rate limited: " + e.Err.Error()
}

func (e *RateLimitError) Unwrap() error {
	return e.Err
}

// ProviderError indicates a non-rate-limit upstream failure.
type ProviderError struct {
	Provider   string
	StatusCode int
	Err        error
}

func (e *ProviderError) Error() string {
	return e.Provider + ": " + e.Err.Error()
}

func (e *ProviderError) Unwrap() error {
	return e.Err
}
