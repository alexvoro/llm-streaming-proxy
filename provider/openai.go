package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/alexvoro/llm-stream-proxy/proxy"
)

// OpenAIProvider streams chat completions from an OpenAI-compatible endpoint.
type OpenAIProvider struct {
	name     string
	endpoint string
	apiKey   string
	client   *http.Client
}

type OpenAIProviderConfig struct {
	Name     string
	Endpoint string // e.g. "https://api.openai.com"
	APIKey   string
	Client   *http.Client
}

func NewOpenAIProvider(cfg OpenAIProviderConfig) *OpenAIProvider {
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	return &OpenAIProvider{
		name:     cfg.Name,
		endpoint: cfg.Endpoint,
		apiKey:   cfg.APIKey,
		client:   client,
	}
}

func (p *OpenAIProvider) Name() string { return p.name }

func (p *OpenAIProvider) StreamChat(ctx context.Context, reqBody []byte) (<-chan proxy.StreamChunk, error) {
	url := p.endpoint + "/v1/chat/completions"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, io.NopCloser(
		newJSONReaderWithStream(reqBody),
	))
	if err != nil {
		return nil, fmt.Errorf("%s: create request: %w", p.name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, &proxy.ProviderError{Provider: p.name, Err: fmt.Errorf("request failed: %w", err)}
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		resp.Body.Close()
		return nil, &proxy.RateLimitError{Provider: p.name, Err: fmt.Errorf("HTTP 429")}
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, &proxy.ProviderError{
			Provider:   p.name,
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body)),
		}
	}

	// Read SSE stream from the response body. The channel is closed when
	// the body is exhausted or the context is cancelled.
	ch := proxy.ReadSSEStream(resp.Body)

	// Wrap in a goroutine that closes the body when the channel drains
	// or context is cancelled.
	out := make(chan proxy.StreamChunk, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		for chunk := range ch {
			select {
			case <-ctx.Done():
				return
			case out <- chunk:
			}
		}
	}()

	return out, nil
}
