package provider

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/alexvoro/llm-stream-proxy/proxy"
)

// AnthropicProvider streams chat completions from Anthropic's Messages API
// and translates the response into OpenAI-compatible SSE chunks.
type AnthropicProvider struct {
	name     string
	endpoint string
	apiKey   string
	version  string
	client   *http.Client
}

type AnthropicProviderConfig struct {
	Name     string
	Endpoint string // e.g. "https://api.anthropic.com"
	APIKey   string
	Version  string // e.g. "2023-06-01"
	Client   *http.Client
}

func NewAnthropicProvider(cfg AnthropicProviderConfig) *AnthropicProvider {
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	version := cfg.Version
	if version == "" {
		version = "2023-06-01"
	}
	return &AnthropicProvider{
		name:     cfg.Name,
		endpoint: cfg.Endpoint,
		apiKey:   cfg.APIKey,
		version:  version,
		client:   client,
	}
}

func (p *AnthropicProvider) Name() string { return p.name }

func (p *AnthropicProvider) StreamChat(ctx context.Context, reqBody []byte) (<-chan proxy.StreamChunk, error) {
	// Translate OpenAI request format to Anthropic format.
	anthropicBody, model, err := translateRequestToAnthropic(reqBody)
	if err != nil {
		return nil, fmt.Errorf("%s: translate request: %w", p.name, err)
	}

	url := p.endpoint + "/v1/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, io.NopCloser(
		strings.NewReader(string(anthropicBody)),
	))
	if err != nil {
		return nil, fmt.Errorf("%s: create request: %w", p.name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", p.apiKey)
	req.Header.Set("anthropic-version", p.version)

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

	out := make(chan proxy.StreamChunk, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		p.readAnthropicSSE(ctx, resp.Body, model, out)
	}()

	return out, nil
}

// readAnthropicSSE parses Anthropic's SSE event stream and translates
// content_block_delta events into OpenAI-compatible chunks.
func (p *AnthropicProvider) readAnthropicSSE(ctx context.Context, r io.Reader, model string, out chan<- proxy.StreamChunk) {
	scanner := bufio.NewScanner(r)
	var eventType string

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
			continue
		}

		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")

		switch eventType {
		case "content_block_delta":
			var delta anthropicDelta
			if err := json.Unmarshal([]byte(payload), &delta); err != nil {
				continue
			}
			if delta.Delta.Text == "" {
				continue
			}
			chunk := proxy.NewChunk(model, delta.Delta.Text)
			data, err := json.Marshal(chunk)
			if err != nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case out <- proxy.StreamChunk{Data: data}:
			}

		case "message_stop":
			doneChunk := proxy.NewDoneChunk(model)
			data, _ := json.Marshal(doneChunk)
			select {
			case <-ctx.Done():
				return
			case out <- proxy.StreamChunk{Data: data, Done: true}:
			}
			return
		}
	}

	if err := scanner.Err(); err != nil {
		select {
		case <-ctx.Done():
		case out <- proxy.StreamChunk{Err: err}:
		}
	}
}

// Anthropic API types for parsing SSE events.
type anthropicDelta struct {
	Delta struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"delta"`
}

type anthropicRequest struct {
	Model     string            `json:"model"`
	MaxTokens int               `json:"max_tokens"`
	Stream    bool              `json:"stream"`
	Messages  []anthropicMsg    `json:"messages"`
}

type anthropicMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// translateRequestToAnthropic converts an OpenAI-format request body
// into an Anthropic Messages API request. Returns the marshaled body
// and the model name.
func translateRequestToAnthropic(openaiBody []byte) ([]byte, string, error) {
	var req proxy.ChatCompletionRequest
	if err := json.Unmarshal(openaiBody, &req); err != nil {
		return nil, "", fmt.Errorf("unmarshal request: %w", err)
	}

	model := req.Model
	if model == "" {
		model = "claude-sonnet-4-20250514"
	}

	var msgs []anthropicMsg
	for _, m := range req.Messages {
		if m.Role == "system" {
			continue // Anthropic handles system differently; skip for simplicity.
		}
		msgs = append(msgs, anthropicMsg{Role: m.Role, Content: m.Content})
	}

	ar := anthropicRequest{
		Model:     model,
		MaxTokens: 1024,
		Stream:    true,
		Messages:  msgs,
	}

	data, err := json.Marshal(ar)
	if err != nil {
		return nil, "", err
	}
	return data, model, nil
}
