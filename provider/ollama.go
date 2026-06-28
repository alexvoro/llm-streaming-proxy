package provider

import (
	"net/http"
)

// OllamaProvider streams chat completions from a local Ollama instance
// using its OpenAI-compatible API endpoint.
type OllamaProvider struct {
	*OpenAIProvider
}

type OllamaProviderConfig struct {
	Name     string
	Endpoint string // e.g. "http://localhost:11434"
	Client   *http.Client
}

func NewOllamaProvider(cfg OllamaProviderConfig) *OllamaProvider {
	return &OllamaProvider{
		OpenAIProvider: NewOpenAIProvider(OpenAIProviderConfig{
			Name:     cfg.Name,
			Endpoint: cfg.Endpoint,
			Client:   cfg.Client,
		}),
	}
}
