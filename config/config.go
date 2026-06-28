package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Strategy string

const (
	StrategyRace     Strategy = "race"
	StrategyFallback Strategy = "fallback"
)

type Config struct {
	ListenAddr string
	Strategy   Strategy

	// Provider endpoints
	OpenAIEndpoint    string
	OpenAIAPIKey      string
	AnthropicEndpoint string
	AnthropicAPIKey   string
	OllamaEndpoint    string

	// Timeouts
	RequestTimeout    time.Duration
	UpstreamTimeout   time.Duration
	ShutdownTimeout   time.Duration

	// Mock provider settings
	UseMockProviders   bool
	MockALatency       time.Duration
	MockATokenDelay    time.Duration
	MockBLatency       time.Duration
	MockBTokenDelay    time.Duration
}

func Load() (*Config, error) {
	c := &Config{
		ListenAddr:        envOrDefault("LISTEN_ADDR", ":8080"),
		Strategy:          Strategy(envOrDefault("STRATEGY", "race")),
		OpenAIEndpoint:    envOrDefault("OPENAI_ENDPOINT", "https://api.openai.com"),
		OpenAIAPIKey:      os.Getenv("OPENAI_API_KEY"),
		AnthropicEndpoint: envOrDefault("ANTHROPIC_ENDPOINT", "https://api.anthropic.com"),
		AnthropicAPIKey:   os.Getenv("ANTHROPIC_API_KEY"),
		OllamaEndpoint:    envOrDefault("OLLAMA_ENDPOINT", "http://localhost:11434"),
		RequestTimeout:    envDurationOrDefault("REQUEST_TIMEOUT", 30*time.Second),
		UpstreamTimeout:   envDurationOrDefault("UPSTREAM_TIMEOUT", 25*time.Second),
		ShutdownTimeout:   envDurationOrDefault("SHUTDOWN_TIMEOUT", 5*time.Second),
		UseMockProviders:  envBoolOrDefault("USE_MOCK_PROVIDERS", true),
		MockALatency:      envDurationOrDefault("MOCK_A_LATENCY", 20*time.Millisecond),
		MockATokenDelay:   envDurationOrDefault("MOCK_A_TOKEN_DELAY", 30*time.Millisecond),
		MockBLatency:      envDurationOrDefault("MOCK_B_LATENCY", 80*time.Millisecond),
		MockBTokenDelay:   envDurationOrDefault("MOCK_B_TOKEN_DELAY", 25*time.Millisecond),
	}

	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) validate() error {
	switch c.Strategy {
	case StrategyRace, StrategyFallback:
	default:
		return fmt.Errorf("invalid strategy %q: must be %q or %q", c.Strategy, StrategyRace, StrategyFallback)
	}

	if !c.UseMockProviders {
		hasOpenAI := c.OpenAIAPIKey != ""
		hasAnthropic := c.AnthropicAPIKey != ""
		hasOllama := c.OllamaEndpoint != ""

		providerCount := 0
		if hasOpenAI {
			providerCount++
		}
		if hasAnthropic {
			providerCount++
		}
		if hasOllama {
			providerCount++
		}

		if c.Strategy == StrategyRace && providerCount < 2 {
			return fmt.Errorf("race strategy requires at least 2 configured providers, got %d", providerCount)
		}
		if providerCount == 0 {
			return fmt.Errorf("no providers configured; set API keys or enable USE_MOCK_PROVIDERS=true")
		}
	}

	return nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOrDefault(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envBoolOrDefault(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
