package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alexvoro/llm-stream-proxy/config"
	"github.com/alexvoro/llm-stream-proxy/provider"
	"github.com/alexvoro/llm-stream-proxy/proxy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Structured JSON logging.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	providers := buildProviders(cfg)
	slog.Info("initialized providers", "count", len(providers), "strategy", cfg.Strategy)
	for _, p := range providers {
		slog.Info("provider registered", "name", p.Name())
	}

	mux := proxy.NewMultiplexer(providers, cfg.Strategy)
	handler := proxy.NewHandler(mux)

	router := http.NewServeMux()
	router.Handle("POST /v1/chat/completions", handler)
	router.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})
	router.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      router,
		ReadTimeout:  cfg.RequestTimeout,
		WriteTimeout: cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown.
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("server starting", "addr", cfg.ListenAddr, "strategy", cfg.Strategy)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-done
	slog.Info("shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
	slog.Info("server stopped")
}

func buildProviders(cfg *config.Config) []proxy.Provider {
	var providers []proxy.Provider

	if cfg.UseMockProviders {
		providers = append(providers,
			provider.NewMockProvider(provider.MockProviderConfig{
				Name:       "mock-fast",
				Model:      "mock-gpt-4",
				Latency:    cfg.MockALatency,
				TokenDelay: cfg.MockATokenDelay,
			}),
			provider.NewMockProvider(provider.MockProviderConfig{
				Name:       "mock-slow",
				Model:      "mock-claude",
				Latency:    cfg.MockBLatency,
				TokenDelay: cfg.MockBTokenDelay,
			}),
		)
		return providers
	}

	if cfg.OpenAIAPIKey != "" {
		providers = append(providers, provider.NewOpenAIProvider(provider.OpenAIProviderConfig{
			Name:     "openai",
			Endpoint: cfg.OpenAIEndpoint,
			APIKey:   cfg.OpenAIAPIKey,
		}))
	}

	if cfg.AnthropicAPIKey != "" {
		providers = append(providers, provider.NewAnthropicProvider(provider.AnthropicProviderConfig{
			Name:     "anthropic",
			Endpoint: cfg.AnthropicEndpoint,
			APIKey:   cfg.AnthropicAPIKey,
		}))
	}

	if cfg.OllamaEndpoint != "" {
		providers = append(providers, provider.NewOllamaProvider(provider.OllamaProviderConfig{
			Name:     "ollama",
			Endpoint: cfg.OllamaEndpoint,
		}))
	}

	return providers
}
