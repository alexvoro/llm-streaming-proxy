package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_requests_total",
			Help: "Total number of proxy requests by strategy and status code.",
		},
		[]string{"strategy", "status_code"},
	)

	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "proxy_request_duration_seconds",
			Help:    "Duration of proxy requests in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"strategy"},
	)

	firstTokenLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "proxy_first_token_latency_seconds",
			Help:    "Time to first token from a provider in seconds.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"provider", "strategy"},
	)

	providerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_provider_errors_total",
			Help: "Total provider errors by provider and error type.",
		},
		[]string{"provider", "error_type"},
	)
)

func RecordRequest(strategy string, statusCode int, duration time.Duration) {
	requestsTotal.WithLabelValues(strategy, statusCodeStr(statusCode)).Inc()
	requestDuration.WithLabelValues(strategy).Observe(duration.Seconds())
}

func RecordFirstTokenLatency(provider, strategy string, latency time.Duration) {
	firstTokenLatency.WithLabelValues(provider, strategy).Observe(latency.Seconds())
}

func RecordProviderError(provider, errorType string) {
	providerErrors.WithLabelValues(provider, errorType).Inc()
}

func statusCodeStr(code int) string {
	switch code {
	case 200:
		return "200"
	case 400:
		return "400"
	case 429:
		return "429"
	case 500:
		return "500"
	case 502:
		return "502"
	case 504:
		return "504"
	default:
		return "other"
	}
}
