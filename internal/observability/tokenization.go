package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"log/slog"
	"strconv"
	"sync"
)

type TokenizationMetrics struct {
	mu         sync.Mutex
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	rejections *prometheus.CounterVec
	logger     *slog.Logger
	snapshot   func() *registry.Snapshot
	observed   map[backendMetricLabels]struct{}
}

func (m *Metrics) NewTokenizationObserver(logger *slog.Logger, snapshot func() *registry.Snapshot, c coordination.TokenizationCoordinator) *TokenizationMetrics {
	o := &TokenizationMetrics{logger: logger, snapshot: snapshot, observed: make(map[backendMetricLabels]struct{}), requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_tokenize_requests_total", Help: "Completed utility tokenization HTTP requests."}, []string{"model", "backend", "status"}), duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_tokenize_duration_seconds", Help: "Utility tokenization request duration."}, []string{"model", "backend"}), rejections: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_tokenize_rejections_total", Help: "Utility tokenization rejections."}, []string{"reason"})}
	m.registry.MustRegister(o.requests, o.duration, o.rejections, prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "llmgw_tokenize_coordination_available", Help: "Utility coordination availability."}, func() float64 {
		if c != nil && c.Status().Available {
			return 1
		}
		return 0
	}))
	return o
}
func (o *TokenizationMetrics) Reconcile() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reconcile(o.snapshot())
}
func (o *TokenizationMetrics) reconcile(s *registry.Snapshot) {
	for labels := range o.observed {
		valid := labels.model == "" && labels.backend == ""
		if p, ok := s.PoolsByName[labels.model]; ok {
			valid = labels.backend == ""
			for _, b := range s.BackendsByPool[p.ID] {
				if b.Name == labels.backend {
					valid = true
					break
				}
			}
		}
		if !valid {
			o.requests.DeletePartialMatch(prometheus.Labels{"model": labels.model, "backend": labels.backend})
			o.duration.DeleteLabelValues(labels.model, labels.backend)
			delete(o.observed, labels)
		}
	}
}
func (o *TokenizationMetrics) TokenizationComplete(e gateway.TokenizationEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := o.snapshot()
	o.reconcile(s)
	if p, ok := s.PoolsByName[e.Model]; !ok {
		e.Model = ""
		e.Backend = ""
	} else if e.Backend != "" {
		found := false
		for _, b := range s.BackendsByPool[p.ID] {
			if b.Name == e.Backend {
				found = true
				break
			}
		}
		if !found {
			e.Backend = ""
		}
	}
	status := e.Status
	if status < 100 || status > 599 {
		status = 500
	}
	o.requests.WithLabelValues(e.Model, e.Backend, strconv.Itoa(status)).Inc()
	o.duration.WithLabelValues(e.Model, e.Backend).Observe(e.Duration.Seconds())
	o.observed[backendMetricLabels{model: e.Model, backend: e.Backend}] = struct{}{}
	reason := bounded(e.Reason, "success", "invalid_api_key", "invalid_request", "invalid_request_error", "model_not_allowed", "unsupported_endpoint", "internal_error", "client_concurrency", "backend_capacity", "client_rate", "coordination_unavailable", "stale_configuration", "stale_operation", "idempotency_conflict", "lease_lost", "no_eligible_backend", "transport_error", "cancelled", "upstream_error", "panic", "rejected")
	if status >= 400 && status < 500 || status == 503 {
		o.rejections.WithLabelValues(reason).Inc()
	}
	if o.logger != nil {
		o.logger.Info("utility request completed", "event", "tokenization_complete", "request_id", e.RequestID, "client", e.Client, "model", e.Model, "backend", e.Backend, "status", status, "reason", reason, "duration_ms", e.Duration.Milliseconds(), "cancelled", e.Cancelled)
	}
}
