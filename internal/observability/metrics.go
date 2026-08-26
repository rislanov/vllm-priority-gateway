package observability

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

// Metrics owns an isolated Prometheus registry and implements gateway.Observer.
type Metrics struct {
	registry         *prometheus.Registry
	requests         *prometheus.CounterVec
	requestsInflight *prometheus.GaugeVec
	rejected         *prometheus.CounterVec
	clientInflight   *prometheus.GaugeVec
	backendInflight  *prometheus.GaugeVec
	backendPressure  *prometheus.GaugeVec
	backendRunning   *prometheus.GaugeVec
	backendWaiting   *prometheus.GaugeVec
	backendKV        *prometheus.GaugeVec
	duration         *prometheus.HistogramVec
	ttft             *prometheus.HistogramVec
	disconnects      *prometheus.CounterVec
	backendFailures  *prometheus.CounterVec
	retries          *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry()}
	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_requests_total", Help: "Completed public inference requests."}, []string{"client", "model", "priority_class", "status_class"})
	m.requestsInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_requests_inflight", Help: "Public inference requests admitted and currently in flight."}, []string{"model", "priority_class"})
	m.rejected = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_requests_rejected_total", Help: "Requests rejected by the gateway."}, []string{"client", "model", "priority_class", "reason"})
	m.clientInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_client_inflight", Help: "Admitted requests currently in flight by client."}, []string{"client", "model", "priority_class"})
	m.backendInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_requests_inflight", Help: "Gateway requests currently assigned to a backend."}, []string{"model", "backend"})
	m.backendPressure = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_pressure", Help: "Smoothed backend pressure."}, []string{"model", "backend"})
	m.backendRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_running_requests", Help: "Running requests reported by vLLM."}, []string{"model", "backend"})
	m.backendWaiting = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_waiting_requests", Help: "Waiting requests reported by vLLM."}, []string{"model", "backend"})
	m.backendKV = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_kv_cache_usage", Help: "KV cache utilization reported by vLLM."}, []string{"model", "backend"})
	m.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_request_duration_seconds", Help: "End-to-end public request duration.", Buckets: prometheus.DefBuckets}, []string{"model", "backend", "priority_class", "status_class"})
	m.ttft = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_ttft_seconds", Help: "Time to first upstream response byte.", Buckets: prometheus.DefBuckets}, []string{"model", "backend", "priority_class"})
	m.disconnects = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_stream_disconnects_total", Help: "Streaming requests cancelled by a disconnected downstream."}, []string{"model", "backend", "priority_class"})
	m.backendFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_backend_failures_total", Help: "Requests that failed at a selected backend."}, []string{"model", "backend", "reason"})
	m.retries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_retries_total", Help: "Transparent pre-response retries."}, []string{"model", "backend"})
	m.registry.MustRegister(
		m.requests, m.requestsInflight, m.rejected, m.clientInflight, m.backendInflight,
		m.backendPressure, m.backendRunning, m.backendWaiting, m.backendKV, m.duration,
		m.ttft, m.disconnects, m.backendFailures, m.retries,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ClientInflight(event gateway.InflightEvent, delta int) {
	model, priority := value(event.Model), value(string(event.PriorityClass))
	m.requestsInflight.WithLabelValues(model, priority).Add(float64(delta))
	m.clientInflight.WithLabelValues(value(event.Client), model, priority).Add(float64(delta))
}

func (m *Metrics) BackendInflight(event gateway.InflightEvent, delta int) {
	m.backendInflight.WithLabelValues(value(event.Model), value(event.Backend)).Add(float64(delta))
}

func (m *Metrics) Complete(event gateway.RequestEvent) {
	client, model := value(event.Client), value(event.Model)
	priority, status := value(string(event.PriorityClass)), statusClass(event.Status)
	backend := value(event.Backend)
	m.requests.WithLabelValues(client, model, priority, status).Inc()
	m.duration.WithLabelValues(model, backend, priority, status).Observe(event.Duration.Seconds())
	if event.Reason != "" {
		m.rejected.WithLabelValues(client, model, priority, value(event.Reason)).Inc()
	}
	if event.TTFT > 0 {
		m.ttft.WithLabelValues(model, backend, priority).Observe(event.TTFT.Seconds())
	}
	if event.Disconnect {
		m.disconnects.WithLabelValues(model, backend, priority).Inc()
	}
	if event.Status >= http.StatusInternalServerError && event.Backend != "" {
		reason := event.Reason
		if reason == "" {
			reason = "upstream_status"
		}
		m.backendFailures.WithLabelValues(model, backend, reason).Inc()
	}
	if event.RetryCount > 0 {
		m.retries.WithLabelValues(model, backend).Add(float64(event.RetryCount))
	}
}

func (m *Metrics) SetBackend(model, backend string, runtime domain.BackendRuntime) {
	labels := []string{value(model), value(backend)}
	m.backendPressure.WithLabelValues(labels...).Set(runtime.Pressure)
	m.backendRunning.WithLabelValues(labels...).Set(runtime.Running)
	m.backendWaiting.WithLabelValues(labels...).Set(runtime.Waiting)
	m.backendKV.WithLabelValues(labels...).Set(runtime.KVCacheUsage)
}

func value(input string) string {
	if input == "" {
		return "unknown"
	}
	return input
}

func statusClass(status int) string {
	if status < 100 || status > 999 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}
