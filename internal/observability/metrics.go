package observability

import (
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

// Metrics owns an isolated Prometheus registry and implements gateway.Observer.
type Metrics struct {
	registry               *prometheus.Registry
	requests               *prometheus.CounterVec
	requestsInflight       *prometheus.GaugeVec
	rejected               *prometheus.CounterVec
	clientInflight         *prometheus.GaugeVec
	backendInflight        *prometheus.GaugeVec
	backendPressure        *prometheus.GaugeVec
	backendRunning         *prometheus.GaugeVec
	backendWaiting         *prometheus.GaugeVec
	backendKV              *prometheus.GaugeVec
	backendCircuitState    *prometheus.GaugeVec
	backendCircuitFailures *prometheus.GaugeVec
	poolGatewayInflight    *prometheus.GaugeVec
	poolWaiting            *prometheus.GaugeVec
	poolAvailableBackends  *prometheus.GaugeVec
	duration               *prometheus.HistogramVec
	ttft                   *prometheus.HistogramVec
	disconnects            *prometheus.CounterVec
	backendFailures        *prometheus.CounterVec
	retries                *prometheus.CounterVec
	inputTokens            *prometheus.CounterVec
	outputTokens           *prometheus.CounterVec
	cacheReadTokens        *prometheus.CounterVec
	usageParseFails        *prometheus.CounterVec
	usagePersistFail       prometheus.Counter
	runtimeMu              sync.Mutex
	runtimeTopologyKnown   bool
	knownPoolLabels        map[string]struct{}
	knownBackendLabels     map[backendMetricLabels]struct{}
	knownRequestLabels     map[requestMetricLabels]struct{}
	knownClientLabels      map[clientMetricLabels]struct{}
	observedBackendLabels  map[backendMetricLabels]struct{}
	observedRequestLabels  map[requestMetricLabels]struct{}
	observedClientLabels   map[clientMetricLabels]struct{}
	backendInflightValues  map[backendMetricLabels]int
	requestInflightValues  map[requestMetricLabels]int
	clientInflightValues   map[clientMetricLabels]int
}

type backendMetricLabels struct {
	model   string
	backend string
}

type requestMetricLabels struct {
	model    string
	priority string
}

type clientMetricLabels struct {
	client   string
	model    string
	priority string
}

// PoolRuntimeMetric is one current-topology pool runtime sample.
type PoolRuntimeMetric struct {
	Model   string
	Runtime domain.PoolRuntime
}

// BackendRuntimeMetric is one current-topology backend runtime sample.
type BackendRuntimeMetric struct {
	Model   string
	Backend string
	Runtime domain.BackendRuntime
}

// InflightRuntimeLabels is one enabled client/model/priority admission label combination.
type InflightRuntimeLabels struct {
	Client        string
	Model         string
	PriorityClass domain.PriorityClass
}

func NewMetrics() *Metrics {
	m := &Metrics{
		registry:              prometheus.NewRegistry(),
		knownPoolLabels:       make(map[string]struct{}),
		knownBackendLabels:    make(map[backendMetricLabels]struct{}),
		knownRequestLabels:    make(map[requestMetricLabels]struct{}),
		knownClientLabels:     make(map[clientMetricLabels]struct{}),
		observedBackendLabels: make(map[backendMetricLabels]struct{}),
		observedRequestLabels: make(map[requestMetricLabels]struct{}),
		observedClientLabels:  make(map[clientMetricLabels]struct{}),
		backendInflightValues: make(map[backendMetricLabels]int),
		requestInflightValues: make(map[requestMetricLabels]int),
		clientInflightValues:  make(map[clientMetricLabels]int),
	}
	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_requests_total", Help: "Completed public inference requests."}, []string{"client", "model", "priority_class", "status_class"})
	m.requestsInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_requests_inflight", Help: "Public inference requests admitted and currently in flight."}, []string{"model", "priority_class"})
	m.rejected = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_requests_rejected_total", Help: "Requests rejected by the gateway."}, []string{"client", "model", "priority_class", "reason"})
	m.clientInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_client_inflight", Help: "Admitted requests currently in flight by client."}, []string{"client", "model", "priority_class"})
	m.backendInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_requests_inflight", Help: "Gateway requests currently assigned to a backend."}, []string{"model", "backend"})
	m.backendPressure = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_pressure", Help: "Smoothed backend pressure."}, []string{"model", "backend"})
	m.backendRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_running_requests", Help: "Running requests reported by vLLM."}, []string{"model", "backend"})
	m.backendWaiting = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_waiting_requests", Help: "Waiting requests reported by vLLM."}, []string{"model", "backend"})
	m.backendKV = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_kv_cache_usage", Help: "KV cache utilization reported by vLLM."}, []string{"model", "backend"})
	m.backendCircuitState = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_circuit_state", Help: "Backend circuit state (unmanaged/unknown=-1, closed=0, open=1, half_open=2)."}, []string{"model", "backend"})
	m.backendCircuitFailures = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_backend_circuit_failures", Help: "Qualifying failures retained by the backend circuit."}, []string{"model", "backend"})
	m.poolGatewayInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_pool_gateway_inflight", Help: "Gateway requests currently holding a pool lease."}, []string{"model"})
	m.poolWaiting = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_pool_waiting_requests", Help: "Aggregate waiting requests reported by healthy, metrics-fresh, non-draining pool backends."}, []string{"model"})
	m.poolAvailableBackends = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "llmgw_pool_available_backends", Help: "Backends currently available to accept inference traffic."}, []string{"model"})
	m.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_request_duration_seconds", Help: "End-to-end public request duration.", Buckets: prometheus.DefBuckets}, []string{"model", "backend", "priority_class", "status_class"})
	m.ttft = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_ttft_seconds", Help: "Time to first upstream response byte.", Buckets: prometheus.DefBuckets}, []string{"model", "backend", "priority_class"})
	m.disconnects = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_stream_disconnects_total", Help: "Streaming requests cancelled by a disconnected downstream."}, []string{"model", "backend", "priority_class"})
	m.backendFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_backend_failures_total", Help: "Requests that failed at a selected backend."}, []string{"model", "backend", "reason"})
	m.retries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_retries_total", Help: "Transparent pre-response retries."}, []string{"model", "backend"})
	m.inputTokens = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_input_tokens_total", Help: "Input tokens reported by upstream inference responses."}, []string{"client", "model"})
	m.outputTokens = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_output_tokens_total", Help: "Output tokens reported by upstream inference responses."}, []string{"client", "model"})
	m.cacheReadTokens = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_cache_read_tokens_total", Help: "Cache-read tokens reported by upstream inference responses."}, []string{"client", "model"})
	m.usageParseFails = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_usage_parse_failures_total", Help: "Upstream usage metadata parse failures."}, []string{"format"})
	m.usagePersistFail = prometheus.NewCounter(prometheus.CounterOpts{Name: "llmgw_usage_persistence_failures_total", Help: "Usage records that failed to persist."})
	m.registry.MustRegister(
		m.requests, m.requestsInflight, m.rejected, m.clientInflight, m.backendInflight,
		m.backendPressure, m.backendRunning, m.backendWaiting, m.backendKV, m.duration,
		m.backendCircuitState, m.backendCircuitFailures, m.poolGatewayInflight, m.poolWaiting,
		m.poolAvailableBackends, m.ttft, m.disconnects, m.backendFailures, m.retries,
		m.inputTokens, m.outputTokens, m.cacheReadTokens, m.usageParseFails, m.usagePersistFail,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ClientInflight(event gateway.InflightEvent, delta int) {
	requestLabels := requestMetricLabels{model: value(event.Model), priority: value(string(event.PriorityClass))}
	clientLabels := clientMetricLabels{
		client: value(event.Client), model: requestLabels.model, priority: requestLabels.priority,
	}
	m.runtimeMu.Lock()
	defer m.runtimeMu.Unlock()

	m.observedRequestLabels[requestLabels] = struct{}{}
	m.observedClientLabels[clientLabels] = struct{}{}
	requestInflight := updateInflightValue(m.requestInflightValues, requestLabels, delta)
	clientInflight := updateInflightValue(m.clientInflightValues, clientLabels, delta)
	if m.runtimeTopologyKnown {
		if _, current := m.knownRequestLabels[requestLabels]; !current {
			m.requestsInflight.DeleteLabelValues(requestLabels.model, requestLabels.priority)
			if requestInflight == 0 {
				delete(m.observedRequestLabels, requestLabels)
			}
		} else {
			m.requestsInflight.WithLabelValues(requestLabels.model, requestLabels.priority).Set(float64(requestInflight))
		}
		if _, current := m.knownClientLabels[clientLabels]; !current {
			m.clientInflight.DeleteLabelValues(clientLabels.client, clientLabels.model, clientLabels.priority)
			if clientInflight == 0 {
				delete(m.observedClientLabels, clientLabels)
			}
		} else {
			m.clientInflight.WithLabelValues(clientLabels.client, clientLabels.model, clientLabels.priority).Set(float64(clientInflight))
		}
		return
	}
	m.requestsInflight.WithLabelValues(requestLabels.model, requestLabels.priority).Set(float64(requestInflight))
	m.clientInflight.WithLabelValues(clientLabels.client, clientLabels.model, clientLabels.priority).Set(float64(clientInflight))
}

func (m *Metrics) BackendInflight(event gateway.InflightEvent, delta int) {
	labels := backendMetricLabels{model: value(event.Model), backend: value(event.Backend)}
	m.runtimeMu.Lock()
	defer m.runtimeMu.Unlock()

	m.observedBackendLabels[labels] = struct{}{}
	inflight := updateInflightValue(m.backendInflightValues, labels, delta)
	if m.runtimeTopologyKnown {
		if _, current := m.knownBackendLabels[labels]; !current {
			m.backendInflight.DeleteLabelValues(labels.model, labels.backend)
			if inflight == 0 {
				delete(m.observedBackendLabels, labels)
			}
			return
		}
	}
	m.backendInflight.WithLabelValues(labels.model, labels.backend).Set(float64(inflight))
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
	if event.Usage != nil {
		m.inputTokens.WithLabelValues(client, model).Add(float64(event.Usage.InputTokens))
		m.outputTokens.WithLabelValues(client, model).Add(float64(event.Usage.OutputTokens))
		if event.Usage.CacheReadTokens != nil {
			m.cacheReadTokens.WithLabelValues(client, model).Add(float64(*event.Usage.CacheReadTokens))
		}
	}
	if format, ok := usageParseFailureFormat(event.UsageParseFailure); ok {
		m.usageParseFails.WithLabelValues(format).Inc()
	}
}

// UsagePersistenceFailure records a usage row that could not be persisted.
// Prometheus counters are safe for concurrent use by recorder workers.
func (m *Metrics) UsagePersistenceFailure() {
	m.usagePersistFail.Inc()
}

func (m *Metrics) SetBackend(model, backend string, runtime domain.BackendRuntime) {
	m.runtimeMu.Lock()
	defer m.runtimeMu.Unlock()
	m.setBackend(model, backend, runtime)
}

func (m *Metrics) setBackend(model, backend string, runtime domain.BackendRuntime) {
	labels := []string{value(model), value(backend)}
	m.backendPressure.WithLabelValues(labels...).Set(runtime.Pressure)
	m.backendRunning.WithLabelValues(labels...).Set(runtime.Running)
	m.backendWaiting.WithLabelValues(labels...).Set(runtime.Waiting)
	m.backendKV.WithLabelValues(labels...).Set(runtime.KVCacheUsage)
	m.backendCircuitState.WithLabelValues(labels...).Set(circuitStateValue(runtime.CircuitState))
	m.backendCircuitFailures.WithLabelValues(labels...).Set(float64(runtime.CircuitFailures))
}

func (m *Metrics) SetPool(model string, runtime domain.PoolRuntime) {
	m.runtimeMu.Lock()
	defer m.runtimeMu.Unlock()
	m.setPool(model, runtime)
}

func (m *Metrics) setPool(model string, runtime domain.PoolRuntime) {
	label := value(model)
	m.poolGatewayInflight.WithLabelValues(label).Set(float64(runtime.GatewayInflight))
	m.poolWaiting.WithLabelValues(label).Set(runtime.TotalWaiting)
	m.poolAvailableBackends.WithLabelValues(label).Set(float64(runtime.AvailableBackends))
}

// PublishRuntime replaces the bounded current-topology gauge label set and
// publishes its latest samples atomically with respect to inflight updates.
// Historical counters and histograms are intentionally left untouched.
func (m *Metrics) PublishRuntime(pools []PoolRuntimeMetric, backends []BackendRuntimeMetric, inflight []InflightRuntimeLabels) {
	currentPools := make(map[string]struct{}, len(pools))
	currentBackends := make(map[backendMetricLabels]struct{}, len(backends))
	currentRequests := make(map[requestMetricLabels]struct{}, len(inflight))
	currentClients := make(map[clientMetricLabels]struct{}, len(inflight))
	for _, pool := range pools {
		currentPools[value(pool.Model)] = struct{}{}
	}
	for _, backend := range backends {
		currentBackends[backendMetricLabels{
			model: value(backend.Model), backend: value(backend.Backend),
		}] = struct{}{}
	}
	for _, labels := range inflight {
		requestLabels := requestMetricLabels{
			model: value(labels.Model), priority: value(string(labels.PriorityClass)),
		}
		currentRequests[requestLabels] = struct{}{}
		currentClients[clientMetricLabels{
			client: value(labels.Client), model: requestLabels.model, priority: requestLabels.priority,
		}] = struct{}{}
	}

	m.runtimeMu.Lock()
	defer m.runtimeMu.Unlock()

	for labels := range m.knownPoolLabels {
		if _, current := currentPools[labels]; current {
			continue
		}
		m.poolGatewayInflight.DeleteLabelValues(labels)
		m.poolWaiting.DeleteLabelValues(labels)
		m.poolAvailableBackends.DeleteLabelValues(labels)
	}
	for labels := range m.knownBackendLabels {
		if _, current := currentBackends[labels]; current {
			continue
		}
		m.deleteBackendRuntimeLabels(labels)
	}
	for labels := range m.observedBackendLabels {
		if _, current := currentBackends[labels]; !current {
			m.backendInflight.DeleteLabelValues(labels.model, labels.backend)
			if m.backendInflightValues[labels] == 0 {
				delete(m.observedBackendLabels, labels)
			}
		}
	}
	for labels := range m.knownRequestLabels {
		if _, current := currentRequests[labels]; !current {
			m.requestsInflight.DeleteLabelValues(labels.model, labels.priority)
		}
	}
	for labels := range m.observedRequestLabels {
		if _, current := currentRequests[labels]; !current {
			m.requestsInflight.DeleteLabelValues(labels.model, labels.priority)
			if m.requestInflightValues[labels] == 0 {
				delete(m.observedRequestLabels, labels)
			}
		}
	}
	for labels := range m.knownClientLabels {
		if _, current := currentClients[labels]; !current {
			m.clientInflight.DeleteLabelValues(labels.client, labels.model, labels.priority)
		}
	}
	for labels := range m.observedClientLabels {
		if _, current := currentClients[labels]; !current {
			m.clientInflight.DeleteLabelValues(labels.client, labels.model, labels.priority)
			if m.clientInflightValues[labels] == 0 {
				delete(m.observedClientLabels, labels)
			}
		}
	}

	m.runtimeTopologyKnown = true
	m.knownPoolLabels = currentPools
	m.knownBackendLabels = currentBackends
	m.knownRequestLabels = currentRequests
	m.knownClientLabels = currentClients
	for _, pool := range pools {
		m.setPool(pool.Model, pool.Runtime)
	}
	for _, backend := range backends {
		m.setBackend(backend.Model, backend.Backend, backend.Runtime)
		labels := backendMetricLabels{model: value(backend.Model), backend: value(backend.Backend)}
		m.backendInflight.WithLabelValues(labels.model, labels.backend).Set(float64(m.backendInflightValues[labels]))
	}
	for labels := range currentRequests {
		m.requestsInflight.WithLabelValues(labels.model, labels.priority).Set(float64(m.requestInflightValues[labels]))
	}
	for labels := range currentClients {
		m.clientInflight.WithLabelValues(labels.client, labels.model, labels.priority).Set(float64(m.clientInflightValues[labels]))
	}
}

func updateInflightValue[K comparable](values map[K]int, labels K, delta int) int {
	inflight := values[labels] + delta
	if inflight <= 0 {
		delete(values, labels)
		return 0
	}
	values[labels] = inflight
	return inflight
}

func (m *Metrics) deleteBackendRuntimeLabels(labels backendMetricLabels) {
	m.backendInflight.DeleteLabelValues(labels.model, labels.backend)
	m.backendPressure.DeleteLabelValues(labels.model, labels.backend)
	m.backendRunning.DeleteLabelValues(labels.model, labels.backend)
	m.backendWaiting.DeleteLabelValues(labels.model, labels.backend)
	m.backendKV.DeleteLabelValues(labels.model, labels.backend)
	m.backendCircuitState.DeleteLabelValues(labels.model, labels.backend)
	m.backendCircuitFailures.DeleteLabelValues(labels.model, labels.backend)
}

func circuitStateValue(state domain.CircuitState) float64 {
	switch state {
	case domain.CircuitClosed:
		return 0
	case domain.CircuitOpen:
		return 1
	case domain.CircuitHalfOpen:
		return 2
	default:
		return -1
	}
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

func usageParseFailureFormat(input string) (string, bool) {
	format := strings.ToLower(strings.TrimSpace(input))
	if format == "" {
		return "", false
	}
	if format == "json" || format == "sse" {
		return format, true
	}
	return "", false
}
