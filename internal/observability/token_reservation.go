package observability

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

type tokenAdmissionMetrics struct {
	estimate, reserved, actual, latency *prometheus.HistogramVec
	unknown, underestimate, exhausted   *prometheus.CounterVec
	accounting                          *prometheus.CounterVec
}

func newTokenAdmissionMetrics(registry *prometheus.Registry) tokenAdmissionMetrics {
	labels := []string{"mode", "priority_class", "estimator"}
	tokens := func(name, help string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: name, Help: help, Buckets: []float64{0, 32, 128, 512, 2048, 8192, 32768, 131072, 1048576},
		}, labels)
	}
	m := tokenAdmissionMetrics{
		estimate: tokens("llmgw_admission_estimated_input_tokens", "Estimated input tokens observed on request completion."),
		reserved: tokens("llmgw_admission_reserved_tokens", "Tokens reserved per admitted request."),
		actual:   tokens("llmgw_admission_actual_tokens", "Actual input plus output tokens for admitted requests with known usage."),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "llmgw_admission_preflight_seconds", Help: "Elapsed input estimation time per request.", Buckets: prometheus.DefBuckets,
		}, labels),
		unknown: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_admission_unknown_usage_total", Help: "Admitted requests that finished without valid upstream usage.",
		}, labels),
		underestimate: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_admission_underestimate_total", Help: "Admitted requests whose actual token usage exceeded the reservation.",
		}, labels),
		accounting: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_admission_reservation_events_total", Help: "Best-effort reservation lifecycle events emitted by the coordinator.",
		}, []string{"mode", "priority_class", "action", "reason", "usage"}),
		exhausted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llmgw_admission_token_budget_rejections_total", Help: "Requests rejected by the outstanding token budget.",
		}, []string{"mode", "priority_class", "scope"}),
	}
	registry.MustRegister(m.estimate, m.reserved, m.actual, m.latency, m.unknown, m.underestimate, m.exhausted, m.accounting)
	return m
}

func (m tokenAdmissionMetrics) observeRequest(event gateway.RequestEvent) {
	token := event.TokenAdmission
	if !token.Enabled {
		return
	}
	mode := bounded(string(token.Mode), "normal", "emergency")
	priority := bounded(string(event.PriorityClass), "critical", "high", "normal", "background")
	kind := bounded(token.EstimatorKind, "upstream", "token_ids", domain.TokenFallbackUTF8Bytes, "mixed")
	if token.Attempted {
		m.latency.WithLabelValues(mode, priority, kind).Observe(token.PreflightDuration.Seconds())
	}
	if token.Estimated {
		m.estimate.WithLabelValues(mode, priority, kind).Observe(float64(token.InputEstimate))
	}
	if event.DecisionReason == gateway.DecisionTokenBudgetExhausted {
		m.exhausted.WithLabelValues(mode, priority, bounded(string(token.BudgetScope), "client", "pool")).Inc()
	}
	if token.ReservedTokens == 0 {
		return
	}
	m.reserved.WithLabelValues(mode, priority, kind).Observe(float64(token.ReservedTokens))
	if !validActualTokenUsage(event.Usage) {
		m.unknown.WithLabelValues(mode, priority, kind).Inc()
		return
	}
	actual := float64(event.Usage.InputTokens) + float64(event.Usage.OutputTokens)
	m.actual.WithLabelValues(mode, priority, kind).Observe(actual)
	if actual > float64(token.ReservedTokens) {
		m.underestimate.WithLabelValues(mode, priority, kind).Inc()
	}
}

func (m *Metrics) TokenReservationChanged(event coordination.TokenReservationEvent) {
	mode := "normal"
	if event.Emergency {
		mode = "emergency"
	}
	priority := bounded(string(event.PriorityClass), "critical", "high", "normal", "background")
	action := bounded(string(event.Action), "acquired", "released", "expired", "reconciled")
	reason := "none"
	if event.Reason != "" {
		reason = bounded(string(event.Reason), string(coordination.ReasonLeaseLost))
	}
	usage := "not_applicable"
	if event.Action == coordination.ReservationReconciled {
		usage = "unknown"
		if event.UsageKnown {
			usage = "known"
		}
	}
	m.tokenAdmission.accounting.WithLabelValues(mode, priority, action, reason, usage).Inc()
}

func validActualTokenUsage(usage *domain.TokenUsage) bool {
	return usage != nil && usage.InputTokens >= 0 && usage.OutputTokens >= 0 &&
		(usage.CacheReadTokens == nil || *usage.CacheReadTokens >= 0 && *usage.CacheReadTokens <= usage.InputTokens)
}

// RegisterOutstandingTokenMetrics attaches the normal and emergency sources
// before serving scrapes. A normal PostgreSQL sample is shared across replicas.
func (m *Metrics) RegisterOutstandingTokenMetrics(normal, emergency coordination.OutstandingTokenReader, enabledPools func() []int64) {
	m.registry.MustRegister(outstandingTokenCollector{
		desc: prometheus.NewDesc("llmgw_admission_outstanding_tokens",
			"Active reservations from the latest coordinator snapshot. PostgreSQL normal samples are shared across replicas.",
			[]string{"pool_id", "mode"}, nil),
		normal: normal, emergency: emergency, enabledPools: enabledPools,
	})
}

type outstandingTokenCollector struct {
	desc              *prometheus.Desc
	normal, emergency coordination.OutstandingTokenReader
	enabledPools      func() []int64
}

func (c outstandingTokenCollector) Describe(out chan<- *prometheus.Desc) { out <- c.desc }

func (c outstandingTokenCollector) Collect(out chan<- prometheus.Metric) {
	pools := c.enabledPools()
	for _, item := range []struct {
		mode   string
		reader coordination.OutstandingTokenReader
	}{
		{"normal", c.normal},
		{"emergency", c.emergency},
	} {
		if item.reader == nil {
			continue
		}
		values := item.reader.OutstandingTokenSnapshot()
		for _, id := range pools {
			out <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(values[id]), strconv.FormatInt(id, 10), item.mode)
			delete(values, id)
		}
		// Existing reservations can outlive a policy disable or pool deletion.
		for id, tokens := range values {
			out <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(tokens), strconv.FormatInt(id, 10), item.mode)
		}
	}
}
