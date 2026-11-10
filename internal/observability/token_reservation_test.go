package observability_test

import (
	"bytes"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"github.com/rislanov/vllm-priority-gateway/internal/observability"
)

func TestTokenReservationTelemetry(t *testing.T) {
	metrics := observability.NewMetrics()
	event := gateway.RequestEvent{
		RequestID: "unbounded-request-identifier", PriorityClass: domain.PriorityHigh, Status: 200,
		TokenAdmission: gateway.TokenAdmissionEvent{
			Enabled: true, Attempted: true, Estimated: true, Mode: gateway.EstimationNormal, EstimatorKind: "upstream",
			InputEstimate: 100, ReservedTokens: 600, PreflightDuration: 4 * time.Millisecond,
		},
	}
	metrics.Complete(event)
	event.Usage = &domain.TokenUsage{InputTokens: 150, OutputTokens: 100}
	metrics.Complete(event)
	event.Usage = &domain.TokenUsage{}
	metrics.Complete(event)
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	output := recorder.Body.String()
	for _, sample := range []string{
		`llmgw_admission_estimated_input_tokens_sum{estimator="upstream",mode="normal",priority_class="high"} 300`,
		`llmgw_admission_reserved_tokens_sum{estimator="upstream",mode="normal",priority_class="high"} 1800`,
		`llmgw_admission_actual_tokens_sum{estimator="upstream",mode="normal",priority_class="high"} 250`,
		`llmgw_admission_actual_tokens_count{estimator="upstream",mode="normal",priority_class="high"} 2`,
		`llmgw_admission_unknown_usage_total{estimator="upstream",mode="normal",priority_class="high"} 1`,
	} {
		if !strings.Contains(output, sample) {
			t.Errorf("missing token metric %q", sample)
		}
	}
	if strings.Contains(output, "unbounded-request-identifier") {
		t.Fatal("request ID became a metric label")
	}
}

func TestTokenReservationLoggerDistinguishesUnknownUsage(t *testing.T) {
	var logs bytes.Buffer
	logger := observability.NewLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	event := gateway.RequestEvent{TokenAdmission: gateway.TokenAdmissionEvent{
		Enabled: true, Estimated: true, InputEstimate: 100, ReservedTokens: 600, EstimatorKind: "upstream",
	}}
	logger.Complete(event)
	if output := logs.String(); !strings.Contains(output, `"usageKnown":false`) || strings.Contains(output, `"actualInputTokens"`) {
		t.Fatalf("missing usage was not represented distinctly: %s", output)
	}
}

type outstandingSnapshot map[int64]int64

func (s outstandingSnapshot) OutstandingTokenSnapshot() map[int64]int64 { return maps.Clone(s) }

func TestOutstandingTokenGaugeKeepsCoordinationModesSeparate(t *testing.T) {
	metrics := observability.NewMetrics()
	metrics.RegisterOutstandingTokenMetrics(outstandingSnapshot{7: 600}, outstandingSnapshot{7: 100}, func() []int64 { return []int64{7, 9} })
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, sample := range []string{
		`llmgw_admission_outstanding_tokens{mode="normal",pool_id="7"} 600`,
		`llmgw_admission_outstanding_tokens{mode="emergency",pool_id="7"} 100`,
		`llmgw_admission_outstanding_tokens{mode="normal",pool_id="9"} 0`,
	} {
		if !strings.Contains(recorder.Body.String(), sample) {
			t.Errorf("missing gauge sample %q", sample)
		}
	}
}
