package monitor_test

import (
	"strings"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/monitor"
)

func TestParseMetricsSumsSamplesAndUsesCurrentKVName(t *testing.T) {
	fixture := `
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="a"} 3
vllm:num_requests_running{model_name="b"} 4
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="a"} 1
vllm:num_requests_waiting{model_name="b"} 2
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{model_name="a"} 0.25
vllm:kv_cache_usage_perc{model_name="b"} 0.5
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc{model_name="legacy"} 0.9
`
	metrics, err := monitor.ParseMetrics(strings.NewReader(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Running != 7 || metrics.Waiting != 3 || metrics.KVCacheUsage != .75 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestParseMetricsFallsBackToLegacyKVName(t *testing.T) {
	fixture := `
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 1
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting 2
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc 0.6
`
	metrics, err := monitor.ParseMetrics(strings.NewReader(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if metrics.KVCacheUsage != .6 {
		t.Fatalf("KVCacheUsage = %v", metrics.KVCacheUsage)
	}
}

func TestParseMetricsRejectsMissingOrInvalidRequiredMetric(t *testing.T) {
	fixtures := []string{
		`vllm:num_requests_running 1
vllm:kv_cache_usage_perc 0.5
`,
		`vllm:num_requests_running NaN
vllm:num_requests_waiting 0
vllm:kv_cache_usage_perc 0.5
`,
	}
	for _, fixture := range fixtures {
		if _, err := monitor.ParseMetrics(strings.NewReader(fixture)); err == nil {
			t.Fatalf("ParseMetrics(%q) unexpectedly succeeded", fixture)
		}
	}
}
