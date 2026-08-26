package monitor

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const (
	runningMetric  = "vllm:num_requests_running"
	waitingMetric  = "vllm:num_requests_waiting"
	kvMetric       = "vllm:kv_cache_usage_perc"
	legacyKVMetric = "vllm:gpu_cache_usage_perc"
)

type Metrics struct {
	Running      float64
	Waiting      float64
	KVCacheUsage float64
}

func ParseMetrics(reader io.Reader) (Metrics, error) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(reader)
	if err != nil {
		return Metrics{}, fmt.Errorf("parse Prometheus metrics: %w", err)
	}
	running, err := sumFamily(families[runningMetric], runningMetric)
	if err != nil {
		return Metrics{}, err
	}
	waiting, err := sumFamily(families[waitingMetric], waitingMetric)
	if err != nil {
		return Metrics{}, err
	}
	kvFamily := families[kvMetric]
	kvName := kvMetric
	if kvFamily == nil {
		kvFamily = families[legacyKVMetric]
		kvName = legacyKVMetric
	}
	kv, err := sumFamily(kvFamily, kvName)
	if err != nil {
		return Metrics{}, err
	}
	return Metrics{Running: running, Waiting: waiting, KVCacheUsage: kv}, nil
}

func sumFamily(family *io_prometheus_client.MetricFamily, name string) (float64, error) {
	if family == nil || len(family.Metric) == 0 {
		return 0, fmt.Errorf("required metric %s is missing", name)
	}
	var sum float64
	for _, metric := range family.Metric {
		var value float64
		switch family.GetType() {
		case io_prometheus_client.MetricType_GAUGE:
			if metric.Gauge == nil {
				return 0, fmt.Errorf("metric %s has no gauge value", name)
			}
			value = metric.Gauge.GetValue()
		case io_prometheus_client.MetricType_UNTYPED:
			if metric.Untyped == nil {
				return 0, fmt.Errorf("metric %s has no untyped value", name)
			}
			value = metric.Untyped.GetValue()
		default:
			return 0, fmt.Errorf("metric %s must be a gauge", name)
		}
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return 0, fmt.Errorf("metric %s contains invalid value %v", name, value)
		}
		sum += value
	}
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		return 0, errors.New("metric sum is not finite")
	}
	return sum, nil
}
