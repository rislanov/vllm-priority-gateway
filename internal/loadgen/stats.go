package loadgen

import (
	"math"
	"sort"
	"time"
)

type DurationSummary struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

func Summarize(values []time.Duration) DurationSummary {
	if len(values) == 0 {
		return DurationSummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return DurationSummary{
		Count: len(sorted), Min: sorted[0], P50: nearestRank(sorted, .50),
		P95: nearestRank(sorted, .95), P99: nearestRank(sorted, .99), Max: sorted[len(sorted)-1],
	}
}

func nearestRank(sorted []time.Duration, percentile float64) time.Duration {
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
