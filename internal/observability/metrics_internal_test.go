package observability

import (
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestRuntimeLabelSetsDeduplicateTopology(t *testing.T) {
	sets := makeRuntimeLabelSets(
		[]PoolRuntimeMetric{{Model: "model"}, {Model: "model"}},
		[]BackendRuntimeMetric{{Model: "model", Backend: "backend"}, {Model: "model", Backend: "backend"}},
		[]InflightRuntimeLabels{{Client: "client", Model: "model", PriorityClass: domain.PriorityNormal}},
	)
	if len(sets.pools) != 1 || len(sets.backends) != 1 || len(sets.requests) != 1 || len(sets.clients) != 1 {
		t.Fatalf("runtime label cardinalities = %d/%d/%d/%d", len(sets.pools), len(sets.backends), len(sets.requests), len(sets.clients))
	}
}
