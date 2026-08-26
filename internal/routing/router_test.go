package routing_test

import (
	"errors"
	"math"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/routing"
)

func candidate(id int64, pressure float64, inflight int) routing.Candidate {
	return routing.Candidate{
		Backend:  domain.Backend{ID: id, Enabled: true, Name: "backend"},
		Pressure: pressure, GatewayInflight: inflight, Eligible: true,
	}
}

func TestSelectChoosesLeastPressure(t *testing.T) {
	router := routing.New(.02, routing.FixedSource(0))
	got, err := router.Select([]routing.Candidate{candidate(1, .30, 1), candidate(2, 1.10, 0)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend.ID != 1 {
		t.Fatalf("backend = %d, want 1", got.Backend.ID)
	}
}

func TestSelectUsesInflightWithinPressureEpsilon(t *testing.T) {
	router := routing.New(.02, routing.FixedSource(0))
	got, err := router.Select([]routing.Candidate{candidate(1, .30, 3), candidate(2, .31, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend.ID != 2 {
		t.Fatalf("backend = %d, want 2", got.Backend.ID)
	}
}

func TestSelectUsesInjectedTieBreakAndExclusion(t *testing.T) {
	router := routing.New(.02, routing.FixedSource(1))
	got, err := router.Select([]routing.Candidate{candidate(1, .3, 1), candidate(2, .3, 1), candidate(3, .3, 1)}, map[int64]struct{}{1: {}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend.ID != 3 {
		t.Fatalf("backend = %d, want 3", got.Backend.ID)
	}
}

func TestSelectFiltersIneligibleDisabledDrainingAndInvalidPressure(t *testing.T) {
	values := []routing.Candidate{
		candidate(1, .1, 0),
		candidate(2, .2, 0),
		candidate(3, .3, 0),
		candidate(4, math.NaN(), 0),
		candidate(5, .5, 0),
	}
	values[0].Eligible = false
	values[1].Backend.Enabled = false
	values[2].Backend.Draining = true
	values[4].Backend.Enabled = true
	router := routing.New(.02, routing.FixedSource(0))
	got, err := router.Select(values, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend.ID != 5 {
		t.Fatalf("backend = %d, want 5", got.Backend.ID)
	}
}

func TestSelectReturnsTypedErrorWithoutCandidates(t *testing.T) {
	router := routing.New(.02, routing.FixedSource(0))
	if _, err := router.Select(nil, nil); !errors.Is(err, routing.ErrNoBackend) {
		t.Fatalf("Select() error = %v", err)
	}
}
