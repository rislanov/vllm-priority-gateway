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

func TestSelectWithSessionAffinityIsStableAcrossCandidateOrder(t *testing.T) {
	router := routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0))
	forward := []routing.Candidate{
		candidate(11, .3, 0), candidate(22, .3, 0), candidate(33, .3, 0),
	}
	reversed := []routing.Candidate{forward[2], forward[1], forward[0]}

	first, err := router.SelectWithSessionAffinity(forward, nil, "client=1\x00pool=10\x00session=alpha")
	if err != nil {
		t.Fatal(err)
	}
	second, err := router.SelectWithSessionAffinity(reversed, nil, "client=1\x00pool=10\x00session=alpha")
	if err != nil {
		t.Fatal(err)
	}
	if first.Backend.ID != second.Backend.ID {
		t.Fatalf("stable session mapped to %d then %d", first.Backend.ID, second.Backend.ID)
	}
}

func TestSelectWithSessionAffinityFallsBackWhenPreferredIsOverloaded(t *testing.T) {
	router := routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0))
	candidates := []routing.Candidate{candidate(11, .3, 0), candidate(22, .3, 0), candidate(33, .3, 0)}
	key := "client=1\x00pool=10\x00session=alpha"

	preferred, err := router.SelectWithSessionAffinity(candidates, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	for index := range candidates {
		if candidates[index].Backend.ID == preferred.Backend.ID {
			candidates[index].Pressure = 1.1
		} else {
			candidates[index].Pressure = .2
		}
	}

	selected, err := router.SelectWithSessionAffinity(candidates, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Backend.ID == preferred.Backend.ID {
		t.Fatalf("overloaded preferred backend %d was selected", preferred.Backend.ID)
	}
	if selected.Pressure != .2 {
		t.Fatalf("fallback pressure = %.2f, want .2", selected.Pressure)
	}
}

func TestSelectWithSessionAffinityHonorsEligibilityAndRetryExclusion(t *testing.T) {
	router := routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0))
	candidates := []routing.Candidate{candidate(11, .3, 0), candidate(22, .3, 0), candidate(33, .3, 0)}
	key := "client=1\x00pool=10\x00session=alpha"

	first, err := router.SelectWithSessionAffinity(candidates, nil, key)
	if err != nil {
		t.Fatal(err)
	}
	second, err := router.SelectWithSessionAffinity(candidates, map[int64]struct{}{first.Backend.ID: {}}, key)
	if err != nil {
		t.Fatal(err)
	}
	if second.Backend.ID == first.Backend.ID {
		t.Fatalf("excluded backend %d was selected again", first.Backend.ID)
	}

	for index := range candidates {
		if candidates[index].Backend.ID == second.Backend.ID {
			candidates[index].Eligible = false
		}
	}
	third, err := router.SelectWithSessionAffinity(candidates, map[int64]struct{}{first.Backend.ID: {}}, key)
	if err != nil {
		t.Fatal(err)
	}
	if third.Backend.ID == second.Backend.ID {
		t.Fatalf("ineligible backend %d was selected", second.Backend.ID)
	}
}

func TestSelectWithSessionAffinityRehashesAcrossEligibleBackendsBeforePressureFallback(t *testing.T) {
	router := routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0))
	candidates := []routing.Candidate{
		candidate(11, .1, 0),
		candidate(22, .8, 0),
		candidate(33, .2, 0),
	}
	candidates[2].Eligible = false

	selected, err := router.SelectWithSessionAffinity(candidates, nil, "client=1\x00pool=10\x00session=alpha")
	if err != nil {
		t.Fatal(err)
	}
	if selected.Backend.ID != 22 {
		t.Fatalf("backend = %d, want next eligible rendezvous backend 22", selected.Backend.ID)
	}
}

func TestSelectWithEmptySessionAffinityUsesLeastPressure(t *testing.T) {
	router := routing.NewWithSessionAffinity(.02, 1, routing.FixedSource(0))
	selected, err := router.SelectWithSessionAffinity([]routing.Candidate{
		candidate(11, .8, 0), candidate(22, .2, 0),
	}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if selected.Backend.ID != 22 {
		t.Fatalf("backend = %d, want least-pressure backend 22", selected.Backend.ID)
	}
}
