package pressure_test

import (
	"math"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
)

func TestCalculateUsesSpecifiedWeights(t *testing.T) {
	got := pressure.Calculate(
		pressure.Sample{Running: 8, Waiting: 2, KVUsage: .875},
		pressure.Limits{QueueSoft: 2, KVSoft: .80, KVHard: .95, RunningSoft: 8},
	)
	const want = .85
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("Calculate() = %v, want %v", got, want)
	}
}

func TestCalculateClampsEachComponentAtTwo(t *testing.T) {
	got := pressure.Calculate(
		pressure.Sample{Running: 100, Waiting: 100, KVUsage: 1.5},
		pressure.Limits{QueueSoft: 2, KVSoft: .80, KVHard: .95, RunningSoft: 8},
	)
	if got != 2 {
		t.Fatalf("Calculate() = %v, want 2", got)
	}
}

func TestLimitsValidateRejectsInvalidValues(t *testing.T) {
	tests := []pressure.Limits{
		{QueueSoft: 0, KVSoft: .8, KVHard: .95, RunningSoft: 8},
		{QueueSoft: 2, KVSoft: .95, KVHard: .8, RunningSoft: 8},
		{QueueSoft: 2, KVSoft: .8, KVHard: .95, RunningSoft: 0},
	}
	for _, limits := range tests {
		if err := limits.Validate(); err == nil {
			t.Fatalf("Validate(%+v) unexpectedly succeeded", limits)
		}
	}
}

func TestEWMAUsesElapsedTime(t *testing.T) {
	average := pressure.NewEWMA(4 * time.Second)
	start := time.Unix(100, 0)
	if got := average.Add(start, 0); got != 0 {
		t.Fatalf("first Add() = %v", got)
	}
	got := average.Add(start.Add(4*time.Second), 1)
	want := 1 - math.Exp(-1)
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("second Add() = %v, want %v", got, want)
	}
}

func TestEWMAIgnoresOutOfOrderSample(t *testing.T) {
	average := pressure.NewEWMA(time.Second)
	start := time.Unix(100, 0)
	average.Add(start, .25)
	if got := average.Add(start.Add(-time.Second), 1); got != .25 {
		t.Fatalf("out-of-order Add() = %v, want .25", got)
	}
}
