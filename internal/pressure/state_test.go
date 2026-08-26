package pressure_test

import (
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/pressure"
)

func testThresholds() pressure.Thresholds {
	return pressure.Thresholds{
		Busy: .70, Saturated: 1, Emergency: 1.40,
		BusyRecovery: .55, SaturatedRecovery: .85, EmergencyRecovery: 1.20,
		EnterWindow: 3 * time.Second, RecoveryWindow: 10 * time.Second,
	}
}

func TestPoolMachineIgnoresShortBusySpike(t *testing.T) {
	machine := pressure.NewPoolMachine(testThresholds())
	start := time.Unix(100, 0)
	if got := machine.Observe(start, .8, false, true); got != domain.PoolNormal {
		t.Fatalf("first state = %s", got)
	}
	if got := machine.Observe(start.Add(2*time.Second), .8, false, true); got != domain.PoolNormal {
		t.Fatalf("spike state = %s", got)
	}
	if got := machine.Observe(start.Add(2500*time.Millisecond), .4, false, true); got != domain.PoolNormal {
		t.Fatalf("recovered state = %s", got)
	}
}

func TestPoolMachineEntersBusyAfterPersistenceWindow(t *testing.T) {
	machine := pressure.NewPoolMachine(testThresholds())
	start := time.Unix(100, 0)
	machine.Observe(start, .8, false, true)
	if got := machine.Observe(start.Add(3*time.Second), .8, false, true); got != domain.PoolBusy {
		t.Fatalf("state = %s, want busy", got)
	}
}

func TestPoolMachineAllWaitingEntersSaturated(t *testing.T) {
	machine := pressure.NewPoolMachine(testThresholds())
	start := time.Unix(100, 0)
	machine.Observe(start, .2, true, true)
	if got := machine.Observe(start.Add(3*time.Second), .2, true, true); got != domain.PoolSaturated {
		t.Fatalf("state = %s, want saturated", got)
	}
}

func TestPoolMachineEntersEmergencyAndRecoversOneLevelAtATime(t *testing.T) {
	machine := pressure.NewPoolMachine(testThresholds())
	start := time.Unix(100, 0)
	machine.Observe(start, 1.5, false, true)
	if got := machine.Observe(start.Add(3*time.Second), 1.5, false, true); got != domain.PoolEmergency {
		t.Fatalf("state = %s, want emergency", got)
	}
	machine.Observe(start.Add(4*time.Second), .4, false, true)
	if got := machine.Observe(start.Add(14*time.Second), .4, false, true); got != domain.PoolSaturated {
		t.Fatalf("first recovery = %s, want saturated", got)
	}
	machine.Observe(start.Add(15*time.Second), .4, false, true)
	if got := machine.Observe(start.Add(25*time.Second), .4, false, true); got != domain.PoolBusy {
		t.Fatalf("second recovery = %s, want busy", got)
	}
	machine.Observe(start.Add(26*time.Second), .4, false, true)
	if got := machine.Observe(start.Add(36*time.Second), .4, false, true); got != domain.PoolNormal {
		t.Fatalf("third recovery = %s, want normal", got)
	}
}

func TestPoolMachineUnavailableIsImmediateAndRecoveryDerivesCurrentLoad(t *testing.T) {
	machine := pressure.NewPoolMachine(testThresholds())
	start := time.Unix(100, 0)
	if got := machine.Observe(start, 0, false, false); got != domain.PoolUnavailable {
		t.Fatalf("unavailable state = %s", got)
	}
	if got := machine.Observe(start.Add(time.Second), 1.1, false, true); got != domain.PoolSaturated {
		t.Fatalf("recovered state = %s, want saturated", got)
	}
}
