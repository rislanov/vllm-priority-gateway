package coordination_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

type recoveryObserverStub struct {
	transitions [][2]coordination.RecoveryState
}

func (o *recoveryObserverStub) CoordinationRecoveryTransition(from, to coordination.RecoveryState) {
	o.transitions = append(o.transitions, [2]coordination.RecoveryState{from, to})
}

func TestRecoveryManagerRequiresEveryStepInContractOrder(t *testing.T) {
	var calls []string
	step := func(name string) func(context.Context) error {
		return func(context.Context) error {
			calls = append(calls, name)
			return nil
		}
	}
	at := time.Unix(1700000000, 0)
	manager := coordination.NewRecoveryManager(coordination.RecoverySteps{
		Ping: step("ping"), CheckFingerprint: step("fingerprint"), ReloadConfiguration: step("configuration"),
		ReconcileLeases: step("leases"), ReplayFailures: step("failures"), RefreshCircuits: step("circuits"),
	}, func() time.Time { return at })
	manager.MarkDegraded(errors.New("database unavailable"))

	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"ping", "fingerprint", "configuration", "leases", "failures", "circuits"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("recovery order = %v, want %v", calls, want)
	}
	status := manager.Status()
	if status.State != coordination.RecoveryReady || !status.LastSuccess.Equal(at) || status.LastError != nil {
		t.Fatalf("status = %+v", status)
	}
}

func TestRecoveryManagerStaysDegradedWhenMandatoryStepFails(t *testing.T) {
	var calls []string
	manager := coordination.NewRecoveryManager(coordination.RecoverySteps{
		Ping: func(context.Context) error { calls = append(calls, "ping"); return nil },
		CheckFingerprint: func(context.Context) error {
			calls = append(calls, "fingerprint")
			return errors.New("mismatch check unavailable")
		},
		ReloadConfiguration: func(context.Context) error { calls = append(calls, "configuration"); return nil },
	}, nil)
	manager.MarkDegraded(errors.New("outage"))

	if err := manager.Recover(context.Background()); err == nil {
		t.Fatal("Recover() succeeded")
	}
	if want := []string{"ping", "fingerprint"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if got := manager.Status().State; got != coordination.RecoveryDegraded {
		t.Fatalf("state = %q", got)
	}
}

func TestRecoveryManagerNeverClearsPermanentFault(t *testing.T) {
	called := false
	manager := coordination.NewRecoveryManager(coordination.RecoverySteps{
		Ping: func(context.Context) error { called = true; return nil },
	}, nil)
	manager.MarkPermanent(errors.New("revision regression"))

	var permanent coordination.PermanentError
	if err := manager.Recover(context.Background()); !errors.As(err, &permanent) {
		t.Fatalf("Recover() error = %v", err)
	}
	if called || manager.Status().State != coordination.RecoveryPermanentFault {
		t.Fatalf("called=%v status=%+v", called, manager.Status())
	}
}

func TestRecoveryManagerObservesOnlyActualStateTransitions(t *testing.T) {
	observer := &recoveryObserverStub{}
	manager := coordination.NewRecoveryManager(coordination.RecoverySteps{}, nil, observer)
	manager.MarkDegraded(errors.New("first"))
	manager.MarkDegraded(errors.New("second"))
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.MarkPermanent(errors.New("regression"))
	manager.MarkPermanent(errors.New("same permanent state"))
	want := [][2]coordination.RecoveryState{
		{coordination.RecoveryReady, coordination.RecoveryDegraded},
		{coordination.RecoveryDegraded, coordination.RecoveryReady},
		{coordination.RecoveryReady, coordination.RecoveryPermanentFault},
	}
	if !reflect.DeepEqual(observer.transitions, want) {
		t.Fatalf("transitions=%v want=%v", observer.transitions, want)
	}
}
