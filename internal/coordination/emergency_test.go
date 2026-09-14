package coordination_test

import (
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

func TestEmergencyAdmissionEnforcesClassAndClientCaps(t *testing.T) {
	e := coordination.NewEmergencyAdmission(2, 1)
	r1, ok := e.Acquire(domain.PriorityCritical, 10, 1)
	if !ok {
		t.Fatal("first critical request rejected")
	}
	if release, admitted := e.Acquire(domain.PriorityCritical, 10, 1); admitted || release != nil {
		t.Fatal("client cap was exceeded")
	}
	r2, ok := e.Acquire(domain.PriorityCritical, 11, 2)
	if !ok {
		t.Fatal("second critical request rejected")
	}
	if release, admitted := e.Acquire(domain.PriorityCritical, 12, 2); admitted || release != nil {
		t.Fatal("class cap was exceeded")
	}
	if release, admitted := e.Acquire(domain.PriorityNormal, 13, 2); admitted || release != nil {
		t.Fatal("normal request entered emergency admission")
	}
	r1()
	r2()
	if _, ok := e.Acquire(domain.PriorityHigh, 20, 1); !ok {
		t.Fatal("high class cap was not independent")
	}
}
