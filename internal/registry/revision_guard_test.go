package registry_test

import (
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

func TestRevisionGuardDistinguishesDelayedPublicationFromFreshRegression(t *testing.T) {
	guard := &registry.RevisionGuard{}
	guard.ObservePublished(12)
	guard.ObservePublished(9)
	if guard.Faulted() {
		t.Fatal("delayed older publication tripped the fault")
	}
	captured := guard.Highest()
	guard.ObservePublished(13)
	if guard.VerifyFresh(captured, 11) == nil {
		t.Fatal("fresh primary regression was accepted")
	}
	if !guard.Faulted() {
		t.Fatal("regression fault was not permanent")
	}
	if guard.VerifyFresh(guard.Highest(), 99) == nil {
		t.Fatal("permanent fault was cleared")
	}
}
