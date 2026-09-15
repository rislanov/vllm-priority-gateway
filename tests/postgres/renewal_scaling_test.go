package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
)

func TestPostgresRenewManyLeasesWithStatementLatency(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := updateFixtureLimits(t, store, createFixture(t, store, 0), 300, 300)
	ctx := context.Background()
	coordinator := coordpostgres.NewAdmissionCoordinator(store, 500*time.Millisecond)
	leases := make([]coordination.LeaseIdentity, 300)
	for i := range leases {
		decision, err := coordinator.Acquire(ctx, admissionRequest(f, uuid.New()))
		if err != nil || !decision.Admitted() {
			t.Fatalf("acquire %d: %+v, %v", i, decision, err)
		}
		leases[i] = *decision.Lease
	}
	// Fixed latency per UPDATE statement makes a per-lease SQL loop exceed
	// the operation budget without relying on the host's network performance.
	_, err := store.ConfigPool().Exec(ctx, `CREATE FUNCTION renewal_statement_latency() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.005); RETURN NULL; END $$;
		CREATE TRIGGER renewal_statement_latency AFTER UPDATE ON request_leases
		FOR EACH STATEMENT EXECUTE FUNCTION renewal_statement_latency()`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.ConfigPool().Exec(context.Background(), "DROP TRIGGER renewal_statement_latency ON request_leases; DROP FUNCTION renewal_statement_latency()")
	})
	results, err := coordinator.Renew(ctx, leases)
	if err != nil {
		t.Fatalf("renew %d streams: %v", len(leases), err)
	}
	if len(results) != len(leases) {
		t.Fatalf("results=%d, want %d", len(results), len(leases))
	}
	for i, result := range results {
		if result.Reason != "" || result.Lease.LeaseID != leases[i].LeaseID || !result.Lease.ExpiresAt.After(leases[i].ExpiresAt) {
			t.Fatalf("renewal %d: %+v", i, result)
		}
	}
}

func TestPostgresRenewWrongIdentityDoesNotExpireLiveReceipt(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	ctx := context.Background()
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	decision, err := coordinator.Acquire(ctx, admissionRequest(f, uuid.New()))
	if err != nil || !decision.Admitted() {
		t.Fatalf("acquire: %+v, %v", decision, err)
	}
	wrong := *decision.Lease
	wrong.ReplicaID = uuid.New()
	results, err := coordinator.Renew(ctx, []coordination.LeaseIdentity{wrong})
	if err != nil || len(results) != 1 || results[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("renew: %+v, %v", results, err)
	}
	var expired *time.Time
	if err := store.ConfigPool().QueryRow(ctx, "SELECT lease_expired_at FROM admission_operations WHERE lease_id=$1::uuid", wrong.LeaseID).Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if expired != nil {
		t.Fatal("wrong-identity renewal marked another owner's live receipt expired")
	}
}

func TestPostgresRenewReturnsOnlyCommittedPrefixOnLaterBatchFailure(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := updateFixtureLimits(t, store, createFixture(t, store, 0), 300, 300)
	ctx := context.Background()
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	leases := make([]coordination.LeaseIdentity, 300)
	for i := range leases {
		decision, err := coordinator.Acquire(ctx, admissionRequest(f, uuid.New()))
		if err != nil || !decision.Admitted() {
			t.Fatalf("acquire %d: %+v, %v", i, decision, err)
		}
		leases[i] = *decision.Lease
	}
	// Abort a later transaction after an earlier renewal transaction commits.
	// The generated UUID is the sole interpolation into this test-only DDL.
	_, err := store.ConfigPool().Exec(ctx, fmt.Sprintf(`CREATE FUNCTION renewal_batch_failure() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected renewal failure'; END $$;
		CREATE TRIGGER renewal_batch_failure BEFORE UPDATE ON request_leases
		FOR EACH ROW WHEN (NEW.lease_id='%s'::uuid) EXECUTE FUNCTION renewal_batch_failure()`, leases[len(leases)-1].LeaseID))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.ConfigPool().Exec(context.Background(), "DROP TRIGGER renewal_batch_failure ON request_leases; DROP FUNCTION renewal_batch_failure()")
	})
	results, err := coordinator.Renew(ctx, leases)
	if err == nil || len(results) == 0 || len(results) >= len(leases) {
		t.Fatalf("results=%d err=%v; want committed prefix and later failure", len(results), err)
	}
	for i, lease := range leases {
		var expires time.Time
		if err := store.ConfigPool().QueryRow(ctx, "SELECT expires_at FROM request_leases WHERE lease_id=$1::uuid", lease.LeaseID).Scan(&expires); err != nil {
			t.Fatal(err)
		}
		if i < len(results) {
			if results[i].Lease.LeaseID != lease.LeaseID || !expires.After(lease.ExpiresAt) || !expires.Equal(results[i].Lease.ExpiresAt) {
				t.Fatalf("committed renewal %d: expires=%s result=%+v", i, expires, results[i])
			}
		} else if !expires.Equal(lease.ExpiresAt) {
			t.Fatalf("unreported renewal %d was committed: %s != %s", i, expires, lease.ExpiresAt)
		}
	}
}
