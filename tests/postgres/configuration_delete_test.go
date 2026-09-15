package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
)

func TestPostgresConfigurationDeletesDependentsAndNotifiesCommittedRevision(t *testing.T) {
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	store := openTestStore(t, dsn)
	f := createFixture(t, store, 0)
	ctx := context.Background()
	secondKey, err := store.CreateAPIKey(ctx, basestore.CreateAPIKeyParams{
		ClientID: f.client.ID, Prefix: "llmgw_second", SecretHash: [32]byte{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(context.Background()) })
	if _, err := listener.Exec(ctx, "LISTEN llmgw_config_changed"); err != nil {
		t.Fatal(err)
	}
	var deletedKeyIDs []int64
	for index, deleteValue := range []func() error{
		func() error { return store.DeleteBackend(ctx, f.backend.ID) },
		func() error { return store.DeletePool(ctx, f.pool.ID) },
		func() error {
			deletedKeyIDs, err = store.DeleteClient(ctx, f.client.ID)
			return err
		},
	} {
		if err := deleteValue(); err != nil {
			t.Fatal(err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, time.Second)
		notification, err := listener.WaitForNotification(waitCtx)
		cancel()
		wantRevision := int64(6 + index)
		if err != nil || notification.Payload != fmt.Sprint(wantRevision) {
			t.Fatalf("deletion notification=%+v err=%v, want revision %d", notification, err, wantRevision)
		}
		revision, err := store.CurrentRevision(ctx)
		if err != nil || revision != wantRevision {
			t.Fatalf("committed revision=%d err=%v, want %d", revision, err, wantRevision)
		}
	}
	if !reflect.DeepEqual(deletedKeyIDs, []int64{f.key.ID, secondKey.ID}) {
		t.Fatalf("deleted key IDs=%v, want [%d %d]", deletedKeyIDs, f.key.ID, secondKey.ID)
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != 8 || len(snapshot.Clients)+len(snapshot.Keys)+len(snapshot.Pools)+len(snapshot.Backends)+len(snapshot.Access) != 0 {
		t.Fatalf("configuration after deletion=%+v", snapshot)
	}
	var clientTombstoned, poolTombstoned bool
	var backendRevision, generation int64
	if err := store.ConfigPool().QueryRow(ctx, `SELECT
		(SELECT tombstoned FROM client_admission_scopes WHERE client_id=$1),
		(SELECT tombstoned FROM pool_admission_scopes WHERE pool_id=$2),
		backend_revision,generation FROM backend_circuit_state WHERE backend_id=$3`,
		f.client.ID, f.pool.ID, f.backend.ID).Scan(&clientTombstoned, &poolTombstoned, &backendRevision, &generation); err != nil {
		t.Fatal(err)
	}
	if !clientTombstoned || !poolTombstoned || backendRevision != f.backend.Revision+1 || generation != 1 {
		t.Fatalf("retained identities: client=%v pool=%v backend revision=%d generation=%d", clientTombstoned, poolTombstoned, backendRevision, generation)
	}
}

func TestPostgresConfigurationDeleteRejectsReferencesAndMissingTargetsWithoutRevisionChange(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	ctx := context.Background()
	before, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeletePool(ctx, f.pool.ID); !errors.Is(err, basestore.ErrPoolHasBackends) {
		t.Fatalf("referenced pool deletion error=%v, want ErrPoolHasBackends", err)
	}
	if keyIDs, err := store.DeleteClient(ctx, 999); !errors.Is(err, pgx.ErrNoRows) || len(keyIDs) != 0 {
		t.Fatalf("missing client deletion key IDs=%v err=%v, want ErrNoRows", keyIDs, err)
	}
	for name, deleteValue := range map[string]func(context.Context, int64) error{
		"pool": store.DeletePool, "backend": store.DeleteBackend,
	} {
		if err := deleteValue(ctx, 999); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("missing %s deletion error=%v, want ErrNoRows", name, err)
		}
	}
	after, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("configuration changed after rejected deletions: before=%+v after=%+v", before, after)
	}
}

func TestPostgresConfigurationDeleteRollsBackWhenRevisionCannotCommit(t *testing.T) {
	for _, target := range []string{"client", "pool", "backend"} {
		t.Run(target, func(t *testing.T) {
			store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
			f := createFixture(t, store, 0)
			ctx := context.Background()
			if target == "pool" {
				if err := store.DeleteBackend(ctx, f.backend.ID); err != nil {
					t.Fatal(err)
				}
			}
			before, err := store.LoadSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ConfigPool().Exec(ctx, `CREATE FUNCTION llmgw_test_reject_delete_revision() RETURNS trigger
				LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject test revision'; END $$;
				CREATE TRIGGER llmgw_test_reject_delete_revision BEFORE UPDATE ON config_meta
				FOR EACH ROW EXECUTE FUNCTION llmgw_test_reject_delete_revision()`); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := store.ConfigPool().Exec(context.Background(), `DROP TRIGGER llmgw_test_reject_delete_revision ON config_meta;
					DROP FUNCTION llmgw_test_reject_delete_revision()`); err != nil {
					t.Errorf("remove revision failure trigger: %v", err)
				}
			})
			switch target {
			case "client":
				var keyIDs []int64
				keyIDs, err = store.DeleteClient(ctx, f.client.ID)
				if len(keyIDs) != 0 {
					t.Fatalf("rolled-back deletion returned key IDs %v", keyIDs)
				}
			case "pool":
				err = store.DeletePool(ctx, f.pool.ID)
			case "backend":
				err = store.DeleteBackend(ctx, f.backend.ID)
			}
			if err == nil {
				t.Fatal("deletion committed despite rejected configuration revision")
			}
			after, err := store.LoadSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("configuration changed after rolled-back deletion: before=%+v after=%+v", before, after)
			}
			var clientTombstoned, poolTombstoned bool
			if err := store.ConfigPool().QueryRow(ctx, `SELECT
				(SELECT tombstoned FROM client_admission_scopes WHERE client_id=$1),
				(SELECT tombstoned FROM pool_admission_scopes WHERE pool_id=$2)`, f.client.ID, f.pool.ID).Scan(&clientTombstoned, &poolTombstoned); err != nil {
				t.Fatal(err)
			}
			if clientTombstoned || poolTombstoned {
				t.Fatalf("rollback left scope tombstones: client=%v pool=%v", clientTombstoned, poolTombstoned)
			}
		})
	}
}

func TestPostgresConfigurationDeletionPreservesInflightAdmissionCompletion(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createRateFixture(t, store, 60, 6000)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, time.Second)
	ctx := context.Background()
	request := admissionRequest(f, uuid.New())
	decision, err := coordinator.Acquire(ctx, request)
	if err != nil || !decision.Admitted() {
		t.Fatalf("initial admission=%+v err=%v", decision, err)
	}
	if _, err := store.DeleteClient(ctx, f.client.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteBackend(ctx, f.backend.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeletePool(ctx, f.pool.ID); err != nil {
		t.Fatal(err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "request_leases", "lease_id", request.LeaseID, 1)
	completion := coordination.LeaseCompletion{
		Lease: decision.Lease.Identity(), Usage: &coordination.TokenUsage{InputTokens: 20, OutputTokens: 10},
	}
	results, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{completion})
	if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionReleased {
		t.Fatalf("completion after deletion=%+v err=%v", results, err)
	}
	results, err = coordinator.Complete(ctx, []coordination.LeaseCompletion{completion})
	if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionAlreadyCompleted || results[0].OriginalResult != coordination.CompletionReleased {
		t.Fatalf("completion replay after deletion=%+v err=%v", results, err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "request_leases", "lease_id", request.LeaseID, 0)
	assertPostgresRowCount(t, store.CoordinationPool(), "admission_operations", "lease_id", request.LeaseID, 1)
	request.LeaseID = uuid.New()
	rejected, err := coordinator.Acquire(ctx, request)
	if err != nil || rejected.Reason != coordination.ReasonStaleConfiguration || rejected.Lease != nil {
		t.Fatalf("new admission after deletion=%+v err=%v", rejected, err)
	}
	var rateRows int
	if err := store.CoordinationPool().QueryRow(ctx, "SELECT count(*) FROM coordination_rate_state WHERE client_id=$1", f.client.ID).Scan(&rateRows); err != nil || rateRows != 0 {
		t.Fatalf("deleted client rate rows=%d err=%v", rateRows, err)
	}
}

func TestPostgresConfigurationDeleteBackendSupersedesPermitsAndRejectsStaleTopology(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Millisecond, HalfOpenMaxProbes: 1}
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	identity := coordination.BackendIdentity{ID: f.backend.ID, Revision: f.backend.Revision, Enabled: true}
	if err := coordinator.Reconcile(ctx, []coordination.BackendIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	failureRequest := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute,
	}
	closed, err := coordinator.Acquire(ctx, failureRequest)
	if err != nil || closed.Reason != "" {
		t.Fatalf("closed acquire=%+v err=%v", closed, err)
	}
	if _, err := coordinator.Complete(ctx, coordination.CircuitCompletion{
		AttemptID: failureRequest.AttemptID, Backend: identity, Generation: closed.Snapshot.Generation,
		Outcome: domain.InferenceFailure, ReportedOutcomeAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(options.OpenCooldown + time.Millisecond)
	probeRequest := coordination.CircuitAcquireRequest{
		AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: identity, ProbeTTL: time.Minute,
	}
	probe, err := coordinator.Acquire(ctx, probeRequest)
	if err != nil || probe.Permit == nil {
		t.Fatalf("half-open acquire=%+v err=%v", probe, err)
	}
	if err := store.DeleteBackend(ctx, f.backend.ID); err != nil {
		t.Fatal(err)
	}
	var outcome string
	var processed, retainUntil time.Time
	if err := store.ConfigPool().QueryRow(ctx, "SELECT outcome,processed_at,retain_until FROM backend_circuit_probes WHERE permit_id=$1", probe.Permit.PermitID).Scan(&outcome, &processed, &retainUntil); err != nil {
		t.Fatal(err)
	}
	if outcome != "superseded" || !retainUntil.Equal(processed.Add(24*time.Hour)) {
		t.Fatalf("deleted backend probe: outcome=%q processed=%s retained=%s", outcome, processed, retainUntil)
	}
	renewed, err := coordinator.RenewProbes(ctx, []coordination.ProbeIdentity{*probe.Permit})
	if err != nil || len(renewed) != 1 || renewed[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("deleted backend probe renewal=%+v err=%v", renewed, err)
	}
	completed, err := coordinator.Complete(ctx, coordination.CircuitCompletion{
		AttemptID: probe.Permit.PermitID, Backend: identity, Generation: probe.Permit.Generation,
		Outcome: domain.InferenceSuccess, ReportedOutcomeAt: time.Now().UTC(),
	})
	if err != nil || completed.Backend.Revision != identity.Revision+1 || completed.FailureCount != 0 {
		t.Fatalf("superseded probe completion=%+v err=%v", completed, err)
	}
	if err := coordinator.Reconcile(ctx, []coordination.BackendIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	replayed, err := coordinator.Acquire(ctx, probeRequest)
	if err != nil || replayed.Reason != coordination.ReasonStaleBackend || replayed.Permit != nil {
		t.Fatalf("stale topology acquire=%+v err=%v", replayed, err)
	}
	assertPostgresRowCount(t, store.CoordinationPool(), "backend_circuit_probes", "permit_id", probe.Permit.PermitID, 1)
}
