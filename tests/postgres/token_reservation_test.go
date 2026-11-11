package postgres_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination/contracttest"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func tokenReservationFixtures(t *testing.T, store *pgstore.Store, rpm, tpm int64) [2]fixture {
	t.Helper()
	ctx := context.Background()
	policy := domain.RequestSizePolicy{
		Enabled: true, MaxOutstandingTokens: 1000, EstimationProfile: "qualified-v1",
		Limits: domain.RequestSizeLimits{MaxInputTokens: 2000, MaxOutputTokens: 500, DefaultOutputTokens: 10},
	}
	pool, err := store.CreatePool(ctx, basestore.CreatePoolParams{
		PublicModelName: "token-model", UpstreamModelName: "upstream", Enabled: true,
		MaxGatewayInflight: 100, RequestSizeAdmission: &policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	var result [2]fixture
	budget := int64(800)
	for i := range result {
		name := fmt.Sprintf("token-client-%d", i)
		client, err := store.CreateClient(ctx, basestore.CreateClientParams{
			Name: name, Enabled: true, PriorityClass: domain.PriorityNormal, MaxConcurrency: 100,
			MaxOutstandingTokens: &budget, RequestsPerMinute: rpm, TokensPerMinute: tpm, ModelPoolIDs: []int64{pool.ID},
		})
		if err != nil {
			t.Fatal(err)
		}
		key, err := store.CreateAPIKey(ctx, basestore.CreateAPIKeyParams{
			ClientID: client.ID, Prefix: name, SecretHash: sha256.Sum256([]byte(name)),
		})
		if err != nil {
			t.Fatal(err)
		}
		result[i] = fixture{pool: pool, client: client, key: key}
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range result {
		result[i].revision = snapshot.Revision
	}
	return result
}

func postgresTokenRequest(f fixture, replica uuid.UUID, amount int64) coordination.AdmissionRequest {
	r := admissionRequest(f, replica)
	r.PriorityClass = f.client.PriorityClass
	r.TokenReservation = coordination.TokenReservation{
		Enabled: true, InputEstimate: amount - 1, OutputAllowance: 1, PromptCount: 1, Samples: 1, ReservedTokens: amount,
		ClientBudget: f.client.MaxOutstandingTokens, PoolBudget: f.pool.RequestSizeAdmission.MaxOutstandingTokens,
		PoolPolicyRevision: f.pool.Revision, EstimationProfile: f.pool.RequestSizeAdmission.EstimationProfile, EstimatorKind: "upstream",
	}
	return r
}

func TestPostgresTokenReservationContract(t *testing.T) {
	contracttest.TokenReservationContract(t, func(t *testing.T) (coordination.AdmissionCoordinator, func(int, int64) coordination.AdmissionRequest) {
		db := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
		f := tokenReservationFixtures(t, db, 0, 0)
		replica := uuid.New()
		return coordpostgres.NewAdmissionCoordinator(db, 3*time.Second), func(client int, tokens int64) coordination.AdmissionRequest {
			return postgresTokenRequest(f[client], replica, tokens)
		}
	})
}

func TestPostgresTokenReservationAcrossReplicas(t *testing.T) {
	db := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := tokenReservationFixtures(t, db, 100, 0)
	coordinators := [2]*coordpostgres.AdmissionCoordinator{
		coordpostgres.NewAdmissionCoordinator(db, 5*time.Second),
		coordpostgres.NewAdmissionCoordinator(db, 5*time.Second),
	}
	replicas := [2]uuid.UUID{uuid.New(), uuid.New()}
	ctx := context.Background()
	var wg sync.WaitGroup
	outcomes := make(chan coordination.AdmissionDecision, 100)
	started := time.Now()
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := postgresTokenRequest(f[i%2], replicas[i%2], 100)
			d, err := coordinators[i%2].Acquire(ctx, req)
			if err != nil {
				t.Errorf("acquire %d: %v", i, err)
			}
			outcomes <- d
		}(i)
	}
	wg.Wait()
	t.Logf("100 concurrent acquisitions finished in %s", time.Since(started))
	close(outcomes)
	var admitted int
	for d := range outcomes {
		if d.Admitted() {
			admitted++
		} else if d.Reason != coordination.ReasonTokenBudgetExhausted {
			t.Errorf("unexpected rejection: %+v", d)
		}
	}
	var reserved int64
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT COALESCE(sum(reserved_tokens),0)::bigint FROM request_leases").Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	if admitted != 10 || reserved != 1000 {
		t.Fatalf("admitted=%d reserved=%d, want 10 admissions and 1000 tokens", admitted, reserved)
	}
}

func TestPostgresTokenReservationLostCommit(t *testing.T) {
	raw := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if raw == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	runtime, armed := commitLossProxy(t, raw)
	db := openTestStoreWithMigration(t, runtime, raw)
	f := tokenReservationFixtures(t, db, 10, 500)
	coordinator := coordpostgres.NewAdmissionCoordinator(db, 3*time.Second)
	var events []coordination.TokenReservationEvent
	coordinator.SetTokenReservationObserver(tokenReservationObserverFunc(func(event coordination.TokenReservationEvent) {
		events = append(events, event)
	}))
	ctx := context.Background()
	req := postgresTokenRequest(f[0], uuid.New(), 600)
	armed.Store(true)
	d, err := coordinator.Acquire(ctx, req)
	if err != nil || !d.Admitted() || d.Lease.LeaseID != req.LeaseID || armed.Load() {
		t.Fatalf("lost acquire commit did not resolve once: %+v, %v", d, err)
	}
	var reserved int64
	var rpm float64
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT (SELECT sum(reserved_tokens)::bigint FROM request_leases), (SELECT balance FROM coordination_rate_state WHERE rate_kind='rpm')").Scan(&reserved, &rpm); err != nil {
		t.Fatal(err)
	}
	if reserved != 600 || rpm != 9 {
		t.Fatalf("acquire replay reserved %d with RPM balance %v", reserved, rpm)
	}
	changed := req
	changed.TokenReservation.EstimationProfile += "-changed"
	if d, err := coordinator.Acquire(ctx, changed); err != nil || d.Reason != coordination.ReasonIdempotencyConflict {
		t.Fatalf("changed replay = %+v, %v", d, err)
	}
	items := []coordination.LeaseCompletion{{Lease: *d.Lease, Usage: &coordination.TokenUsage{InputTokens: 150, OutputTokens: 100}}}
	armed.Store(true)
	results, err := coordinator.Complete(ctx, items)
	if err == nil || len(results) != 0 || armed.Load() {
		t.Fatalf("expected unresolved completion after commit loss: %+v, %v", results, err)
	}
	results, err = coordinator.Complete(ctx, items)
	if err != nil || len(results) != 1 || results[0].Result != coordination.CompletionAlreadyCompleted {
		t.Fatalf("completion retry = %+v, %v", results, err)
	}
	var tpm float64
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT (SELECT COALESCE(sum(reserved_tokens),0)::bigint FROM request_leases), (SELECT balance FROM coordination_rate_state WHERE rate_kind='tpm')").Scan(&reserved, &tpm); err != nil {
		t.Fatal(err)
	}
	if reserved != 0 || tpm != 250 {
		t.Fatalf("completion replay left %d reserved and %v TPM, want 0 and 250", reserved, tpm)
	}
	if len(events) != 0 {
		t.Fatalf("ambiguous commits or replays emitted lifecycle events: %+v", events)
	}
}

func TestPostgresTokenReservationExpiryAndPolicyRace(t *testing.T) {
	db := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	f := tokenReservationFixtures(t, db, 0, 0)
	ctx := context.Background()
	coordinator := coordpostgres.NewAdmissionCoordinator(db, 3*time.Second)
	replica := uuid.New()
	acquire := func(req coordination.AdmissionRequest, want coordination.Reason, scope coordination.AdmissionScope) coordination.AdmissionDecision {
		t.Helper()
		got, err := coordinator.Acquire(ctx, req)
		if err != nil || got.Reason != want || got.TokenBudgetScope != scope || got.Admitted() != (want == "") {
			t.Fatalf("acquire=%+v err=%v, want %q %q", got, err, want, scope)
		}
		return got
	}
	updatePool := func(policy domain.RequestSizePolicy) {
		t.Helper()
		p := f[0].pool
		_, err := db.UpdatePool(ctx, p.ID, basestore.UpdatePoolParams{
			PublicModelName: p.PublicModelName, UpstreamModelName: p.UpstreamModelName, Enabled: true,
			MaxGatewayInflight: p.MaxGatewayInflight, RequestSizeAdmission: &policy,
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range f {
			f[i] = refreshFixture(t, db, f[i])
		}
	}

	policy := f[0].pool.RequestSizeAdmission
	first := acquire(postgresTokenRequest(f[0], replica, 600), "", "")
	other := *first.Lease
	other.ReplicaID = uuid.New()
	renewed, err := coordinator.Renew(ctx, []coordination.LeaseIdentity{other})
	if err != nil || renewed[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("wrong-owner renewal = %+v, %v", renewed, err)
	}
	completed, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: other}})
	if err != nil || completed[0].Reason != coordination.ReasonLeaseLost {
		t.Fatalf("wrong-owner completion = %+v, %v", completed, err)
	}

	reduced := policy
	reduced.MaxOutstandingTokens = 500
	stale := postgresTokenRequest(f[1], replica, 100)
	updatePool(reduced)
	acquire(stale, coordination.ReasonStaleConfiguration, "")
	acquire(postgresTokenRequest(f[1], replica, 100), coordination.ReasonTokenBudgetExhausted, coordination.AdmissionPoolScope)

	updatePool(domain.RequestSizePolicy{})
	legacy := acquire(admissionRequest(f[1], replica), "", "")
	secondPool, err := db.CreatePool(ctx, basestore.CreatePoolParams{
		PublicModelName: "second-model", UpstreamModelName: "upstream", Enabled: true,
		MaxGatewayInflight: 100, RequestSizeAdmission: &policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := f[0].client
	_, err = db.UpdateClient(ctx, client.ID, basestore.UpdateClientParams{
		Name: client.Name, Enabled: true, PriorityClass: domain.PriorityBackground, MaxConcurrency: 100,
		ModelPoolIDs: []int64{f[0].pool.ID, secondPool.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	second := f[0]
	second.pool = secondPool
	second = refreshFixture(t, db, second)
	acquire(postgresTokenRequest(second, replica, 300), coordination.ReasonTokenBudgetExhausted, coordination.AdmissionClientScope)
	updatePool(policy)
	acquire(postgresTokenRequest(f[1], replica, 100), coordination.ReasonTokenPolicyTransition, "")

	if _, err := db.CoordinationPool().Exec(ctx, "UPDATE request_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE lease_id IN ($1::uuid,$2::uuid)", first.Lease.LeaseID, legacy.Lease.LeaseID); err != nil {
		t.Fatal(err)
	}
	// The expired rows still exist, but they no longer block either scope.
	second = refreshFixture(t, db, second)
	next := acquire(postgresTokenRequest(second, replica, 300), "", "")
	acquire(postgresTokenRequest(f[1], replica, 100), "", "")

	var wg sync.WaitGroup
	for _, operation := range []func() error{
		func() error { return coordinator.Cleanup(ctx, 100) },
		func() error {
			results, err := coordinator.Renew(ctx, []coordination.LeaseIdentity{*first.Lease})
			if err == nil && results[0].Reason != coordination.ReasonLeaseLost {
				return fmt.Errorf("renewed expired lease: %+v", results)
			}
			return err
		},
		func() error {
			results, err := coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: *first.Lease, Usage: &coordination.TokenUsage{InputTokens: 125, OutputTokens: 125}}})
			if err == nil && results[0].Result != coordination.CompletionLeaseLost {
				return fmt.Errorf("completion of expired lease: %+v", results)
			}
			return err
		},
	} {
		wg.Add(1)
		go func(operation func() error) {
			defer wg.Done()
			if err := operation(); err != nil {
				t.Error(err)
			}
		}(operation)
	}
	wg.Wait()
	var reserved, input, output int64
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT token_actual_input,token_actual_output FROM admission_operations WHERE lease_id=$1::uuid", first.Lease.LeaseID).Scan(&input, &output); err != nil || input != 125 || output != 125 {
		t.Fatalf("late actual usage = %d/%d, %v", input, output, err)
	}
	if err := db.CoordinationPool().QueryRow(ctx, "SELECT sum(reserved_tokens)::bigint FROM request_leases").Scan(&reserved); err != nil || reserved != 400 {
		t.Fatalf("cleanup/renew/complete affected unrelated reserves: %d, %v", reserved, err)
	}
	if _, err := db.DeleteClient(ctx, f[0].client.ID); err != nil {
		t.Fatal(err)
	}
	completed, err = coordinator.Complete(ctx, []coordination.LeaseCompletion{{Lease: *next.Lease}})
	if err != nil || completed[0].Result != coordination.CompletionReleased {
		t.Fatalf("completion after configuration deletion = %+v, %v", completed, err)
	}
}

func TestPostgresTokenReservationContractUpgrade(t *testing.T) {
	db := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	ctx := context.Background()
	previous := coordpostgres.ReplicaPolicy{LeaseTTL: time.Minute, LeaseRenewInterval: 10 * time.Second, ContractVersion: 3}
	oldID := uuid.New()
	fp := coordpostgres.PolicyFingerprint(previous)
	_, err := db.CoordinationPool().Exec(ctx, `INSERT INTO coordination_replicas
		(replica_id,started_at,heartbeat_at,heartbeat_expires_at,binary_version,coordination_contract_version,policy_fingerprint)
		VALUES($1::uuid,now(),now(),now()+interval '1 minute','old',3,$2::bytea)`, oldID, fp[:])
	if err != nil {
		t.Fatal(err)
	}
	previous.ContractVersion = 0
	current := coordpostgres.NewReplicaManager(db, uuid.New(), previous, time.Second)
	if err := current.Start(ctx); err == nil {
		current.Close()
		t.Fatal("version 4 admitted a live version 3 registration")
	}
	if _, err := db.CoordinationPool().Exec(ctx, "UPDATE coordination_replicas SET heartbeat_expires_at=now()-interval '1 second' WHERE replica_id=$1::uuid", oldID); err != nil {
		t.Fatal(err)
	}
	if err := current.Start(ctx); err != nil {
		t.Fatalf("version 4 did not start after version 3 drained: %v", err)
	}
	defer current.Close()
}

type tokenReservationObserverFunc func(coordination.TokenReservationEvent)

func (f tokenReservationObserverFunc) TokenReservationChanged(event coordination.TokenReservationEvent) {
	f(event)
}

func TestPostgresTokenReservationExpiryTelemetry(t *testing.T) {
	db := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixtures := tokenReservationFixtures(t, db, 0, 0)
	ctx := context.Background()
	acquiring := coordpostgres.NewAdmissionCoordinator(db, 3*time.Second)
	expiring := coordpostgres.NewAdmissionCoordinator(db, 3*time.Second)
	var events []coordination.TokenReservationEvent
	expiring.SetTokenReservationObserver(tokenReservationObserverFunc(func(event coordination.TokenReservationEvent) {
		events = append(events, event)
	}))
	for _, via := range []string{"cleanup", "renew", "complete"} {
		t.Run(via, func(t *testing.T) {
			events = nil
			request := postgresTokenRequest(fixtures[0], uuid.New(), 600)
			result, err := acquiring.Acquire(ctx, request)
			if err != nil || !result.Admitted() {
				t.Fatalf("acquire = %+v, %v", result, err)
			}
			if _, err := db.CoordinationPool().Exec(ctx, "UPDATE request_leases SET expires_at=clock_timestamp() WHERE lease_id=$1::uuid", request.LeaseID); err != nil {
				t.Fatal(err)
			}
			switch via {
			case "cleanup":
				err = expiring.Cleanup(ctx, 16)
			case "renew":
				_, err = expiring.Renew(ctx, []coordination.LeaseIdentity{*result.Lease})
			case "complete":
				_, err = expiring.Complete(ctx, []coordination.LeaseCompletion{{Lease: *result.Lease}})
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := expiring.Cleanup(ctx, 16); err != nil {
				t.Fatal(err)
			}
			want := 1
			if via == "complete" {
				want++
			}
			if len(events) != want || events[0].Action != coordination.ReservationExpired ||
				events[0].ReservedTokens != 600 || events[0].PriorityClass != fixtures[0].client.PriorityClass ||
				events[0].Reason != coordination.ReasonLeaseLost {
				t.Fatalf("events = %+v", events)
			}
		})
	}
}
