package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

func TestPostgresRevokeAPIKeyPreservesDatabaseTimeout(t *testing.T) {
	store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
	fixture := createFixture(t, store, 0)
	blocker, err := store.ConfigPool().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	if _, err := blocker.Exec(context.Background(), "SELECT id FROM api_keys WHERE id=$1 FOR UPDATE", fixture.key.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = store.RevokeAPIKey(ctx, fixture.key.ID)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("revoke error=%v; database timeout must remain distinguishable from a missing key", err)
	}
	if err := blocker.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	var revoked bool
	if err := store.ConfigPool().QueryRow(context.Background(), "SELECT revoked_at IS NOT NULL FROM api_keys WHERE id=$1", fixture.key.ID).Scan(&revoked); err != nil {
		t.Fatal(err)
	}
	if revoked {
		t.Fatal("timed-out revocation changed the key")
	}
	if err := store.RevokeAPIKey(context.Background(), fixture.key.ID+1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing key error=%v, want ErrNoRows", err)
	}
}

func TestPostgresAdminDatabaseTimeoutAfterPreflightReturnsUnavailable(t *testing.T) {
	for _, operation := range []string{"revoke key", "update client", "update pool", "update backend", "drain backend", "create key"} {
		t.Run(operation, func(t *testing.T) {
			store := openTestStore(t, os.Getenv("LLMGW_POSTGRES_TEST_DSN"))
			fixture := createFixture(t, store, 0)
			registryValue := registry.New(store)
			if err := registryValue.Reload(context.Background()); err != nil {
				t.Fatal(err)
			}
			preflightChecks := 0
			service, err := httpapi.NewAdminService(httpapi.AdminDependencies{
				Store: store, Analytics: store, Registry: registryValue, Runtime: idleAdminRuntime{},
				HMACSecret:       postgresHTTPSecret,
				MutationsAllowed: func() bool { preflightChecks++; return true },
			})
			if err != nil {
				t.Fatal(err)
			}
			method, path, body, lock := http.MethodPut, "", "", ""
			switch operation {
			case "revoke key":
				method, path = http.MethodDelete, fmt.Sprintf("/admin/api/keys/%d", fixture.key.ID)
				lock = fmt.Sprintf("SELECT id FROM api_keys WHERE id=%d FOR UPDATE", fixture.key.ID)
			case "update client":
				path = fmt.Sprintf("/admin/api/clients/%d", fixture.client.ID)
				body = fmt.Sprintf(`{"name":"updated-client","enabled":true,"priorityClass":"high","maxConcurrency":1,"modelPoolIds":[%d]}`, fixture.pool.ID)
				lock = fmt.Sprintf("SELECT id FROM clients WHERE id=%d FOR UPDATE", fixture.client.ID)
			case "update pool":
				path = fmt.Sprintf("/admin/api/pools/%d", fixture.pool.ID)
				body = `{"publicModelName":"updated-model","upstreamModelName":"upstream","enabled":true,"maxGatewayInflight":1}`
				lock = fmt.Sprintf("SELECT pool_id FROM pool_admission_scopes WHERE pool_id=%d FOR UPDATE", fixture.pool.ID)
			case "update backend":
				path = fmt.Sprintf("/admin/api/backends/%d", fixture.backend.ID)
				body = fmt.Sprintf(`{"modelPoolId":%d,"name":"updated-backend","baseUrl":"http://127.0.0.1:1","enabled":true,"capacityHint":1,"runningSoftLimit":1}`, fixture.pool.ID)
				lock = fmt.Sprintf("SELECT backend_id FROM backend_circuit_state WHERE backend_id=%d FOR UPDATE", fixture.backend.ID)
			case "drain backend":
				method, path = http.MethodPost, fmt.Sprintf("/admin/api/backends/%d/drain", fixture.backend.ID)
				lock = fmt.Sprintf("SELECT backend_id FROM backend_circuit_state WHERE backend_id=%d FOR UPDATE", fixture.backend.ID)
			case "create key":
				method, path, body = http.MethodPost, fmt.Sprintf("/admin/api/clients/%d/keys", fixture.client.ID), `{}`
				lock = "LOCK TABLE api_keys IN ACCESS EXCLUSIVE MODE"
			}
			blocker, err := store.ConfigPool().Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
			if _, err := blocker.Exec(context.Background(), lock); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			request := httptest.NewRequest(method, path, strings.NewReader(body)).WithContext(ctx)
			response := httptest.NewRecorder()
			httpapi.NewAdminAPI(service).ServeHTTP(response, request)
			if preflightChecks != 1 {
				t.Fatalf("preflight checks=%d, want one successful preflight before the database operation", preflightChecks)
			}
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("mutation did not wait on the database lock: status=%d body=%s", response.Code, response.Body.String())
			}
			if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"configuration_unavailable"`) {
				t.Errorf("database timeout response=%d body=%s, want retryable configuration_unavailable", response.Code, response.Body.String())
			}
			if err := blocker.Rollback(context.Background()); err != nil {
				t.Fatal(err)
			}
			revision, err := store.CurrentRevision(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if revision != fixture.revision {
				t.Fatalf("failed mutation changed revision: got %d, want %d", revision, fixture.revision)
			}
		})
	}
}

type idleAdminRuntime struct{}

func (idleAdminRuntime) Reconcile([]domain.Backend) error { return nil }
func (idleAdminRuntime) PoolSnapshot(int64, time.Time) domain.PoolRuntime {
	return domain.PoolRuntime{}
}
func (idleAdminRuntime) Snapshot(int64, time.Time) domain.BackendRuntime {
	return domain.BackendRuntime{}
}
