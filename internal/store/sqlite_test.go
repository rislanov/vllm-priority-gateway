package store_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

//go:embed migrations/001_initial.sql
var initialMigration string

func openTestDB(t *testing.T) *store.SQLite {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.db")
	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return db
}

func TestSQLiteMigratesAndReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "gateway.db")
	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if got := sqliteUserVersion(t, path); got != 2 {
		t.Fatalf("fresh database user_version = %d, want 2", got)
	}

	db, err = store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
	if db.Path() != path {
		t.Fatalf("Path() = %q, want %q", db.Path(), path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("database permissions = %o", info.Mode().Perm())
	}
}

func TestSQLiteMigratesVersionOnePoolSafetyDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	createLegacyPoolDatabase(t, path, 1)

	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() v1 database error = %v", err)
	}
	pools, err := db.ListPools(context.Background())
	if err != nil {
		t.Fatalf("ListPools() error = %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("ListPools() length = %d, want 1", len(pools))
	}
	pool := pools[0]
	if pool.PublicModelName != "legacy-public" || pool.UpstreamModelName != "legacy-upstream" {
		t.Fatalf("migrated pool = %+v", pool)
	}
	if pool.MaxGatewayInflight != 0 || pool.MaxWaiting != 0 {
		t.Fatalf("migrated safety limits = (%d, %d), want (0, 0)", pool.MaxGatewayInflight, pool.MaxWaiting)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() migrated database error = %v", err)
	}
	if got := sqliteUserVersion(t, path); got != 2 {
		t.Fatalf("migrated user_version = %d, want 2", got)
	}

	db, err = store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen migrated database error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() reopened database error = %v", err)
	}
}

func TestSQLiteMigrationRollsBackWhenSecondStatementFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	createLegacyPoolDatabase(t, path, 1)

	legacyDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	if _, err := legacyDB.Exec(`
		ALTER TABLE model_pools ADD COLUMN max_waiting INTEGER NOT NULL DEFAULT 0 CHECK (max_waiting >= 0);
		UPDATE model_pools SET max_waiting = 23;`); err != nil {
		legacyDB.Close()
		t.Fatalf("prepare conflicting v1 schema: %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("close conflicting v1 database: %v", err)
	}

	if db, err := store.Open(context.Background(), path); err == nil {
		db.Close()
		t.Fatal("Open() succeeded despite duplicate max_waiting column")
	} else if !strings.Contains(err.Error(), "duplicate column name: max_waiting") {
		t.Fatalf("Open() error = %v, want duplicate max_waiting failure from second ALTER", err)
	}

	inspectDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database after failed migration: %v", err)
	}
	defer inspectDB.Close()

	var version int
	if err := inspectDB.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version after failed migration: %v", err)
	}
	if version != 1 {
		t.Fatalf("user_version after failed migration = %d, want 1", version)
	}

	rows, err := inspectDB.Query("PRAGMA table_info(model_pools)")
	if err != nil {
		t.Fatalf("inspect model_pools schema: %v", err)
	}
	defer rows.Close()
	var hasMaxGatewayInflight, hasMaxWaiting bool
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan model_pools column: %v", err)
		}
		hasMaxGatewayInflight = hasMaxGatewayInflight || name == "max_gateway_inflight"
		hasMaxWaiting = hasMaxWaiting || name == "max_waiting"
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate model_pools schema: %v", err)
	}
	if hasMaxGatewayInflight {
		t.Fatal("max_gateway_inflight persisted despite migration rollback")
	}
	if !hasMaxWaiting {
		t.Fatal("pre-existing max_waiting column was lost after migration rollback")
	}

	var publicModelName, upstreamModelName string
	var maxWaiting int
	if err := inspectDB.QueryRow(`
		SELECT public_model_name, upstream_model_name, max_waiting
		FROM model_pools`).Scan(&publicModelName, &upstreamModelName, &maxWaiting); err != nil {
		t.Fatalf("read legacy pool after failed migration: %v", err)
	}
	if publicModelName != "legacy-public" || upstreamModelName != "legacy-upstream" || maxWaiting != 23 {
		t.Fatalf("legacy pool after failed migration = (%q, %q, %d)", publicModelName, upstreamModelName, maxWaiting)
	}
}

func TestSQLiteMigratesLegacyVersionZeroDatabaseWithExistingTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	createLegacyPoolDatabase(t, path, 0)

	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() legacy version-zero database error = %v", err)
	}
	defer db.Close()
	pools, err := db.ListPools(context.Background())
	if err != nil {
		t.Fatalf("ListPools() error = %v", err)
	}
	if len(pools) != 1 || pools[0].PublicModelName != "legacy-public" {
		t.Fatalf("legacy pool was not preserved: %+v", pools)
	}
	if pools[0].MaxGatewayInflight != 0 || pools[0].MaxWaiting != 0 {
		t.Fatalf("legacy safety limits = (%d, %d), want (0, 0)", pools[0].MaxGatewayInflight, pools[0].MaxWaiting)
	}
	if got := sqliteUserVersion(t, path); got != 2 {
		t.Fatalf("legacy user_version = %d, want 2", got)
	}
}

func createLegacyPoolDatabase(t *testing.T, path string, version int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(initialMigration); err != nil {
		db.Close()
		t.Fatalf("apply initial migration: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO model_pools (public_model_name, upstream_model_name, enabled, created_at, updated_at)
		VALUES ('legacy-public', 'legacy-upstream', 1, '2026-08-27T00:00:00Z', '2026-08-27T00:00:00Z')`); err != nil {
		db.Close()
		t.Fatalf("insert legacy pool: %v", err)
	}
	if _, err := db.Exec("PRAGMA user_version = " + strconv.Itoa(version)); err != nil {
		db.Close()
		t.Fatalf("set legacy user_version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}
}

func sqliteUserVersion(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	return version
}

func TestSQLiteCRUDSnapshotAndRevision(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pool, err := db.CreatePool(ctx, store.CreatePoolParams{
		PublicModelName: "qwen-coder", UpstreamModelName: "Qwen/Qwen3-Coder-Next", Enabled: true,
		MaxGatewayInflight: 17, MaxWaiting: 9,
	})
	if err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	client, err := db.CreateClient(ctx, store.CreateClientParams{
		Name: "codex-ci", Enabled: true, PriorityClass: domain.PriorityBackground,
		VLLMPriority: 100, MaxConcurrency: 20, ModelPoolIDs: []int64{pool.ID},
	})
	if err != nil {
		t.Fatalf("CreateClient() error = %v", err)
	}
	backend, err := db.CreateBackend(ctx, store.CreateBackendParams{
		ModelPoolID: pool.ID, Name: "gpu-1", BaseURL: "http://127.0.0.1:8000/",
		Enabled: true, CapacityHint: 1, RunningSoftLimit: 8,
	})
	if err != nil {
		t.Fatalf("CreateBackend() error = %v", err)
	}
	if backend.BaseURL != "http://127.0.0.1:8000" {
		t.Fatalf("BaseURL = %q", backend.BaseURL)
	}

	digest := sha256.Sum256([]byte("digest only, never the plaintext key"))
	key, err := db.CreateAPIKey(ctx, store.CreateAPIKeyParams{
		ClientID: client.ID, Prefix: "llmgw_abcd12", SecretHash: digest,
	})
	if err != nil {
		t.Fatalf("CreateAPIKey() error = %v", err)
	}
	if err := db.SetBackendDraining(ctx, backend.ID, true); err != nil {
		t.Fatalf("SetBackendDraining() error = %v", err)
	}

	data, err := db.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if data.Revision != 5 {
		t.Fatalf("Revision = %d, want 5", data.Revision)
	}
	if len(data.Clients) != 1 || len(data.Pools) != 1 || len(data.Backends) != 1 || len(data.Keys) != 1 || len(data.Access) != 1 {
		t.Fatalf("unexpected snapshot sizes: %+v", data)
	}
	if data.Keys[0].SecretHash != digest || data.Keys[0].ID != key.ID {
		t.Fatalf("key snapshot = %+v", data.Keys[0])
	}
	if !data.Backends[0].Draining {
		t.Fatal("backend draining state was not persisted")
	}
	if data.Pools[0].MaxGatewayInflight != 17 || data.Pools[0].MaxWaiting != 9 {
		t.Fatalf("pool safety snapshot = (%d, %d), want (17, 9)", data.Pools[0].MaxGatewayInflight, data.Pools[0].MaxWaiting)
	}
}

func TestSQLiteRevocationExpiryAndLastUsed(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	client, err := db.CreateClient(ctx, store.CreateClientParams{
		Name: "developer", Enabled: true, PriorityClass: domain.PriorityHigh,
		VLLMPriority: -10, MaxConcurrency: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	digest := sha256.Sum256([]byte("digest"))
	key, err := db.CreateAPIKey(ctx, store.CreateAPIKeyParams{
		ClientID: client.ID, Prefix: "llmgw_expire", SecretHash: digest, ExpiresAt: &expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	used := time.Now().UTC().Truncate(time.Microsecond)
	if err := db.TouchKeyLastUsed(ctx, key.ID, used); err != nil {
		t.Fatal(err)
	}
	if err := db.RevokeAPIKey(ctx, key.ID); err != nil {
		t.Fatal(err)
	}
	keys, err := db.ListAPIKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].ExpiresAt == nil || !keys[0].ExpiresAt.Equal(expires) || keys[0].LastUsedAt == nil || !keys[0].LastUsedAt.Equal(used) || keys[0].RevokedAt == nil {
		t.Fatalf("key lifecycle = %+v", keys)
	}
}

func TestSQLiteRejectsBrokenReferencesAndDuplicates(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	_, err := db.CreateBackend(ctx, store.CreateBackendParams{
		ModelPoolID: 999, Name: "orphan", BaseURL: "http://127.0.0.1:8000",
		Enabled: true, CapacityHint: 1, RunningSoftLimit: 1,
	})
	if err == nil {
		t.Fatal("CreateBackend() accepted missing pool")
	}
	_, err = db.CreatePool(ctx, store.CreatePoolParams{PublicModelName: "model", UpstreamModelName: "upstream", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CreatePool(ctx, store.CreatePoolParams{PublicModelName: "model", UpstreamModelName: "other", Enabled: true})
	if err == nil {
		t.Fatal("CreatePool() accepted duplicate public name")
	}
}

func TestSQLiteNeverReceivesPlaintextKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	client, err := db.CreateClient(ctx, store.CreateClientParams{
		Name: "secure", Enabled: true, PriorityClass: domain.PriorityNormal, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("llmgw_secret-value"))
	if _, err := db.CreateAPIKey(ctx, store.CreateAPIKeyParams{ClientID: client.ID, Prefix: "llmgw_secret", SecretHash: digest}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{db.Path(), db.Path() + "-wal"} {
		contents, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if string(contents) != "" && contains(contents, []byte("llmgw_secret-value")) {
			t.Fatalf("plaintext key found in %s", path)
		}
	}
}

func contains(data, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for offset := 0; offset+len(needle) <= len(data); offset++ {
		match := true
		for index := range needle {
			if data[offset+index] != needle[index] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
