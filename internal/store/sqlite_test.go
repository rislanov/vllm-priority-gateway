package store_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

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

func TestSQLiteCRUDSnapshotAndRevision(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	pool, err := db.CreatePool(ctx, store.CreatePoolParams{
		PublicModelName: "qwen-coder", UpstreamModelName: "Qwen/Qwen3-Coder-Next", Enabled: true,
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
