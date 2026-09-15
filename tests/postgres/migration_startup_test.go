package postgres_test

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

// Startup tests cannot use openTestStore: it applies migrations before returning.
// An isolated search_path starts empty even when other tests migrated the database.
func emptyMigrationSchema(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse PostgreSQL test URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "migration_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		_ = conn.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		defer conn.Close(cleanupCtx)
		if _, err := conn.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop migration test schema: %v", err)
		}
	})
	var relations int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relnamespace=$1::regnamespace", schema).Scan(&relations); err != nil {
		t.Fatal(err)
	}
	if relations != 0 {
		t.Fatalf("startup schema contains %d relations before migrations", relations)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func assertCurrentMigrationSchema(t *testing.T, ctx context.Context, store *pgstore.Store) {
	t.Helper()
	manifest, err := pgstore.MigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	var latest int64
	for _, name := range manifest {
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		version, err := strconv.ParseInt(strings.SplitN(strings.TrimPrefix(name, "migrations/"), "_", 2)[0], 10, 64)
		if err != nil {
			t.Fatalf("migration version in %q: %v", name, err)
		}
		if version > latest {
			latest = version
		}
	}
	var version int64
	var dirty bool
	if err := store.ConfigPool().QueryRow(ctx, "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		t.Fatal(err)
	}
	if latest == 0 || version != latest || dirty {
		t.Errorf("migration state = (%d, dirty=%t), want (%d, false)", version, dirty, latest)
	}
}

func TestPostgresFutureMigrationRefusesStartup(t *testing.T) {
	dsn := emptyMigrationSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	options := pgstore.Options{
		DatabaseURL: dsn, MigrationURL: dsn,
		ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 2,
	}
	store, err := pgstore.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	assertCurrentMigrationSchema(t, ctx, store)
	var futureVersion int64
	if err := store.ConfigPool().QueryRow(ctx, "UPDATE schema_migrations SET version=version+1 RETURNING version").Scan(&futureVersion); err != nil {
		t.Fatal(err)
	}
	opened, err := pgstore.Open(ctx, options)
	if opened != nil {
		_ = opened.Close()
		t.Error("future migration state returned a usable store")
	}
	if err == nil {
		t.Error("schema newer than the binary allowed startup")
	}
	var version int64
	var dirty bool
	if err := store.ConfigPool().QueryRow(ctx, "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		t.Fatal(err)
	}
	if version != futureVersion || dirty {
		t.Errorf("refused startup changed future migration state to (%d, dirty=%t), want (%d, false)", version, dirty, futureVersion)
	}
}
