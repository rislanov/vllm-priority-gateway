package postgres

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
)

var (
	_ basestore.ConfigurationStore = (*Store)(nil)
	_ basestore.AnalyticsStore     = (*Store)(nil)
	_ basestore.LifecycleStore     = (*Store)(nil)
)

func TestParsePoolConfigForcesExecModeAndBoundsConnections(t *testing.T) {
	cfg, err := ParsePoolConfig("postgres://gateway:secret@db.example/llmgw?sslmode=require", 7)
	if err != nil {
		t.Fatalf("ParsePoolConfig() error = %v", err)
	}
	if cfg.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeExec {
		t.Fatalf("mode = %v", cfg.ConnConfig.DefaultQueryExecMode)
	}
	if cfg.MaxConns != 7 {
		t.Fatalf("MaxConns = %d", cfg.MaxConns)
	}
}

func TestParsePoolConfigRejectsIncompatibleModeWithoutLeakingDSN(t *testing.T) {
	const dsn = "postgres://gateway:super-secret@db.example/llmgw?default_query_exec_mode=cache_statement"
	_, err := ParsePoolConfig(dsn, 4)
	if err == nil {
		t.Fatal("expected incompatible mode error")
	}
	if strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), dsn) {
		t.Fatalf("error leaked DSN: %v", err)
	}
}

func TestParsePoolConfigRejectsNonPositiveMaxConnections(t *testing.T) {
	if _, err := ParsePoolConfig("postgres://db.example/llmgw", 0); err == nil {
		t.Fatal("expected max connection error")
	}
}
