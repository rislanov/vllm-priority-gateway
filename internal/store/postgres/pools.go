package postgres

import (
	"errors"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ParsePoolConfig validates a runtime DSN without ever wrapping parser errors,
// because those errors can contain credentials from the original URL.
func ParsePoolConfig(rawURL string, maxConns int) (*pgxpool.Config, error) {
	if maxConns <= 0 {
		return nil, errors.New("PostgreSQL max connections must be positive")
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return nil, errors.New("invalid PostgreSQL connection URL")
	}
	if mode := parsed.Query().Get("default_query_exec_mode"); mode != "" && mode != "exec" {
		return nil, errors.New("PostgreSQL runtime URL requires default_query_exec_mode=exec")
	}
	cfg, err := pgxpool.ParseConfig(rawURL)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL connection URL")
	}
	cfg.MaxConns = int32(maxConns)
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	return cfg, nil
}
