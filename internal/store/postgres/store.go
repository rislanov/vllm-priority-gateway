package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	DatabaseURL          string
	MigrationURL         string
	ConfigMaxConns       int
	AnalyticsMaxConns    int
	CoordinationMaxConns int
}

type Store struct {
	config       *pgxpool.Pool
	analytics    *pgxpool.Pool
	coordination *pgxpool.Pool
	migrationURL string
	closeOnce    sync.Once
}

func Open(ctx context.Context, options Options) (*Store, error) {
	if options.MigrationURL == "" {
		options.MigrationURL = options.DatabaseURL
	}
	config, err := openPool(ctx, options.DatabaseURL, options.ConfigMaxConns)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL configuration pool: %w", err)
	}
	analytics, err := openPool(ctx, options.DatabaseURL, options.AnalyticsMaxConns)
	if err != nil {
		config.Close()
		return nil, fmt.Errorf("open PostgreSQL analytics pool: %w", err)
	}
	coordination, err := openPool(ctx, options.DatabaseURL, options.CoordinationMaxConns)
	if err != nil {
		analytics.Close()
		config.Close()
		return nil, fmt.Errorf("open PostgreSQL coordination pool: %w", err)
	}
	s := &Store{config: config, analytics: analytics, coordination: coordination, migrationURL: options.MigrationURL}
	if err := s.Migrate(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func openPool(ctx context.Context, raw string, max int) (*pgxpool.Pool, error) {
	cfg, err := ParsePoolConfig(raw, max)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("connect to PostgreSQL")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("ping PostgreSQL")
	}
	return pool, nil
}
func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.coordination.Close(); s.analytics.Close(); s.config.Close() })
	return nil
}
func (s *Store) ConfigPool() *pgxpool.Pool       { return s.config }
func (s *Store) AnalyticsPool() *pgxpool.Pool    { return s.analytics }
func (s *Store) CoordinationPool() *pgxpool.Pool { return s.coordination }

type PoolConnectionStats struct {
	Acquired, Idle, Total int32
	Canceled              int64
}

func (s *Store) PoolStats() map[string]PoolConnectionStats {
	return map[string]PoolConnectionStats{
		"configuration": poolConnectionStats(s.config),
		"analytics":     poolConnectionStats(s.analytics),
		"coordination":  poolConnectionStats(s.coordination),
	}
}

func poolConnectionStats(pool *pgxpool.Pool) PoolConnectionStats {
	stats := pool.Stat()
	return PoolConnectionStats{Acquired: stats.AcquiredConns(), Idle: stats.IdleConns(), Total: stats.TotalConns(), Canceled: stats.CanceledAcquireCount()}
}
