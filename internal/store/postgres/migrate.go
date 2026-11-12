package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func (s *Store) Migrate(ctx context.Context) error {
	cfg, err := pgx.ParseConfig(s.migrationURL)
	if err != nil {
		return errors.New("invalid PostgreSQL migration URL")
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return errors.New("connect to PostgreSQL migration endpoint")
	}
	var versionText string
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&versionText); err != nil {
		return errors.New("read PostgreSQL version")
	}
	version, err := strconv.Atoi(versionText)
	if err != nil || version < 160000 {
		return fmt.Errorf("PostgreSQL 16 or newer is required")
	}
	return runMigrations(db)
}

// runMigrations is called on every PostgreSQL startup. Before adding a migration,
// verify restart/rollback of supported older binaries: migrate.Up requires the
// current DB version in their embedded source, even for additive schema changes.
// See docs/coordination-compatibility.md; SQL compatibility alone is insufficient.
func runMigrations(db *sql.DB) error {
	source, err := iofs.New(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return errors.New("initialize PostgreSQL migration driver")
	}
	m, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		return errors.New("initialize PostgreSQL migrations")
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return errors.New("apply PostgreSQL migrations")
	}
	return nil
}
