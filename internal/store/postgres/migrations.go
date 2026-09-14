package postgres

import (
	"embed"
	"io/fs"
	"sort"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

func MigrationManifest() ([]string, error) {
	entries, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		vi, vj := entries[i][:17], entries[j][:17]
		if vi != vj {
			return vi < vj
		}
		return entries[i] > entries[j] // .up.sql before .down.sql
	})
	return entries, nil
}
