package store

import (
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSQLiteDSNUsesAbsoluteFileURL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db with space.sqlite")
	dsn, err := sqliteDSN(path)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "file" || parsed.Host != "" {
		t.Fatalf("SQLite DSN = %q, scheme=%q host=%q", dsn, parsed.Scheme, parsed.Host)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.ToSlash(absolute)
	if filepath.VolumeName(absolute) != "" && !strings.HasPrefix(wantPath, "/") {
		wantPath = "/" + wantPath
	}
	if parsed.Path != wantPath {
		t.Fatalf("SQLite URI path = %q, want %q", parsed.Path, wantPath)
	}
	for _, pragma := range []string{"journal_mode(WAL)", "foreign_keys(1)", "busy_timeout(5000)"} {
		if !slices.Contains(parsed.Query()["_pragma"], pragma) {
			t.Errorf("SQLite DSN lacks pragma %q: %q", pragma, dsn)
		}
	}
}
