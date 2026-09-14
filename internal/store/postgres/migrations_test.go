package postgres

import "testing"

func TestMigrationManifestIsCompleteAndOrdered(t *testing.T) {
	want := []string{
		"migrations/000001_configuration.up.sql", "migrations/000001_configuration.down.sql",
		"migrations/000002_analytics.up.sql", "migrations/000002_analytics.down.sql",
		"migrations/000003_coordination.up.sql", "migrations/000003_coordination.down.sql",
	}
	got, err := MigrationManifest()
	if err != nil {
		t.Fatalf("MigrationManifest() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("manifest = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("manifest[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
