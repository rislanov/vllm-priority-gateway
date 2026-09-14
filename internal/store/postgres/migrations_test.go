package postgres

import (
	"reflect"
	"testing"
)

func TestEmbeddedMigrationManifestHasContiguousUpDownPairs(t *testing.T) {
	manifest, err := MigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"migrations/000001_configuration.up.sql",
		"migrations/000001_configuration.down.sql",
		"migrations/000002_analytics.up.sql",
		"migrations/000002_analytics.down.sql",
		"migrations/000003_coordination.up.sql",
		"migrations/000003_coordination.down.sql",
	}
	if !reflect.DeepEqual(manifest, want) {
		t.Fatalf("MigrationManifest() = %v, want %v", manifest, want)
	}
}
