package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestRequestSizeMigrationPreservesLegacyLeasesAndGuardsDown(t *testing.T) {
	dsn := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close(context.Background())
	schema := pgx.Identifier{"request_size_migration_" + uuid.New().String()}.Sanitize()
	if _, e := conn.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	defer conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	if _, e := conn.Exec(ctx, "SET search_path TO "+schema); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"000001_configuration", "000002_analytics", "000003_coordination", "000004_admission_decision_scope", "000005_priority_capacity_reserve", "000006_tokenization"} {
		sql, e := migrationFS.ReadFile("migrations/" + name + ".up.sql")
		if e != nil {
			t.Fatal(e)
		}
		if _, e := conn.Exec(ctx, string(sql)); e != nil {
			t.Fatal(e)
		}
	}
	lease, replica := uuid.New(), uuid.New()
	if _, e := conn.Exec(ctx, "INSERT INTO model_pools(public_model_name,upstream_model_name,enabled,max_gateway_inflight,created_at,updated_at) VALUES('model','upstream',true,20,clock_timestamp(),clock_timestamp())"); e != nil {
		t.Fatal(e)
	}
	if _, e := conn.Exec(ctx, "INSERT INTO admission_operations(lease_id,operation_started_at,input_fingerprint,request_id,replica_id,client_id,pool_id,decision) VALUES($1,clock_timestamp(),decode('00','hex'),'request',$2,1,1,'admitted')", lease, replica); e != nil {
		t.Fatal(e)
	}
	if _, e := conn.Exec(ctx, "INSERT INTO request_leases(lease_id,request_id,replica_id,client_id,pool_id,acquired_at,expires_at) VALUES($1,'request',$2,1,1,clock_timestamp(),clock_timestamp()+interval '1 hour')", lease, replica); e != nil {
		t.Fatal(e)
	}
	up, e := migrationFS.ReadFile("migrations/000007_request_size_admission.up.sql")
	if e != nil {
		t.Fatal(e)
	}
	down, e := migrationFS.ReadFile("migrations/000007_request_size_admission.down.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e := conn.Exec(ctx, string(up)); e != nil {
		t.Fatal(e)
	}
	var disabled bool
	var reserved *int64
	if err := conn.QueryRow(ctx, "SELECT coalesce((request_size_admission->>'Enabled')::boolean, false) FROM model_pools").Scan(&disabled); err != nil || disabled {
		t.Fatalf("legacy pool enabled = %v, err = %v", disabled, err)
	}
	if err := conn.QueryRow(ctx, "SELECT reserved_tokens FROM request_leases").Scan(&reserved); err != nil || reserved != nil {
		t.Fatalf("legacy reservation = %v, err = %v", reserved, err)
	}
	if _, err := conn.Exec(ctx, "UPDATE request_leases SET reserved_tokens=600"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(down)); err == nil {
		t.Fatal("down migration discarded a live reservation")
	}
	if _, err := conn.Exec(ctx, "UPDATE request_leases SET expires_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(down)); err != nil {
		t.Fatalf("downgrade after expiry: %v", err)
	}
}
