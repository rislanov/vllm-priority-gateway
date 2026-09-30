package postgres

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"os"
	"testing"
	"time"
)

func TestPriorityReserveMigrationBackfillsLegacyLeaseAndGuardsDown(t *testing.T) {
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
	schema := pgx.Identifier{"reserve_migration_" + uuid.New().String()}.Sanitize()
	if _, e := conn.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	defer conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	if _, e := conn.Exec(ctx, "SET search_path TO "+schema); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"000001_configuration", "000002_analytics", "000003_coordination", "000004_admission_decision_scope"} {
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
	up, e := migrationFS.ReadFile("migrations/000005_priority_capacity_reserve.up.sql")
	if e != nil {
		t.Fatal(e)
	}
	down, e := migrationFS.ReadFile("migrations/000005_priority_capacity_reserve.down.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e := conn.Exec(ctx, string(up)); e != nil {
		t.Fatal(e)
	}
	var reserve int
	var class string
	if e := conn.QueryRow(ctx, "SELECT high_priority_reserve FROM model_pools").Scan(&reserve); e != nil || reserve != 0 {
		t.Fatal(reserve, e)
	}
	if e := conn.QueryRow(ctx, "SELECT priority_class FROM request_leases").Scan(&class); e != nil || class != "normal" {
		t.Fatal(class, e)
	}
	if _, e := conn.Exec(ctx, "UPDATE model_pools SET high_priority_reserve=6"); e != nil {
		t.Fatal(e)
	}
	if _, e := conn.Exec(ctx, string(down)); e == nil {
		t.Fatal("down discarded an enabled reserve")
	}
	if _, e := conn.Exec(ctx, "UPDATE model_pools SET high_priority_reserve=0"); e != nil {
		t.Fatal(e)
	}
	if _, e := conn.Exec(ctx, string(down)); e != nil {
		t.Fatal(e)
	}
	if _, e := conn.Exec(ctx, string(up)); e != nil {
		t.Fatal(e)
	}
	if e := conn.QueryRow(ctx, "SELECT priority_class FROM request_leases").Scan(&class); e != nil || class != "normal" {
		t.Fatal(class, e)
	}
}
