package store_test

import (
	"context"
	"database/sql"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
	"testing"
)

func TestSQLitePriorityReserveRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	p, err := db.CreatePool(ctx, store.CreatePoolParams{PublicModelName: "reserved", UpstreamModelName: "upstream", Enabled: true, MaxGatewayInflight: 20, HighPriorityReserve: 6})
	if err != nil {
		t.Fatal(err)
	}
	pools, err := db.ListPools(ctx)
	if err != nil || len(pools) != 1 || pools[0].HighPriorityReserve != 6 {
		t.Fatalf("list=%+v err=%v", pools, err)
	}
	params := store.UpdatePoolParams{PublicModelName: p.PublicModelName, UpstreamModelName: p.UpstreamModelName, Enabled: true, MaxGatewayInflight: 20, HighPriorityReserve: 20}
	if _, err = db.UpdatePool(ctx, p.ID, params); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.LoadSnapshot(ctx)
	if err != nil || snapshot.Pools[0].HighPriorityReserve != 20 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	reopened, err := store.Open(ctx, db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	pools, err = reopened.ListPools(ctx)
	if err != nil || pools[0].HighPriorityReserve != 20 {
		t.Fatalf("reopen=%+v err=%v", pools, err)
	}
	raw, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	for _, query := range []string{
		"UPDATE model_pools SET high_priority_reserve=-1",
		"UPDATE model_pools SET high_priority_reserve=21",
		"UPDATE model_pools SET max_gateway_inflight=0",
		"INSERT INTO model_pools(public_model_name,upstream_model_name,enabled,max_gateway_inflight,high_priority_reserve,created_at,updated_at) VALUES('bad','upstream',1,0,1,'now','now')",
	} {
		if _, err := raw.ExecContext(ctx, query); err == nil {
			t.Fatalf("invalid SQL accepted: %s", query)
		}
	}
}
