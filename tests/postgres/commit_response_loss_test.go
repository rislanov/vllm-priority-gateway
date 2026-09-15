package postgres_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/circuitbreaker"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
)

// Cut the connection after PostgreSQL confirms COMMIT, before pgx receives it.
// This exercises an ambiguous transport result, not a manually repeated call.
func commitLossProxy(t *testing.T, raw string) (string, *atomic.Bool) {
	t.Helper()
	target, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	armed := &atomic.Bool{}
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				server, err := net.DialTimeout("tcp", target.Host, time.Second)
				if err != nil {
					return
				}
				defer server.Close()
				go func() { _, _ = io.Copy(server, client); _ = server.Close() }()
				for {
					header := make([]byte, 5)
					if _, err := io.ReadFull(server, header); err != nil {
						return
					}
					size := binary.BigEndian.Uint32(header[1:])
					if size < 4 || size > 16<<20 {
						return
					}
					payload := make([]byte, size-4)
					if _, err := io.ReadFull(server, payload); err != nil {
						return
					}
					if header[0] == 'C' && string(payload) == "COMMIT\x00" && armed.CompareAndSwap(true, false) {
						return
					}
					if _, err := client.Write(append(header, payload...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	proxy := *target
	proxy.Host = listener.Addr().String()
	query := proxy.Query()
	query.Set("sslmode", "disable")
	proxy.RawQuery = query.Encode()
	return proxy.String(), armed
}

func TestPostgresAdmissionRetriesLostCommitResponse(t *testing.T) {
	raw := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if raw == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	runtime, armed := commitLossProxy(t, raw)
	store := openTestStoreWithMigration(t, runtime, raw)
	f := createFixture(t, store, 10)
	coordinator := coordpostgres.NewAdmissionCoordinator(store, 2*time.Second)
	request := admissionRequest(f, uuid.New())
	armed.Store(true)
	decision, err := coordinator.Acquire(context.Background(), request)
	if armed.Load() {
		t.Fatal("proxy did not intercept COMMIT")
	}
	if err != nil || !decision.Admitted() || decision.Lease.LeaseID != request.LeaseID {
		t.Fatalf("lost COMMIT response was not recovered: decision=%+v err=%v", decision, err)
	}
	var leases int
	var balance float64
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT count(*) FROM request_leases").Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT balance FROM coordination_rate_state WHERE rate_kind='rpm'").Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if leases != 1 || balance != 9 {
		t.Fatalf("leases=%d RPM balance=%v; want one lease and one debit", leases, balance)
	}
}

func TestPostgresProbeRetriesLostCommitResponse(t *testing.T) {
	raw := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	if raw == "" {
		t.Skip("LLMGW_POSTGRES_TEST_DSN is not set")
	}
	runtime, armed := commitLossProxy(t, raw)
	store := openTestStoreWithMigration(t, runtime, raw)
	f := createFixture(t, store, 0)
	options := circuitbreaker.Options{FailureThreshold: 1, FailureWindow: time.Minute, OpenCooldown: time.Second, HalfOpenMaxProbes: 1}
	coordinator, err := coordpostgres.NewCircuitCoordinator(store, 2*time.Second, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CoordinationPool().Exec(context.Background(), "UPDATE backend_circuit_state SET state='half_open', generation=1 WHERE backend_id=$1", f.backend.ID); err != nil {
		t.Fatal(err)
	}
	request := coordination.CircuitAcquireRequest{AttemptID: uuid.New(), AcquisitionStartedAt: time.Now().UTC(), ReplicaID: uuid.New(), Backend: coordination.BackendIdentity{ID: f.backend.ID, Revision: f.backend.Revision, Enabled: true}, ProbeTTL: time.Minute}
	armed.Store(true)
	decision, err := coordinator.Acquire(context.Background(), request)
	if armed.Load() {
		t.Fatal("proxy did not intercept COMMIT")
	}
	if err != nil || decision.Permit == nil || decision.Permit.PermitID != request.AttemptID {
		t.Fatalf("lost probe COMMIT response was not recovered: decision=%+v err=%v", decision, err)
	}
	var permits int
	if err := store.CoordinationPool().QueryRow(context.Background(), "SELECT count(*) FROM backend_circuit_probes").Scan(&permits); err != nil {
		t.Fatal(err)
	}
	if permits != 1 {
		t.Fatalf("permits=%d, want 1", permits)
	}
}
