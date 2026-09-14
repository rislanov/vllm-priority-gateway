package postgres_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
	coordpostgres "github.com/rislanov/vllm-priority-gateway/internal/coordination/postgres"
	pgstore "github.com/rislanov/vllm-priority-gateway/internal/store/postgres"
)

func delayedPostgresDSN(t *testing.T, raw string, delay time.Duration) string {
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
	go func() {
		for {
			downstream, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer downstream.Close()
				upstream, dialErr := net.Dial("tcp", target.Host)
				if dialErr != nil {
					return
				}
				defer upstream.Close()
				requestCopied := make(chan struct{})
				go func() {
					_, _ = io.Copy(upstream, downstream)
					_ = upstream.Close()
					close(requestCopied)
				}()
				buffer := make([]byte, 64*1024)
				for {
					n, readErr := upstream.Read(buffer)
					if n > 0 {
						time.Sleep(delay)
						if _, writeErr := downstream.Write(buffer[:n]); writeErr != nil {
							break
						}
					}
					if readErr != nil {
						break
					}
				}
				_ = downstream.Close()
				<-requestCopied
			}()
		}
	}()
	delayed := *target
	delayed.Host = listener.Addr().String()
	return delayed.String()
}

func TestPostgresCompletionBatchReturnsCommittedPrefixForRetryProgress(t *testing.T) {
	raw := os.Getenv("LLMGW_POSTGRES_TEST_DSN")
	store := openTestStore(t, raw)
	fixture := updateFixtureLimits(t, store, createFixture(t, store, 0), 64, 64)
	setup := coordpostgres.NewAdmissionCoordinator(store, 2*time.Second)
	ctx := context.Background()
	items := make([]coordination.LeaseCompletion, 0, 64)
	for range 64 {
		decision, err := setup.Acquire(ctx, admissionRequest(fixture, uuid.New()))
		if err != nil || !decision.Admitted() {
			t.Fatalf("setup admission: decision=%+v err=%v", decision, err)
		}
		items = append(items, coordination.LeaseCompletion{Lease: decision.Lease.Identity()})
	}

	delayedStore, err := pgstore.Open(ctx, pgstore.Options{
		DatabaseURL: delayedPostgresDSN(t, raw, 2*time.Millisecond), MigrationURL: raw,
		ConfigMaxConns: 2, AnalyticsMaxConns: 2, CoordinationMaxConns: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer delayedStore.Close()
	coordinator := coordpostgres.NewAdmissionCoordinator(delayedStore, 200*time.Millisecond)

	remaining := items
	sawPartialProgress := false
	for attempt := 1; attempt <= 16 && len(remaining) > 0; attempt++ {
		attemptSize := len(remaining)
		results, completionErr := coordinator.Complete(ctx, remaining)
		if len(results) > len(remaining) {
			t.Fatalf("attempt %d returned %d results for %d completions", attempt, len(results), len(remaining))
		}
		for i, result := range results {
			if result.LeaseID != remaining[i].Lease.LeaseID {
				t.Fatalf("attempt %d result %d lease=%s, want prefix lease=%s", attempt, i, result.LeaseID, remaining[i].Lease.LeaseID)
			}
		}
		if completionErr != nil && len(results) > 0 && len(results) < attemptSize {
			sawPartialProgress = true
		}
		remaining = remaining[len(results):]
		if completionErr == nil && len(remaining) > 0 {
			t.Fatalf("attempt %d reported success with %d completions missing", attempt, len(remaining))
		}
	}
	if !sawPartialProgress {
		t.Fatal("latency-controlled completion never returned a committed prefix with its timeout")
	}
	if len(remaining) != 0 {
		t.Fatalf("completion retries made no durable progress; %d of %d items remain", len(remaining), len(items))
	}
	var completedReceipts, activeLeases int
	if err := store.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM admission_operations WHERE completed_at IS NOT NULL").Scan(&completedReceipts); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigPool().QueryRow(ctx, "SELECT count(*) FROM request_leases").Scan(&activeLeases); err != nil {
		t.Fatal(err)
	}
	if completedReceipts != len(items) || activeLeases != 0 {
		t.Fatalf("durable completion state: receipts=%d leases=%d, want %d receipts and 0 leases", completedReceipts, activeLeases, len(items))
	}
}
