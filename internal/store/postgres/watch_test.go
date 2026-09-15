package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type fakeNotificationConn struct {
	waits  atomic.Int32
	closes atomic.Int32
	exec   func(context.Context) error
	wait   func(context.Context, int32) (*pgconn.Notification, error)
	close  func(context.Context) error
}

func (c *fakeNotificationConn) Exec(ctx context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	if c.exec != nil {
		return pgconn.CommandTag{}, c.exec(ctx)
	}
	return pgconn.NewCommandTag("LISTEN"), nil
}
func (c *fakeNotificationConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	return c.wait(ctx, c.waits.Add(1))
}
func (c *fakeNotificationConn) Close(ctx context.Context) error {
	c.closes.Add(1)
	if c.close != nil {
		return c.close(ctx)
	}
	return nil
}

func TestWatchPollingSurvivesStalledNotificationOperations(t *testing.T) {
	for _, operation := range []string{"connect", "listen", "close_after_listen_failure", "close_after_session_failure"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			var reloads atomic.Int32
			done := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("watcher did not stop after cancellation")
				}
			})
			stall := func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}
			connect := func(connectCtx context.Context, _ string) (notificationConn, error) {
				if operation == "connect" {
					return nil, stall(connectCtx)
				}
				conn := &fakeNotificationConn{wait: func(context.Context, int32) (*pgconn.Notification, error) {
					return nil, errors.New("notification session lost")
				}}
				switch operation {
				case "listen":
					conn.exec = stall
				case "close_after_listen_failure":
					conn.exec = func(context.Context) error { return errors.New("LISTEN failed") }
					conn.close = stall
				case "close_after_session_failure":
					conn.close = stall
				}
				return conn, nil
			}
			go func() {
				defer close(done)
				(&Store{}).watchWithConnector(ctx, 10*time.Millisecond, "llmgw_config_changed", func(context.Context) error {
					if reloads.Add(1) == 4 {
						cancel()
					}
					return nil
				}, connect, WatchHooks{})
			}()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("runtime polling stalled in notification %s: reloads=%d", operation, reloads.Load())
			}
			if got := reloads.Load(); got < 4 {
				t.Fatalf("runtime polling stopped after %d reloads", got)
			}
		})
	}
}

func TestWatchPollsWhileNotificationConnectionIsUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reloads atomic.Int32
	var connects atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&Store{}).watchWithConnector(ctx, time.Millisecond, "llmgw_config_changed", func(context.Context) error {
			if reloads.Add(1) == 3 {
				cancel()
			}
			return nil
		}, func(context.Context, string) (notificationConn, error) {
			connects.Add(1)
			return nil, errors.New("notification session unavailable")
		}, WatchHooks{})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher stopped polling after connection failures")
	}
	if reloads.Load() < 3 || connects.Load() < 2 {
		t.Fatalf("reloads=%d connects=%d", reloads.Load(), connects.Load())
	}
}

func TestWatchReloadsAcrossNotificationLossAndReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reloads atomic.Int32
	var connects atomic.Int32
	var reconnects atomic.Int32
	var successes atomic.Int32
	connections := make([]*fakeNotificationConn, 0, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&Store{}).watchWithConnector(ctx, time.Millisecond, "llmgw_circuit_changed", func(context.Context) error {
			if reloads.Add(1) >= 5 {
				cancel()
			}
			return nil
		}, func(context.Context, string) (notificationConn, error) {
			connects.Add(1)
			conn := &fakeNotificationConn{wait: func(context.Context, int32) (*pgconn.Notification, error) {
				return nil, errors.New("notification connection lost")
			}}
			connections = append(connections, conn)
			return conn, nil
		}, WatchHooks{NotificationReconnect: func() { reconnects.Add(1) }, ReloadSuccess: func(time.Time) { successes.Add(1) }})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher did not reconnect")
	}
	if reloads.Load() < 5 || connects.Load() < 2 {
		t.Fatalf("reloads=%d connects=%d", reloads.Load(), connects.Load())
	}
	if reconnects.Load() < 1 || successes.Load() != reloads.Load() {
		t.Fatalf("reconnects=%d successes=%d reloads=%d", reconnects.Load(), successes.Load(), reloads.Load())
	}
	for index, conn := range connections {
		if conn.closes.Load() != 1 {
			t.Fatalf("connection %d closes=%d", index, conn.closes.Load())
		}
	}
}

func TestWatchTreatsNotificationAsReloadHint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reloads atomic.Int32
	conn := &fakeNotificationConn{wait: func(ctx context.Context, wait int32) (*pgconn.Notification, error) {
		if wait == 1 {
			return &pgconn.Notification{Channel: "llmgw_config_changed"}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&Store{}).watchWithConnector(ctx, time.Hour, "llmgw_config_changed", func(context.Context) error {
			if reloads.Add(1) == 3 {
				cancel()
			}
			return nil
		}, func(context.Context, string) (notificationConn, error) {
			return conn, nil
		}, WatchHooks{})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification did not trigger reload")
	}
	if got := reloads.Load(); got != 3 {
		t.Fatalf("reloads=%d, want 3 (poll, post-LISTEN gap close, notification)", got)
	}
}
