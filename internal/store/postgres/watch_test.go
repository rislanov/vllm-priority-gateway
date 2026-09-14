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
	wait   func(context.Context, int32) (*pgconn.Notification, error)
}

func (*fakeNotificationConn) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("LISTEN"), nil
}
func (c *fakeNotificationConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	return c.wait(ctx, c.waits.Add(1))
}
func (c *fakeNotificationConn) Close(context.Context) error {
	c.closes.Add(1)
	return nil
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
