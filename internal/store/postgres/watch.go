package postgres

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type notificationConn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	WaitForNotification(context.Context) (*pgconn.Notification, error)
	Close(context.Context) error
}

type notificationConnector func(context.Context, string) (notificationConn, error)

type WatchHooks struct {
	NotificationReconnect func()
	ReloadSuccess         func(time.Time)
	ReloadFailure         func(error)
}

// WatchConfig treats notifications as hints and polling as authoritative. A
// failed session connection never stops polling, and every reconnect reloads
// once before waiting so a notification cannot be lost in the gap.
func (s *Store) WatchConfig(ctx context.Context, interval time.Duration, reload func(context.Context) error, hooks ...WatchHooks) {
	s.watch(ctx, interval, "llmgw_config_changed", reload, firstWatchHooks(hooks))
}

func (s *Store) WatchCircuit(ctx context.Context, interval time.Duration, refresh func(context.Context) error, hooks ...WatchHooks) {
	s.watch(ctx, interval, "llmgw_circuit_changed", refresh, firstWatchHooks(hooks))
}

func firstWatchHooks(hooks []WatchHooks) WatchHooks {
	if len(hooks) == 0 {
		return WatchHooks{}
	}
	return hooks[0]
}

func (s *Store) watch(ctx context.Context, interval time.Duration, channel string, reload func(context.Context) error, hooks WatchHooks) {
	s.watchWithConnector(ctx, interval, channel, reload, func(connectCtx context.Context, url string) (notificationConn, error) {
		return pgx.Connect(connectCtx, url)
	}, hooks)
}

func (s *Store) watchWithConnector(ctx context.Context, interval time.Duration, channel string, reload func(context.Context) error, connect notificationConnector, hooks WatchHooks) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	reloadObserved := func() {
		err := reload(ctx)
		if err != nil {
			if hooks.ReloadFailure != nil {
				hooks.ReloadFailure(err)
			}
			return
		}
		if hooks.ReloadSuccess != nil {
			hooks.ReloadSuccess(time.Now().UTC())
		}
	}
	connectedOnce := false
	for ctx.Err() == nil {
		reloadObserved()
		if ctx.Err() != nil {
			return
		}
		// Connection establishment and LISTEN share the next polling deadline.
		// A slow direct endpoint must not add another full interval to polling
		// through the independently available runtime pool.
		jitter := time.Duration(rand.Int64N(int64(interval/5 + 1)))
		pollAt := time.Now().Add(interval + jitter)
		setupCtx, cancelSetup := context.WithDeadline(ctx, pollAt)
		conn, err := connect(setupCtx, s.migrationURL)
		if err != nil {
			cancelSetup()
			s.pollWait(ctx, pollAt)
			continue
		}
		listen := "LISTEN llmgw_config_changed"
		if channel == "llmgw_circuit_changed" {
			listen = "LISTEN llmgw_circuit_changed"
		}
		if _, err = conn.Exec(setupCtx, listen); err != nil {
			closeNotificationConnection(ctx, conn, pollAt)
			cancelSetup()
			s.pollWait(ctx, pollAt)
			continue
		}
		cancelSetup()
		if connectedOnce && hooks.NotificationReconnect != nil {
			hooks.NotificationReconnect()
		}
		connectedOnce = true
		reloadObserved()
		for ctx.Err() == nil {
			jitter := time.Duration(rand.Int64N(int64(interval/5 + 1)))
			waitCtx, cancel := context.WithTimeout(ctx, interval+jitter)
			_, waitErr := conn.WaitForNotification(waitCtx)
			cancel()
			if ctx.Err() != nil {
				break
			}
			reloadObserved()
			if waitErr != nil && waitCtx.Err() != context.DeadlineExceeded {
				break
			}
		}
		closeNotificationConnection(ctx, conn, time.Now().Add(interval))
	}
}

func closeNotificationConnection(ctx context.Context, conn notificationConn, deadline time.Time) {
	// pgx always closes the underlying socket, even when the graceful-close
	// deadline has already elapsed. Cleanup must not delay the next poll.
	if latest := time.Now().Add(time.Second); deadline.After(latest) {
		deadline = latest
	}
	closeCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	_ = conn.Close(closeCtx)
}

func (s *Store) pollWait(ctx context.Context, deadline time.Time) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
