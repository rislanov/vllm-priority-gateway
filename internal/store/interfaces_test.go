package store_test

import "github.com/rislanov/vllm-priority-gateway/internal/store"

var (
	_ store.ConfigurationStore = (*store.SQLite)(nil)
	_ store.AnalyticsStore     = (*store.SQLite)(nil)
	_ store.LifecycleStore     = (*store.SQLite)(nil)
)
