package store

import (
	"context"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

type AdminStore interface {
	CreateClient(context.Context, CreateClientParams) (domain.Client, error)
	UpdateClient(context.Context, int64, UpdateClientParams) (domain.Client, error)
	DeleteClient(context.Context, int64) ([]int64, error)
	CreateAPIKey(context.Context, CreateAPIKeyParams) (domain.APIKey, error)
	RevokeAPIKey(context.Context, int64) error
	CreatePool(context.Context, CreatePoolParams) (domain.ModelPool, error)
	UpdatePool(context.Context, int64, UpdatePoolParams) (domain.ModelPool, error)
	DeletePool(context.Context, int64) error
	CreateBackend(context.Context, CreateBackendParams) (domain.Backend, error)
	UpdateBackend(context.Context, int64, UpdateBackendParams) (domain.Backend, error)
	DeleteBackend(context.Context, int64) error
	SetBackendDraining(context.Context, int64, bool) error
}

type KeyUsageStore interface {
	TouchKeyLastUsed(context.Context, int64, time.Time) error
}

type ConfigurationStore interface {
	registry.Loader
	AdminStore
	KeyUsageStore
}

type AnalyticsStore interface {
	analytics.RecordStore
	analytics.QueryStore
}

type LifecycleStore interface{ Close() error }
