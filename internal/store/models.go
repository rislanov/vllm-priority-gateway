package store

import (
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type CreateClientParams struct {
	Name           string
	Enabled        bool
	PriorityClass  domain.PriorityClass
	VLLMPriority   int
	MaxConcurrency int
	ModelPoolIDs   []int64
}

type UpdateClientParams = CreateClientParams

type CreateAPIKeyParams struct {
	ClientID   int64
	Prefix     string
	SecretHash [32]byte
	ExpiresAt  *time.Time
}

type CreatePoolParams struct {
	PublicModelName    string
	UpstreamModelName  string
	Enabled            bool
	MaxGatewayInflight int
	MaxWaiting         int
}

type UpdatePoolParams = CreatePoolParams

type CreateBackendParams struct {
	ModelPoolID       int64
	Name              string
	BaseURL           string
	Enabled           bool
	Draining          bool
	CapacityHint      float64
	RunningSoftLimit  float64
	UpstreamAPIKeyEnv string
}

type UpdateBackendParams = CreateBackendParams
