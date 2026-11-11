package store

import (
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type CreateClientParams struct {
	Name                                                                       string
	Enabled                                                                    bool
	PriorityClass                                                              domain.PriorityClass
	VLLMPriority                                                               int
	MaxConcurrency                                                             int
	RequestsPerMinute                                                          int64
	TokensPerMinute                                                            int64
	MaxInputTokens, MaxOutputTokens, DefaultOutputTokens, MaxOutstandingTokens *int64
	ModelPoolIDs                                                               []int64
}

type UpdateClientParams = CreateClientParams

// ApplyRequestSize preserves existing limits for fields omitted from an update.
func (p CreateClientParams) ApplyRequestSize(client *domain.Client) {
	if p.MaxInputTokens != nil {
		client.RequestSizeLimits.MaxInputTokens = *p.MaxInputTokens
	}
	if p.MaxOutputTokens != nil {
		client.RequestSizeLimits.MaxOutputTokens = *p.MaxOutputTokens
	}
	if p.DefaultOutputTokens != nil {
		client.RequestSizeLimits.DefaultOutputTokens = *p.DefaultOutputTokens
	}
	if p.MaxOutstandingTokens != nil {
		client.MaxOutstandingTokens = *p.MaxOutstandingTokens
	}
}

type CreateAPIKeyParams struct {
	ClientID   int64
	Prefix     string
	SecretHash [32]byte
	ExpiresAt  *time.Time
}

type CreatePoolParams struct {
	PublicModelName      string
	UpstreamModelName    string
	Enabled              bool
	MaxGatewayInflight   int
	HighPriorityReserve  int
	MaxWaiting           int
	RequestSizeAdmission *domain.RequestSizePolicy
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
