package analytics

import (
	"context"
	"time"
)

type QueryStore interface {
	Analytics(context.Context, Filter) (Dataset, error)
	UsageRequests(context.Context, Filter, int, int) (RequestPage, error)
	StreamUsageRequests(context.Context, Filter, func(RequestRecord) error) error
}

type RequestRecord struct {
	ID              int64     `json:"id"`
	OccurredAt      time.Time `json:"occurredAt"`
	RequestID       string    `json:"requestId"`
	ParentRequestID string    `json:"parentRequestId"`
	ClientID        int64     `json:"clientId"`
	ClientName      string    `json:"clientName"`
	ModelPoolID     int64     `json:"modelPoolId"`
	ModelName       string    `json:"modelName"`
	BackendName     string    `json:"backendName"`
	HTTPStatus      int       `json:"httpStatus"`
	DurationMS      int64     `json:"durationMs"`
	TTFTMS          *int64    `json:"ttftMs"`
	RetryCount      int       `json:"retryCount"`
	Disconnected    bool      `json:"disconnected"`
	UsageAvailable  bool      `json:"usageAvailable"`
	InputTokens     *int64    `json:"inputTokens"`
	OutputTokens    *int64    `json:"outputTokens"`
	CacheReadTokens *int64    `json:"cacheReadTokens"`
}

type Filter struct {
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	ClientID       *int64    `json:"clientId"`
	ModelPoolID    *int64    `json:"modelPoolId"`
	UsageAvailable *bool     `json:"usageAvailable"`
}

type Summary struct {
	RequestCount        int64    `json:"requestCount"`
	MeteredRequestCount int64    `json:"meteredRequestCount"`
	UsageCoverage       float64  `json:"usageCoverage"`
	InputTokens         int64    `json:"inputTokens"`
	OutputTokens        int64    `json:"outputTokens"`
	CacheReadTokens     *int64   `json:"cacheReadTokens"`
	UncachedInputTokens *int64   `json:"uncachedInputTokens"`
	CacheHitRatio       *float64 `json:"cacheHitRatio"`
}

type SeriesPoint struct {
	BucketStart     time.Time `json:"bucketStart"`
	RequestCount    int64     `json:"requestCount"`
	InputTokens     int64     `json:"inputTokens"`
	OutputTokens    int64     `json:"outputTokens"`
	CacheReadTokens *int64    `json:"cacheReadTokens"`
	CacheHitRatio   *float64  `json:"cacheHitRatio"`
}

type BreakdownRow struct {
	ClientID            int64    `json:"clientId"`
	ClientName          string   `json:"clientName"`
	ModelPoolID         int64    `json:"modelPoolId"`
	ModelName           string   `json:"modelName"`
	RequestCount        int64    `json:"requestCount"`
	MeteredRequestCount int64    `json:"meteredRequestCount"`
	InputTokens         int64    `json:"inputTokens"`
	OutputTokens        int64    `json:"outputTokens"`
	CacheReadTokens     *int64   `json:"cacheReadTokens"`
	UncachedInputTokens *int64   `json:"uncachedInputTokens"`
	CacheHitRatio       *float64 `json:"cacheHitRatio"`
}

type RequestPage struct {
	Requests []RequestRecord `json:"requests"`
	Total    int64           `json:"total"`
	Limit    int             `json:"limit"`
	Offset   int             `json:"offset"`
}

type Dimension struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Dataset struct {
	Summary   Summary        `json:"summary"`
	Series    []SeriesPoint  `json:"series"`
	Breakdown []BreakdownRow `json:"breakdown"`
	Clients   []Dimension    `json:"clients"`
	Models    []Dimension    `json:"models"`
}
