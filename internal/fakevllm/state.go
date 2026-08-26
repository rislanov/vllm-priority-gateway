package fakevllm

import "time"

type ResetMode string

const (
	ResetNone          ResetMode = ""
	ResetBeforeHeaders ResetMode = "before_headers"
	ResetBeforeBody    ResetMode = "before_body"
	ResetAfterChunks   ResetMode = "after_chunks"
)

type State struct {
	Running          float64
	Waiting          float64
	KVCacheUsage     float64
	TTFT             time.Duration
	TokenDelay       time.Duration
	Tokens           []string
	HTTPStatus       int
	HTTPBody         string
	HealthFailures   int
	LegacyKVMetrics  bool
	ResetMode        ResetMode
	ResetAfterChunks int
	Models           []string
}

type RequestRecord struct {
	Path      string    `json:"path"`
	Model     string    `json:"model"`
	Stream    bool      `json:"stream"`
	Priority  string    `json:"priority"`
	RequestID string    `json:"requestId"`
	StartedAt time.Time `json:"startedAt"`
}

type Snapshot struct {
	State
	Requests          []RequestRecord
	ActiveRequests    int
	CancelledRequests int
}

type controlState struct {
	Running           float64   `json:"running"`
	Waiting           float64   `json:"waiting"`
	KVCacheUsage      float64   `json:"kvCacheUsage"`
	TTFTMilliseconds  int64     `json:"ttftMs"`
	TokenDelayMillis  int64     `json:"tokenDelayMs"`
	Tokens            []string  `json:"tokens"`
	HTTPStatus        int       `json:"httpStatus"`
	HTTPBody          string    `json:"httpBody"`
	HealthFailures    int       `json:"healthFailures"`
	LegacyKVMetrics   bool      `json:"legacyKvMetrics"`
	ResetMode         ResetMode `json:"resetMode"`
	ResetAfterChunks  int       `json:"resetAfterChunks"`
	Models            []string  `json:"models"`
	ActiveRequests    int       `json:"activeRequests,omitempty"`
	CancelledRequests int       `json:"cancelledRequests,omitempty"`
	RequestCount      int       `json:"requestCount,omitempty"`
}

func (c controlState) state() State {
	return State{
		Running: c.Running, Waiting: c.Waiting, KVCacheUsage: c.KVCacheUsage,
		TTFT:       time.Duration(c.TTFTMilliseconds) * time.Millisecond,
		TokenDelay: time.Duration(c.TokenDelayMillis) * time.Millisecond,
		Tokens:     append([]string(nil), c.Tokens...), HTTPStatus: c.HTTPStatus,
		HTTPBody: c.HTTPBody, HealthFailures: c.HealthFailures,
		LegacyKVMetrics: c.LegacyKVMetrics, ResetMode: c.ResetMode,
		ResetAfterChunks: c.ResetAfterChunks, Models: append([]string(nil), c.Models...),
	}
}

func stateControl(snapshot Snapshot) controlState {
	return controlState{
		Running: snapshot.Running, Waiting: snapshot.Waiting, KVCacheUsage: snapshot.KVCacheUsage,
		TTFTMilliseconds: snapshot.TTFT.Milliseconds(), TokenDelayMillis: snapshot.TokenDelay.Milliseconds(),
		Tokens: append([]string(nil), snapshot.Tokens...), HTTPStatus: snapshot.HTTPStatus,
		HTTPBody: snapshot.HTTPBody, HealthFailures: snapshot.HealthFailures,
		LegacyKVMetrics: snapshot.LegacyKVMetrics, ResetMode: snapshot.ResetMode,
		ResetAfterChunks: snapshot.ResetAfterChunks, Models: append([]string(nil), snapshot.Models...),
		ActiveRequests: snapshot.ActiveRequests, CancelledRequests: snapshot.CancelledRequests,
		RequestCount: len(snapshot.Requests),
	}
}
