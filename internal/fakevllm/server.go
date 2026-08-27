package fakevllm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	mu        sync.Mutex
	state     State
	requests  []RequestRecord
	active    int
	cancelled int
	handler   http.Handler
}

func New() *Server {
	server := &Server{state: State{
		Tokens: []string{"hello"}, HTTPStatus: http.StatusOK, Models: []string{"fake-model"},
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.health)
	mux.HandleFunc("GET /metrics", server.metrics)
	mux.HandleFunc("GET /v1/models", server.models)
	mux.HandleFunc("POST /v1/chat/completions", server.completion)
	mux.HandleFunc("POST /v1/completions", server.completion)
	mux.HandleFunc("POST /v1/responses", server.completion)
	mux.HandleFunc("GET /admin/state", server.getState)
	mux.HandleFunc("PUT /admin/state", server.putState)
	server.handler = mux
	return server
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) SetState(state State) {
	state = normalizeState(state)
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
}

func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := cloneState(s.state)
	return Snapshot{
		State: state, Requests: append([]RequestRecord(nil), s.requests...),
		ActiveRequests: s.active, CancelledRequests: s.cancelled,
	}
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	fail := s.state.HealthFailures > 0
	if fail {
		s.state.HealthFailures--
	}
	s.mu.Unlock()
	if fail {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	state := s.Snapshot().State
	model := state.Models[0]
	label := strconv.Quote(model)
	kvMetric := "vllm:kv_cache_usage_perc"
	if state.LegacyKVMetrics {
		kvMetric = "vllm:gpu_cache_usage_perc"
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w,
		"# TYPE vllm:num_requests_running gauge\n"+
			"vllm:num_requests_running{model_name=%s} %g\n"+
			"# TYPE vllm:num_requests_waiting gauge\n"+
			"vllm:num_requests_waiting{model_name=%s} %g\n"+
			"# TYPE %s gauge\n"+
			"%s{model_name=%s} %g\n",
		label, state.Running, label, state.Waiting, kvMetric, kvMetric, label, state.KVCacheUsage,
	)
}

func (s *Server) models(w http.ResponseWriter, _ *http.Request) {
	state := s.Snapshot().State
	data := make([]map[string]any, 0, len(state.Models))
	for _, model := range state.Models {
		data = append(data, map[string]any{"id": model, "object": "model", "owned_by": "fake-vllm"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *Server) completion(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Model         string `json:"model"`
		Stream        bool   `json:"stream"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20))
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	state := s.beginRequest(RequestRecord{
		Path: r.URL.Path, Model: request.Model, Stream: request.Stream,
		Priority: r.Header.Get("X-Vllm-Priority"), RequestID: r.Header.Get("X-Request-Id"),
		IncludeUsage: request.StreamOptions.IncludeUsage,
		StartedAt:    time.Now().UTC(),
	})
	defer s.endRequest()
	cancelled := false
	markCancelled := func() {
		if !cancelled {
			cancelled = true
			s.mu.Lock()
			s.cancelled++
			s.mu.Unlock()
		}
	}

	if state.ResetMode == ResetBeforeHeaders {
		if err := resetConnection(w, false); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if !waitContext(r, state.TTFT) {
		markCancelled()
		return
	}
	status := state.HTTPStatus
	if state.ResetMode == ResetBeforeBody {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		_ = resetConnection(w, true)
		return
	}
	if status != http.StatusOK {
		body := state.HTTPBody
		if body == "" {
			body = fmt.Sprintf(`{"error":{"message":"fake status %d"}}`, status)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
		return
	}
	if request.Stream {
		s.stream(w, r, state, request.StreamOptions.IncludeUsage, markCancelled)
		return
	}
	body := state.HTTPBody
	if body == "" {
		payload := map[string]any{"id": "fake-response", "object": "fake.completion", "model": request.Model}
		if state.Usage != nil {
			payload["object"] = "chat.completion"
			payload["choices"] = []any{map[string]any{
				"finish_reason": "stop", "index": 0,
				"message": map[string]string{"content": strings.Join(state.Tokens, ""), "role": "assistant"},
			}}
			payload["usage"] = qwenUsage(state.Usage)
		}
		encoded, _ := json.Marshal(payload)
		body = string(encoded)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, body)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, state State, includeUsage bool, markCancelled func()) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	for index, token := range state.Tokens {
		if index > 0 && !waitContext(r, state.TokenDelay) {
			markCancelled()
			return
		}
		frame, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]string{"content": token}}}})
		if _, err := fmt.Fprintf(w, "data: %s\n\n", frame); err != nil {
			markCancelled()
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		if state.ResetMode == ResetAfterChunks && index+1 >= state.ResetAfterChunks {
			_ = resetConnection(w, true)
			return
		}
	}
	if includeUsage && state.Usage != nil {
		frame, _ := json.Marshal(map[string]any{"choices": []any{}, "usage": qwenUsage(state.Usage)})
		if _, err := fmt.Fprintf(w, "data: %s\n\n", frame); err != nil {
			markCancelled()
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
		markCancelled()
		return
	}
	if flusher != nil {
		flusher.Flush()
	}
}

func (s *Server) getState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, stateControl(s.Snapshot()))
}

func (s *Server) putState(w http.ResponseWriter, r *http.Request) {
	var control controlState
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&control); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	state := normalizeState(control.state())
	if err := validateState(state); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.SetState(state)
	writeJSON(w, http.StatusOK, stateControl(s.Snapshot()))
}

func (s *Server) beginRequest(record RequestRecord) State {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active++
	s.requests = append(s.requests, record)
	return cloneState(s.state)
}

func (s *Server) endRequest() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

func normalizeState(state State) State {
	state = cloneState(state)
	if state.HTTPStatus == 0 {
		state.HTTPStatus = http.StatusOK
	}
	if len(state.Models) == 0 {
		state.Models = []string{"fake-model"}
	}
	if len(state.Tokens) == 0 {
		state.Tokens = []string{"hello"}
	}
	return state
}

func cloneState(state State) State {
	state.Tokens = append([]string(nil), state.Tokens...)
	state.Models = append([]string(nil), state.Models...)
	state.Usage = cloneUsage(state.Usage)
	return state
}

func validateState(state State) error {
	if state.Running < 0 || state.Waiting < 0 || state.KVCacheUsage < 0 || state.KVCacheUsage > 2 {
		return errors.New("running, waiting, and KV usage must be non-negative and KV usage must be at most 2")
	}
	if state.TTFT < 0 || state.TokenDelay < 0 || state.HealthFailures < 0 {
		return errors.New("delays and health failures cannot be negative")
	}
	if state.HTTPStatus < 100 || state.HTTPStatus > 599 {
		return errors.New("HTTP status must be between 100 and 599")
	}
	if state.Usage != nil && (state.Usage.InputTokens < 0 || state.Usage.OutputTokens < 0 ||
		(state.Usage.CacheReadTokens != nil && (*state.Usage.CacheReadTokens < 0 || *state.Usage.CacheReadTokens > state.Usage.InputTokens))) {
		return errors.New("usage tokens must be non-negative and cache-read tokens cannot exceed input tokens")
	}
	switch state.ResetMode {
	case ResetNone, ResetBeforeHeaders, ResetBeforeBody:
	case ResetAfterChunks:
		if state.ResetAfterChunks <= 0 {
			return errors.New("resetAfterChunks must be positive for after_chunks mode")
		}
	default:
		return fmt.Errorf("unsupported reset mode %q", state.ResetMode)
	}
	return nil
}

func qwenUsage(usage *Usage) map[string]any {
	value := map[string]any{
		"prompt_tokens": usage.InputTokens, "completion_tokens": usage.OutputTokens,
		"total_tokens": usage.InputTokens + usage.OutputTokens,
	}
	if usage.CacheReadTokens != nil {
		value["prompt_tokens_details"] = map[string]int64{"cached_tokens": *usage.CacheReadTokens}
	}
	return value
}

func waitContext(r *http.Request, delay time.Duration) bool {
	if delay <= 0 {
		return r.Context().Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-r.Context().Done():
		return false
	}
}

func resetConnection(w http.ResponseWriter, _ bool) error {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return errors.New("response writer does not support connection reset")
	}
	connection, _, err := hijacker.Hijack()
	if err != nil {
		return err
	}
	return connection.Close()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
