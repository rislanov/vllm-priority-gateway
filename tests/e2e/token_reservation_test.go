package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/admission"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
)

const (
	highTokenCap    = 8
	backgroundEvery = 10
)

type tokenProfile struct {
	GatewayCommit, VLLMBuild, MetalBuild, Hardware, Topology, Scheduling, CachePolicy string
	ModelRevision, TokenizerRevision, TemplateRevision, EstimationProfile             string
	Backends                                                                          []tokenProfileBackend
}

type tokenProfileBackend struct {
	ID     int64
	URL    string
	KeyEnv string
}

func readTokenProfile(path string) (tokenProfile, error) {
	var profile tokenProfile
	file, err := os.Open(path)
	if err != nil {
		return profile, fmt.Errorf("open qualification profile: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return profile, errors.New("invalid qualification profile JSON")
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return profile, errors.New("qualification profile must contain a single JSON object")
	}
	for _, value := range []string{profile.GatewayCommit, profile.VLLMBuild, profile.Hardware, profile.Topology,
		profile.Scheduling, profile.CachePolicy, profile.ModelRevision, profile.TokenizerRevision,
		profile.TemplateRevision, profile.EstimationProfile} {
		if strings.TrimSpace(value) == "" {
			return profile, errors.New("qualification profile requires all reproducibility fields")
		}
	}
	if len(profile.Backends) == 0 {
		return profile, errors.New("qualification profile must list the serving backends")
	}
	for _, backend := range profile.Backends {
		u, err := url.Parse(backend.URL)
		if backend.ID <= 0 || err != nil || (u.Scheme != "http" && u.Scheme != "https") ||
			u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return profile, errors.New("qualification backend requires its ID and an HTTP(S) URL without credentials, query, or fragment")
		}
		if backend.KeyEnv != "" && os.Getenv(backend.KeyEnv) == "" {
			return profile, fmt.Errorf("qualification backend %d requires its configured bearer credential", backend.ID)
		}
	}
	return profile, nil
}

type tokenProbe struct {
	key, prompt     string
	cap             int
	high, ignoreEOS bool
}

type tokenUsage struct {
	Input  *int64 `json:"prompt_tokens"`
	Output *int64 `json:"completion_tokens"`
}

func (u *tokenUsage) known() bool {
	return u != nil && u.Input != nil && u.Output != nil && *u.Input >= 0 && *u.Output >= 0
}

type tokenProbeResult struct {
	status     int
	reason     string
	retryAfter bool
	done       bool
	ttft       *time.Duration
	duration   time.Duration
	usage      *tokenUsage
}

func readTokenStream(body io.Reader, started time.Time, result *tokenProbeResult) {
	reader := bufio.NewScanner(io.LimitReader(body, 8<<20))
	reader.Buffer(make([]byte, 8192), 1<<20)
	for reader.Scan() {
		line := bytes.TrimSpace(reader.Bytes())
		data, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue
		}
		data = bytes.TrimSpace(data)
		if bytes.Equal(data, []byte("[DONE]")) {
			result.done = true
			break
		}
		var chunk struct {
			Choices []struct {
				Text string `json:"text"`
			} `json:"choices"`
			Usage *tokenUsage `json:"usage"`
		}
		if err := json.Unmarshal(data, &chunk); err != nil {
			result.reason = "invalid_stream"
			return
		}
		for _, choice := range chunk.Choices {
			if result.ttft == nil && choice.Text != "" {
				first := time.Since(started)
				result.ttft = &first
			}
		}
		if chunk.Usage.known() {
			result.usage = chunk.Usage
		}
	}
	if !result.done {
		result.reason = "incomplete_stream"
	}
}

func (h *remoteHarness) tokenProbe(parent context.Context, probe tokenProbe) (result tokenProbeResult) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.probeTimeout)
	defer cancel()
	payload := map[string]any{
		"model": h.cfg.model, "prompt": probe.prompt, "temperature": 0, "stream": true,
		"stream_options": map[string]bool{"include_usage": true}, "ignore_eos": probe.ignoreEOS,
	}
	if probe.cap > 0 {
		payload["max_tokens"] = probe.cap
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return tokenProbeResult{reason: "invalid_request"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint("/v1/completions"), bytes.NewReader(body))
	if err != nil {
		return tokenProbeResult{reason: "invalid_request"}
	}
	req.Header.Set("Authorization", "Bearer "+probe.key)
	req.Header.Set("Content-Type", "application/json")
	started := time.Now()
	defer func() { result.duration = time.Since(started) }()
	response, err := h.client.Do(req)
	if err != nil {
		return tokenProbeResult{reason: "transport_error"}
	}
	defer response.Body.Close()
	result.status = response.StatusCode
	result.retryAfter = response.Header.Get("Retry-After") != ""
	if result.status == http.StatusOK {
		readTokenStream(response.Body, started, &result)
		return result
	}
	var envelope struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&envelope)
	switch code := envelope.Error.Code; code {
	case "token_budget_exhausted", "tokenization_client_concurrency", "tokenization_backend_capacity", "tokenization_client_rate",
		"token_estimation_unavailable", "token_policy_transition", "input_token_limit_exceeded", "output_token_limit_exceeded",
		"gateway_overloaded", "rpm_exhausted", "tpm_exhausted", "gateway_unavailable":
		result.reason = code
	default:
		result.reason = "http_" + strconv.Itoa(result.status)
	}
	return result
}

type tokenClassReport struct {
	Attempted, Admitted, Completed, MissingTTFT, UnknownUsage, OutputCapViolations int
	OutputTokens                                                                   int64
	Rejections, Errors                                                             map[string]int
	TTFTMillis, EndToEndMillis                                                     [3]*float64
	ttft, endToEnd                                                                 []time.Duration
}

func (r *tokenClassReport) record(probe tokenProbe, result tokenProbeResult) {
	r.Attempted++
	r.endToEnd = append(r.endToEnd, result.duration)
	if result.status != http.StatusOK {
		if result.status == http.StatusTooManyRequests {
			if r.Rejections == nil {
				r.Rejections = make(map[string]int)
			}
			r.Rejections[result.reason]++
			switch result.reason {
			case "gateway_overloaded", "rpm_exhausted", "tpm_exhausted", "tokenization_client_concurrency",
				"tokenization_backend_capacity", "tokenization_client_rate":
				return
			case "token_budget_exhausted":
				if result.retryAfter {
					return
				}
				result.reason = "token_budget_missing_retry_after"
			}
		}
		r.recordError(result.reason)
		return
	}
	r.Admitted++
	if !result.done {
		r.recordError(result.reason)
		return
	}
	r.Completed++
	if result.ttft == nil {
		r.MissingTTFT++
	} else {
		r.ttft = append(r.ttft, *result.ttft)
	}
	if !result.usage.known() {
		r.UnknownUsage++
		return
	}
	r.OutputTokens += *result.usage.Output
	if probe.cap > 0 && *result.usage.Output > int64(probe.cap) {
		r.OutputCapViolations++
	}
}

func (r *tokenClassReport) recordError(reason string) {
	if r.Errors == nil {
		r.Errors = make(map[string]int)
	}
	r.Errors[reason]++
}

func durationPercentiles(samples []time.Duration) (result [3]*float64) {
	if len(samples) == 0 {
		return result
	}
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	for i, fraction := range []float64{.50, .95, .99} {
		value := float64(ordered[int(math.Ceil(float64(len(ordered))*fraction))-1]) / float64(time.Millisecond)
		result[i] = &value
	}
	return result
}

type tokenRunReport struct {
	High, Background                                       tokenClassReport
	PeakOutstanding                                        int64
	PreflightSamples                                       uint64
	PreflightMeanMillis                                    *float64
	Underestimates, PoolBudgetDenials, ClientBudgetDenials float64
	BackgroundTokensPerSecond                              float64
}

func (r tokenRunReport) validate(enabled bool, highProbes int) error {
	if r.High.Attempted < highProbes || r.High.Completed == 0 || r.High.Completed == r.High.MissingTTFT {
		return errors.New("insufficient High probes or no completed High TTFT samples")
	}
	if r.Background.Completed == 0 {
		return errors.New("no Background stream completed during the mixed workload")
	}
	for _, class := range []tokenClassReport{r.High, r.Background} {
		if len(class.Errors) > 0 || class.OutputCapViolations > 0 {
			return errors.New("mixed workload encountered an unexpected response, stream error, or output cap violation")
		}
		for _, code := range []string{"tokenization_client_concurrency", "tokenization_backend_capacity", "tokenization_client_rate"} {
			if class.Rejections[code] > 0 {
				return errors.New("utility capacity masked the token budget, calibrate utility limits or burst cadence")
			}
		}
	}
	tokenDenials := r.High.Rejections["token_budget_exhausted"] + r.Background.Rejections["token_budget_exhausted"]
	if enabled {
		if tokenDenials == 0 || r.PeakOutstanding == 0 || r.PreflightSamples == 0 || r.PoolBudgetDenials+r.ClientBudgetDenials != float64(tokenDenials) {
			return errors.New("enabled workload did not demonstrate outstanding token pressure and its scoped telemetry")
		}
		if r.Underestimates != 0 {
			return errors.New("actual tokens exceeded a reservation in the qualified workload")
		}
	} else if tokenDenials != 0 || r.PreflightSamples != 0 || r.PeakOutstanding != 0 {
		return errors.New("disabled workload used token admission")
	}
	return nil
}

func tokenWorkload(cfg e2eConfig, seed int64) (high, background []tokenProbe) {
	rng := rand.New(rand.NewSource(seed))
	const words = "market river stone bird rain tree moon road lamp sea "
	makePrompt := func(repeats int) string {
		return fmt.Sprintf("Sequence %d. Continue this list with more words. %s", rng.Int63(), strings.Repeat(words, repeats))
	}
	for i := 0; i < cfg.tokens.highProbes; i++ {
		high = append(high, tokenProbe{key: cfg.highKey, prompt: makePrompt(1), cap: highTokenCap, high: true, ignoreEOS: true})
		if i%backgroundEvery == 0 {
			for j := 0; j < cfg.tokens.backgroundBurst; j++ {
				key := cfg.lowKeys[len(background)%len(cfg.lowKeys)]
				background = append(background, tokenProbe{key: key, prompt: makePrompt(55), cap: cfg.tokens.backgroundOutput, ignoreEOS: true})
			}
		}
	}
	return high, background
}

func tokenOutstanding(metrics metricsResponse, poolID int64) (int64, bool) {
	value, found := metrics.value("llmgw_admission_outstanding_tokens", map[string]string{"pool_id": strconv.FormatInt(poolID, 10), "mode": "normal"})
	return int64(value), found
}

func tokenPreflight(metrics metricsResponse) (count uint64, sum float64) {
	families, _ := metrics.metricFamilies()
	if family := families["llmgw_admission_preflight_seconds"]; family != nil {
		for _, metric := range family.Metric {
			if metricMatches(metric, map[string]string{"mode": "normal"}) && metric.Histogram != nil {
				count += metric.Histogram.GetSampleCount()
				sum += metric.Histogram.GetSampleSum()
			}
		}
	}
	return count, sum
}

func (h *remoteHarness) runTokenWorkload(ctx context.Context, pool adminPool, high, background []tokenProbe) (tokenRunReport, error) {
	before, err := h.fetchMetrics(ctx)
	if err != nil {
		return tokenRunReport{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type sampleResult struct {
		peak int64
		err  error
	}
	samples := make(chan sampleResult, 1)
	go func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		var sample sampleResult
		for {
			select {
			case <-runCtx.Done():
				samples <- sample
				return
			case <-tick.C:
				requestCtx, stop := context.WithTimeout(runCtx, h.cfg.probeTimeout)
				metrics, err := h.fetchMetrics(requestCtx)
				stop()
				if err != nil && runCtx.Err() == nil {
					sample.err = err
				}
				active, _ := tokenOutstanding(metrics, pool.ID)
				sample.peak = max(sample.peak, active)
			}
		}
	}()

	type observation struct {
		probe  tokenProbe
		result tokenProbeResult
	}
	observed := make(chan observation, len(high)+len(background))
	var workers sync.WaitGroup
	var groupStarts int
	start := func(probe tokenProbe) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			observed <- observation{probe, h.tokenProbe(ctx, probe)}
		}()
	}
	waitUntil := func(at time.Time) bool {
		timer := time.NewTimer(time.Until(at))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	started := time.Now()
	for i, probe := range high {
		at := started.Add(time.Duration(i) * h.cfg.tokens.interval)
		if !waitUntil(at) {
			break
		}
		if i%backgroundEvery == 0 {
			end := groupStarts + h.cfg.tokens.backgroundBurst
			for _, job := range background[groupStarts:end] {
				start(job)
			}
			groupStarts = end
		}
		if !waitUntil(at.Add(h.cfg.tokens.interval / 2)) {
			break
		}
		start(probe)
	}
	workers.Wait()
	duration := time.Since(started)
	cancel()
	sample := <-samples
	if sample.err != nil {
		return tokenRunReport{}, sample.err
	}
	close(observed)
	var report tokenRunReport
	for observation := range observed {
		if observation.probe.high {
			report.High.record(observation.probe, observation.result)
		} else {
			report.Background.record(observation.probe, observation.result)
		}
	}
	report.PeakOutstanding = sample.peak
	for _, class := range []*tokenClassReport{&report.High, &report.Background} {
		class.TTFTMillis = durationPercentiles(class.ttft)
		class.EndToEndMillis = durationPercentiles(class.endToEnd)
	}
	report.BackgroundTokensPerSecond = float64(report.Background.OutputTokens) / duration.Seconds()
	after, err := h.fetchMetrics(ctx)
	if err != nil {
		return report, err
	}
	preflightStart, totalStart := tokenPreflight(before)
	preflightEnd, totalEnd := tokenPreflight(after)
	if preflightEnd < preflightStart || totalEnd < totalStart {
		return report, errors.New("preflight metrics reset during the experiment")
	}
	report.PreflightSamples = preflightEnd - preflightStart
	if report.PreflightSamples > 0 {
		mean := 1000 * (totalEnd - totalStart) / float64(report.PreflightSamples)
		report.PreflightMeanMillis = &mean
	}
	delta := func(name string, labels map[string]string) float64 {
		b, _ := before.value(name, labels)
		a, _ := after.value(name, labels)
		return a - b
	}
	report.Underestimates = delta("llmgw_admission_underestimate_total", map[string]string{"mode": "normal"})
	report.PoolBudgetDenials = delta("llmgw_admission_token_budget_rejections_total", map[string]string{"mode": "normal", "scope": "pool"})
	report.ClientBudgetDenials = delta("llmgw_admission_token_budget_rejections_total", map[string]string{"mode": "normal", "scope": "client"})
	return report, nil
}

func validateTokenTarget(status adminStatus, pool adminPool, cfg e2eConfig, profile tokenProfile) error {
	if !pool.Enabled || pool.RequestSizeAdmission == nil ||
		pool.RequestSizeAdmission.EstimationProfile != profile.EstimationProfile {
		return errors.New("dedicated pool must have the pinned request size policy")
	}
	enabledPolicy := pool.RequestSizeAdmission.Policy()
	enabledPolicy.Enabled = true
	if err := enabledPolicy.Validate(); err != nil {
		return fmt.Errorf("invalid experiment pool policy: %w", err)
	}
	backendURLs := make(map[int64]string)
	for _, backend := range status.Backends {
		if backend.ModelPoolID == pool.ID && backend.Enabled && !backend.Draining {
			backendURLs[backend.ID] = strings.TrimSuffix(backend.BaseURL, "/")
		}
	}
	if len(backendURLs) != cfg.expectedBackends || len(backendURLs) != len(profile.Backends) {
		return errors.New("qualification profile must cover every enabled serving backend")
	}
	for _, backend := range profile.Backends {
		if backendURLs[backend.ID] != strings.TrimSuffix(backend.URL, "/") {
			return fmt.Errorf("qualification backend %d does not match the serving pool", backend.ID)
		}
		delete(backendURLs, backend.ID)
	}

	clients := make(map[int64]httpapi.AdminClient)
	for _, client := range status.Clients {
		clients[client.ID] = client
	}
	keyOwners := make(map[string]int64)
	for _, key := range status.Keys {
		keyOwners[key.Prefix] = key.ClientID
	}
	owners := make(map[int64]bool)
	keys := append([]string{cfg.highKey}, cfg.lowKeys...)
	for i, key := range keys {
		if len(key) < 12 {
			return errors.New("experiment requires a gateway-issued key for each client")
		}
		client := clients[keyOwners[key[:12]]]
		class, cap := domain.PriorityBackground, cfg.tokens.backgroundOutput
		if i == 0 {
			class, cap = domain.PriorityHigh, highTokenCap
		}
		if client.ID == 0 || owners[client.ID] || !client.Enabled || client.PriorityClass != class ||
			!slices.Contains(client.ModelPoolIDs, pool.ID) {
			return fmt.Errorf("experiment key %d must belong to a distinct enabled %s client with pool access", i+1, class)
		}
		owners[client.ID] = true
		policy, err := domain.ResolveRequestSizePolicy(domain.Client{
			PriorityClass: class, RequestSizeLimits: client.RequestSizeLimits, MaxOutstandingTokens: client.MaxOutstandingTokens,
		}, domain.ModelPool{RequestSizeAdmission: enabledPolicy})
		if err != nil || policy.Limits.MaxOutputTokens < int64(cap) {
			return fmt.Errorf("experiment client %d cannot request the workload output allowance", i+1)
		}
	}
	return nil
}

// Snapshot only configuration. Runtime gauges and key last-use timestamps can
// change as a direct consequence of the workload.
func tokenTargetUnchanged(original, current adminStatus, poolID int64) bool {
	if !reflect.DeepEqual(original.Clients, current.Clients) ||
		len(original.Pools) != len(current.Pools) || len(original.Backends) != len(current.Backends) ||
		len(original.Keys) != len(current.Keys) {
		return false
	}
	for i, a := range original.Pools {
		b := current.Pools[i]
		if !samePoolConfiguration(a, b) || a.ID != poolID && !reflect.DeepEqual(a.RequestSizeAdmission, b.RequestSizeAdmission) {
			return false
		}
	}
	for i, a := range original.Backends {
		b := current.Backends[i]
		a.Runtime, b.Runtime = backendRuntime{}, backendRuntime{}
		if a != b {
			return false
		}
	}
	for i, a := range original.Keys {
		b := current.Keys[i]
		a.LastUsedAt, b.LastUsedAt = nil, nil
		if !reflect.DeepEqual(a, b) {
			return false
		}
	}
	return true
}

func (h *remoteHarness) waitTokenIdle(ctx context.Context, pool adminPool, enabled bool) error {
	if err := h.waitForIdleBackends(ctx, pool.ID, h.cfg.expectedBackends); err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		metrics, err := h.fetchMetrics(ctx)
		if err != nil {
			return err
		}
		outstanding, found := tokenOutstanding(metrics, pool.ID)
		if outstanding == 0 && (!enabled || found) {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return errors.New("pool did not return to zero outstanding tokens")
		}
	}
}

func (h *remoteHarness) checkTokenCaps(ctx context.Context, pool adminPool) error {
	prompt := "Continue: one, two, three, four, five, "
	oversized := h.tokenProbe(ctx, tokenProbe{key: h.cfg.highKey, prompt: prompt, cap: int(pool.RequestSizeAdmission.MaxOutputTokens + 1)})
	if oversized.status != http.StatusBadRequest || oversized.reason != "output_token_limit_exceeded" {
		return fmt.Errorf("oversize probe received %d/%s, want deterministic output limit rejection", oversized.status, oversized.reason)
	}
	valid := h.tokenProbe(ctx, tokenProbe{key: h.cfg.highKey, prompt: prompt, cap: highTokenCap, ignoreEOS: true})
	if valid.status != http.StatusOK || !valid.done || !valid.usage.known() || *valid.usage.Output != highTokenCap {
		return errors.New("isolated High probe did not complete with its enforced output allowance and known usage")
	}
	return nil
}

func ownsExperimentTokenPolicy(current *httpapi.RequestSizeAdmissionDTO, confirmed httpapi.RequestSizeAdmissionDTO, pending *httpapi.RequestSizeAdmissionDTO) bool {
	return current != nil && (*current == confirmed || pending != nil && *current == *pending)
}

func TestTokenReservationsWithRealVLLM(t *testing.T) {
	cfg := loadE2EConfig(t, modeTokens)
	h := newRemoteHarness(t, cfg)
	profile, err := readTokenProfile(cfg.tokens.profileFile)
	if err != nil {
		t.Fatal(err)
	}
	initial := h.adminStatus()
	original := initial.requirePool(t, cfg.model)
	if err := validateTokenTarget(initial, original, cfg, profile); err != nil {
		t.Fatal(err)
	}
	qualifyTokenProfile(t, profile, original.UpstreamModelName, cfg.probeTimeout)
	t.Logf("qualification model=%s model_revision=%s tokenizer_revision=%s template_revision=%s estimator_profile=%s gateway=%s vllm=%s metal=%s hardware=%s topology=%s scheduling=%s cache=%s",
		original.UpstreamModelName, profile.ModelRevision, profile.TokenizerRevision, profile.TemplateRevision, profile.EstimationProfile,
		profile.GatewayCommit, profile.VLLMBuild, profile.MetalBuild, profile.Hardware, profile.Topology, profile.Scheduling, profile.CachePolicy)
	policyJSON, _ := json.Marshal(original.RequestSizeAdmission)
	t.Logf("pool_limits=%s pool_inflight=%d slot_reserve=%d waiting=%d", policyJSON, original.MaxGatewayInflight, original.HighPriorityReserve, original.MaxWaiting)

	currentPolicy := *original.RequestSizeAdmission
	var pendingPolicy *httpapi.RequestSizeAdmissionDTO
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.recoveryTimeout)
		defer cancel()
		current, err := h.fetchAdminStatus(ctx)
		if err != nil {
			t.Errorf("read experiment pool before restoring policy: %v", err)
			return
		}
		pool := current.requirePool(t, cfg.model)
		if pool.ID != original.ID || !ownsExperimentTokenPolicy(pool.RequestSizeAdmission, currentPolicy, pendingPolicy) {
			t.Error("experiment pool policy changed externally before restoration")
			return
		}
		if err := h.waitTokenIdle(ctx, pool, pool.RequestSizeAdmission.Enabled); err != nil {
			t.Errorf("drain experiment before restoring its token policy: %v", err)
		}
		latest, err := h.fetchAdminStatus(ctx)
		if err != nil {
			t.Errorf("recheck the experiment pool before restoring its token policy: %v", err)
			return
		}
		pool = latest.requirePool(t, cfg.model)
		if pool.ID != original.ID || !ownsExperimentTokenPolicy(pool.RequestSizeAdmission, currentPolicy, pendingPolicy) {
			t.Error("experiment pool policy changed externally during cleanup")
			return
		}
		if *pool.RequestSizeAdmission != *original.RequestSizeAdmission {
			if _, err := h.updatePoolPolicy(ctx, pool, original.RequestSizeAdmission); err != nil {
				t.Errorf("restore request size policy: %v", err)
			}
		}
	})
	for i := 0; i < cfg.tokens.runs; i++ {
		seed := cfg.tokens.seed + int64(i)
		high, background := tokenWorkload(cfg, seed)
		order := []bool{false, true}
		if i%2 != 0 {
			slices.Reverse(order)
		}
		var reports [2]tokenRunReport
		for _, enabled := range order {
			idleCtx, cancel := context.WithTimeout(context.Background(), cfg.recoveryTimeout)
			err := h.waitTokenIdle(idleCtx, original, currentPolicy.Enabled)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			current := h.adminStatus()
			pool := current.requirePool(t, cfg.model)
			if !tokenTargetUnchanged(initial, current, original.ID) || !reflect.DeepEqual(pool.RequestSizeAdmission, &currentPolicy) {
				t.Fatal("configuration changed during the paired experiment")
			}
			target := currentPolicy
			target.Enabled = enabled
			if target != currentPolicy {
				// A lost update response can leave either of these policies
				// in effect. Cleanup owns both until the write is confirmed.
				pendingPolicy = &target
				updated, err := h.updatePoolPolicy(context.Background(), pool, &target)
				if err != nil {
					t.Fatal(err)
				}
				currentPolicy, pendingPolicy = target, nil
				if !samePoolConfiguration(original, updated) || !reflect.DeepEqual(updated.RequestSizeAdmission, &target) {
					t.Fatal("pool policy update changed settings outside the experiment")
				}
			}
			h.waitForPool(func(observed adminPool) bool {
				return samePoolConfiguration(original, observed) && reflect.DeepEqual(observed.RequestSizeAdmission, &target) && observed.Runtime.GatewayInflight == 0
			}, cfg.recoveryTimeout)
			if enabled {
				if err := h.checkTokenCaps(context.Background(), original); err != nil {
					t.Fatal(err)
				}
			}
			for _, warm := range []tokenProbe{high[0], background[0]} {
				if result := h.tokenProbe(context.Background(), warm); result.status != http.StatusOK || !result.done {
					t.Fatalf("warm-up did not finish: HTTP %d/%s", result.status, result.reason)
				}
			}
			idleCtx, cancel = context.WithTimeout(context.Background(), cfg.recoveryTimeout)
			err = h.waitTokenIdle(idleCtx, original, enabled)
			cancel()
			if err != nil {
				t.Fatal(err)
			}

			runCtx, stop := context.WithTimeout(context.Background(), time.Duration(len(high))*cfg.tokens.interval+cfg.probeTimeout+cfg.recoveryTimeout)
			report, err := h.runTokenWorkload(runCtx, original, high, background)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("repetition=%d seed=%d order=%v enabled=%t high_interval_ms=%d background_every=%d background_burst=%d background_output=%d report=%s",
				i+1, seed, order, enabled, cfg.tokens.interval.Milliseconds(), backgroundEvery, cfg.tokens.backgroundBurst, cfg.tokens.backgroundOutput, encoded)
			if err := report.validate(enabled, cfg.tokens.highProbes); err != nil {
				t.Fatal(err)
			}
			if after := h.adminStatus(); !tokenTargetUnchanged(initial, after, original.ID) ||
				!reflect.DeepEqual(after.requirePool(t, cfg.model).RequestSizeAdmission, &target) {
				t.Fatal("configuration changed during the measured workload")
			}
			if enabled {
				reports[1] = report
			} else {
				reports[0] = report
			}
		}
		a, b := reports[0].High, reports[1].High
		t.Logf("paired repetition=%d high_admission_fraction_disabled=%.4f enabled=%.4f ttft_p95_change_ms=%+.3f",
			i+1, float64(a.Admitted)/float64(a.Attempted), float64(b.Admitted)/float64(b.Attempted), *b.TTFTMillis[1]-*a.TTFTMillis[1])
	}
}

func qualifyTokenProfile(t *testing.T, profile tokenProfile, model string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	policy := domain.EffectiveRequestSizePolicy{Enabled: true,
		Limits: domain.RequestSizeLimits{MaxInputTokens: 2000, MaxOutputTokens: 8, DefaultOutputTokens: 1}}
	const completion = "/v1/completions"
	const chat = "/v1/chat/completions"
	const unicodeText = "Continue this list: árbol, éclair, 東京, café, "
	message := func(role, content string) map[string]string {
		return map[string]string{"role": role, "content": content}
	}
	type exactCase struct {
		name, path string
		fields     map[string]any
	}
	for _, backend := range profile.Backends {
		var tokenized struct {
			Count  *int64 `json:"count"`
			Tokens []int  `json:"tokens"`
		}
		if err := directTokenJSON(client, backend, "/tokenize", map[string]any{
			"model": model, "prompt": unicodeText, "add_special_tokens": false,
		}, &tokenized); err != nil || len(tokenized.Tokens) == 0 {
			t.Fatalf("backend %d token-ID fixture: %v", backend.ID, err)
		}
		cases := []exactCase{
			{"text_default", completion, map[string]any{"prompt": unicodeText}},
			{"text_without_special_tokens", completion, map[string]any{"prompt": unicodeText, "add_special_tokens": false}},
			{"text_batch", completion, map[string]any{"prompt": []string{unicodeText, "Write another word: market river "}}},
			{"token_ids", completion, map[string]any{"prompt": tokenized.Tokens}},
			{"token_id_batch", completion, map[string]any{"prompt": [][]int{tokenized.Tokens, tokenized.Tokens}}},
			{"chat_default", chat, map[string]any{"messages": []any{
				message("system", "Continue lists."), message("user", unicodeText),
				message("assistant", "One more."), message("user", "Continue with a city name."),
			}}},
			{"chat_text_parts", chat, map[string]any{"messages": []any{map[string]any{
				"role": "user", "content": []any{map[string]any{"type": "text", "text": unicodeText}},
			}}, "add_special_tokens": true}},
			{"chat_continuation", chat, map[string]any{"messages": []any{
				message("user", "Continue listing places."), message("assistant", "Some cities include "),
			}, "continue_final_message": true}},
			{"chat_template_options", chat, map[string]any{"messages": []any{message("user", unicodeText)},
				"chat_template_kwargs": map[string]any{"enable_thinking": false}, "add_generation_prompt": true}},
			{"chat_template_override", chat, map[string]any{
				"messages":             []any{message("user", unicodeText)},
				"chat_template":        "{% for message in messages %}{{ message.role + ': ' + message.content + '\\n' }}{% endfor %}{% if add_generation_prompt %}assistant: {% endif %}",
				"chat_template_kwargs": map[string]any{"enable_thinking": false}, "add_generation_prompt": true,
			}},
		}
		for _, fixture := range cases {
			fields := fixture.fields
			fields["model"], fields["temperature"] = model, 0
			payload, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := admission.PrepareRequestSize(fixture.path, payload, policy)
			if err != nil || prepared.UseFallback {
				t.Fatalf("qualification fixture %s is not an exact input: %v", fixture.name, err)
			}
			var estimate int64
			for i, input := range prepared.Inputs {
				if len(prepared.TokenIDs) > i && prepared.TokenIDs[i] > 0 {
					estimate += prepared.TokenIDs[i]
					continue
				}
				tokenPayload := make(map[string]any)
				if err := json.Unmarshal(input, &tokenPayload); err != nil {
					t.Fatal("cannot decode prepared qualification input")
				}
				tokenPayload["model"] = model
				var count struct {
					Count *int64 `json:"count"`
				}
				if err := directTokenJSON(client, backend, "/tokenize", tokenPayload, &count); err != nil || count.Count == nil || *count.Count < 0 {
					t.Fatalf("backend %d exact fixture %s tokenize: %v", backend.ID, fixture.name, err)
				}
				estimate += *count.Count
			}
			var result struct {
				Usage *tokenUsage `json:"usage"`
			}
			if err := directTokenJSON(client, backend, fixture.path, json.RawMessage(prepared.Payload), &result); err != nil || !result.Usage.known() {
				t.Fatalf("backend %d exact fixture %s generation: %v", backend.ID, fixture.name, err)
			}
			actual := *result.Usage.Input
			t.Logf("qualification backend_id=%d fixture=%s estimated_input=%d actual_input=%d error_tokens=%d",
				backend.ID, fixture.name, estimate, actual, estimate-actual)
			if estimate != actual {
				t.Fatalf("backend %d exact fixture %s differs by %d input tokens", backend.ID, fixture.name, estimate-actual)
			}
		}
	}
}

func directTokenJSON(client *http.Client, backend tokenProfileBackend, path string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return errors.New("encode direct qualification request")
	}
	request, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(backend.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return errors.New("build direct qualification request")
	}
	request.Header.Set("Content-Type", "application/json")
	if backend.KeyEnv != "" {
		request.Header.Set("Authorization", "Bearer "+os.Getenv(backend.KeyEnv))
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("send direct qualification request")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("direct HTTP %d", response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(output); err != nil {
		return errors.New("decode direct qualification response")
	}
	return nil
}
