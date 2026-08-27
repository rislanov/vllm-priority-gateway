package web

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
)

type Handler struct {
	service   *httpapi.AdminService
	templates map[string]*template.Template
	static    http.Handler
	secretMu  sync.Mutex
	secrets   map[string]secretFlash
}

type secretFlash struct {
	value     string
	expiresAt time.Time
	timer     *time.Timer
}

const (
	secretFlashTTL   = 5 * time.Minute
	maxSecretFlashes = 256
)

type pageData struct {
	Title       string
	Active      string
	CSRF        string
	View        httpapi.AdminView
	EditClient  *httpapi.AdminClient
	EditBackend *httpapi.AdminBackend
	Secret      string
	Error       string
	Analytics   *analyticsPage
}

type analyticsPage struct {
	Dataset        analytics.Dataset
	Requests       analytics.RequestPage
	FromValue      string
	ToValue        string
	FromCanonical  string
	ToCanonical    string
	ClientID       string
	ModelPoolID    string
	UsageAvailable string
	HasCacheSeries bool
	Limit          int
	CSVURL         string
	Presets        []analyticsPreset
	HasPrevious    bool
	PreviousURL    string
	HasNext        bool
	NextURL        string
	FirstRequest   int64
	LastRequest    int64
}

type analyticsPreset struct {
	Label  string
	URL    string
	Active bool
}

func New(service *httpapi.AdminService) (http.Handler, error) {
	if service == nil {
		return nil, fmt.Errorf("admin web service is required")
	}
	functions := template.FuncMap{
		"join":      strings.Join,
		"timeValue": timeValue,
		"percent":   func(value float64) string { return fmt.Sprintf("%.0f%%", value*100) },
		"decimal":   func(value float64) string { return fmt.Sprintf("%.2f", value) },
		"integer":   formatInteger,
		"ratio":     func(value float64) string { return fmt.Sprintf("%.0f%%", value*100) },
		"optionalInteger": func(value *int64) string {
			if value == nil {
				return "—"
			}
			return formatInteger(*value)
		},
		"optionalRatio": func(value *float64) string {
			if value == nil {
				return "—"
			}
			return fmt.Sprintf("%.0f%%", *value*100)
		},
		"utcTimestamp":  func(value time.Time) string { return value.UTC().Format("2006-01-02 15:04:05.000 UTC") },
		"canonicalTime": func(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) },
		"totalRunning": func(backends []httpapi.AdminBackend) string {
			var total float64
			for _, backend := range backends {
				total += backend.Runtime.Running
			}
			return fmt.Sprintf("%.0f", total)
		},
		"stateClass": func(value any) string { return "state-" + strings.ToLower(fmt.Sprint(value)) },
		"hasPool": func(ids []int64, id int64) bool {
			for _, candidate := range ids {
				if candidate == id {
					return true
				}
			}
			return false
		},
	}
	templates := make(map[string]*template.Template, 5)
	for _, page := range []string{"dashboard", "analytics", "clients", "keys", "backends"} {
		parsed, err := template.New("layout.html").Funcs(functions).ParseFS(assets, "templates/layout.html", "templates/"+page+".html")
		if err != nil {
			return nil, fmt.Errorf("parse %s template: %w", page, err)
		}
		templates[page] = parsed
	}
	staticFS, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	return &Handler{service: service, templates: templates, static: http.FileServer(http.FS(staticFS)), secrets: make(map[string]secretFlash)}, nil
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if strings.HasPrefix(request.URL.Path, "/admin/static/") {
		h.static.ServeHTTP(writer, withPath(request, strings.TrimPrefix(request.URL.Path, "/admin/static")))
		return
	}
	switch request.URL.Path {
	case "/admin", "/admin/":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		h.render(writer, request, "dashboard", pageData{Title: "Gateway overview", Active: "Dashboard"}, http.StatusOK)
	case "/admin/clients":
		h.clients(writer, request)
	case "/admin/analytics":
		h.analytics(writer, request)
	case "/admin/keys":
		h.keys(writer, request)
	case "/admin/backends":
		h.backends(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (h *Handler) analytics(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	values, normalized, err := normalizeAnalyticsWebValues(request.URL.Query())
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if normalized {
		http.Redirect(writer, request, analyticsURL("/admin/analytics", values), http.StatusSeeOther)
		return
	}
	query, err := h.service.ParseAnalyticsQuery(values)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	dataset, err := h.service.Analytics(request.Context(), query.Filter)
	if err != nil {
		http.Error(writer, "Unable to query analytics", http.StatusInternalServerError)
		return
	}
	requests, err := h.service.UsageRequests(request.Context(), query)
	if err != nil {
		http.Error(writer, "Unable to query analytics requests", http.StatusInternalServerError)
		return
	}
	data := pageData{Title: "Usage analytics", Active: "Analytics"}
	data.Analytics = buildAnalyticsPage(query, dataset, requests)
	h.render(writer, request, "analytics", data, http.StatusOK)
}

func normalizeAnalyticsWebValues(input url.Values) (url.Values, bool, error) {
	values := input.Clone()
	fromLocal, hasFrom := values["from_local"]
	toLocal, hasTo := values["to_local"]
	if !hasFrom && !hasTo {
		return values, false, nil
	}
	if !hasFrom || !hasTo || len(fromLocal) != 1 || len(toLocal) != 1 {
		return nil, false, fmt.Errorf("custom UTC from and to must be supplied together")
	}
	from, err := parseAnalyticsLocalTime(fromLocal[0])
	if err != nil {
		return nil, false, fmt.Errorf("from must be a valid UTC date and time")
	}
	to, err := parseAnalyticsLocalTime(toLocal[0])
	if err != nil {
		return nil, false, fmt.Errorf("to must be a valid UTC date and time")
	}
	values.Del("from_local")
	values.Del("to_local")
	for _, name := range []string{"client_id", "model_pool_id", "usage_available"} {
		if items := values[name]; len(items) == 1 && items[0] == "" {
			values.Del(name)
		}
	}
	values.Set("from", from.Format(time.RFC3339Nano))
	values.Set("to", to.Format(time.RFC3339Nano))
	return values, true, nil
}

func parseAnalyticsLocalTime(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04"} {
		parsed, err := time.ParseInLocation(layout, value, time.UTC)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid local time")
}

func buildAnalyticsPage(query httpapi.AnalyticsQuery, dataset analytics.Dataset, requests analytics.RequestPage) *analyticsPage {
	filterValues := analyticsFilterValues(query.Filter)
	pageValues := filterValues.Clone()
	pageValues.Set("limit", strconv.Itoa(query.Limit))
	if query.Offset > 0 {
		pageValues.Set("offset", strconv.Itoa(query.Offset))
	}
	page := &analyticsPage{
		Dataset: dataset, Requests: requests,
		FromValue:     query.Filter.From.UTC().Format("2006-01-02T15:04:05.000"),
		ToValue:       query.Filter.To.UTC().Format("2006-01-02T15:04:05.000"),
		FromCanonical: query.Filter.From.UTC().Format(time.RFC3339Nano),
		ToCanonical:   query.Filter.To.UTC().Format(time.RFC3339Nano),
		Limit:         query.Limit,
		CSVURL:        analyticsURL("/admin/api/analytics/export.csv", filterValues),
	}
	if query.Filter.ClientID != nil {
		page.ClientID = strconv.FormatInt(*query.Filter.ClientID, 10)
	}
	if query.Filter.ModelPoolID != nil {
		page.ModelPoolID = strconv.FormatInt(*query.Filter.ModelPoolID, 10)
	}
	if query.Filter.UsageAvailable != nil {
		page.UsageAvailable = strconv.FormatBool(*query.Filter.UsageAvailable)
	}
	for _, point := range dataset.Series {
		if point.CacheReadTokens != nil || point.CacheHitRatio != nil {
			page.HasCacheSeries = true
			break
		}
	}
	for _, preset := range []struct {
		label string
		width time.Duration
	}{{"1h", time.Hour}, {"24h", 24 * time.Hour}, {"7d", 7 * 24 * time.Hour}, {"30d", 30 * 24 * time.Hour}, {"90d", 90 * 24 * time.Hour}} {
		presetValues := filterValues.Clone()
		presetValues.Set("from", query.Filter.To.Add(-preset.width).Format(time.RFC3339Nano))
		page.Presets = append(page.Presets, analyticsPreset{
			Label: preset.label, URL: analyticsURL("/admin/analytics", presetValues),
			Active: query.Filter.To.Sub(query.Filter.From) == preset.width,
		})
	}
	page.HasPrevious = query.Offset > 0
	if page.HasPrevious {
		previous := query.Offset - query.Limit
		if previous < 0 {
			previous = 0
		}
		values := pageValues.Clone()
		if previous == 0 {
			values.Del("offset")
		} else {
			values.Set("offset", strconv.Itoa(previous))
		}
		page.PreviousURL = analyticsURL("/admin/analytics", values)
	}
	page.HasNext = int64(query.Offset+len(requests.Requests)) < requests.Total
	if page.HasNext {
		values := pageValues.Clone()
		values.Set("offset", strconv.Itoa(query.Offset+query.Limit))
		page.NextURL = analyticsURL("/admin/analytics", values)
	}
	if len(requests.Requests) > 0 {
		page.FirstRequest = int64(query.Offset + 1)
		page.LastRequest = int64(query.Offset + len(requests.Requests))
	}
	return page
}

func analyticsFilterValues(filter analytics.Filter) url.Values {
	values := url.Values{
		"from": {filter.From.UTC().Format(time.RFC3339Nano)},
		"to":   {filter.To.UTC().Format(time.RFC3339Nano)},
	}
	if filter.ClientID != nil {
		values.Set("client_id", strconv.FormatInt(*filter.ClientID, 10))
	}
	if filter.ModelPoolID != nil {
		values.Set("model_pool_id", strconv.FormatInt(*filter.ModelPoolID, 10))
	}
	if filter.UsageAvailable != nil {
		values.Set("usage_available", strconv.FormatBool(*filter.UsageAvailable))
	}
	return values
}

func analyticsURL(path string, values url.Values) string {
	if encoded := values.Encode(); encoded != "" {
		return path + "?" + encoded
	}
	return path
}

func formatInteger(value int64) string {
	raw := strconv.FormatInt(value, 10)
	start := 0
	if strings.HasPrefix(raw, "-") {
		start = 1
	}
	for position := len(raw) - 3; position > start; position -= 3 {
		raw = raw[:position] + "," + raw[position:]
	}
	return raw
}

func (h *Handler) clients(writer http.ResponseWriter, request *http.Request) {
	data := pageData{Title: "Clients", Active: "Clients"}
	if request.Method == http.MethodPost {
		if err := request.ParseForm(); err != nil {
			data.Error = "Invalid form submission"
		} else {
			input, err := clientInput(request)
			if err != nil {
				data.Error = err.Error()
			} else if request.Form.Get("action") == "update" {
				id, parseErr := positiveID(request.Form.Get("id"))
				if parseErr != nil {
					data.Error = parseErr.Error()
				} else {
					_, data.Error = valueError(h.service.UpdateClient(request.Context(), id, input))
				}
			} else {
				_, data.Error = valueError(h.service.CreateClient(request.Context(), input))
			}
		}
	} else if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	if rawID := request.URL.Query().Get("edit"); rawID != "" {
		if id, err := positiveID(rawID); err == nil {
			for _, client := range h.service.View().Clients {
				if client.ID == id {
					copy := client
					data.EditClient = &copy
					break
				}
			}
		}
	}
	status := http.StatusOK
	if data.Error != "" {
		status = http.StatusBadRequest
	}
	h.render(writer, request, "clients", data, status)
}

func (h *Handler) keys(writer http.ResponseWriter, request *http.Request) {
	data := pageData{Title: "API Keys", Active: "API Keys"}
	if request.Method == http.MethodPost {
		if err := request.ParseForm(); err != nil {
			data.Error = "Invalid form submission"
		} else {
			id, err := positiveID(request.Form.Get(map[bool]string{true: "key_id", false: "client_id"}[request.Form.Get("action") == "revoke"]))
			if err != nil {
				data.Error = err.Error()
			} else if request.Form.Get("action") == "revoke" {
				data.Error = errorText(h.service.RevokeKey(request.Context(), id))
			} else {
				input := httpapi.KeyInput{}
				if raw := request.Form.Get("expires_at"); raw != "" {
					expires, parseErr := time.Parse("2006-01-02", raw)
					if parseErr != nil {
						data.Error = "Expiry must be a valid date"
					} else {
						input.ExpiresAt = &expires
					}
				}
				if data.Error == "" {
					created, createErr := h.service.CreateKey(request.Context(), id, input)
					data.Error = errorText(createErr)
					if createErr == nil {
						nonce := h.putSecret(httpapi.AdminCSRFToken(request), created.ID, created.Secret)
						http.Redirect(writer, request, "/admin/keys?flash="+nonce, http.StatusSeeOther)
						return
					}
				}
			}
		}
	} else if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	} else {
		data.Secret = h.takeSecret(httpapi.AdminCSRFToken(request), request.URL.Query().Get("flash"))
	}
	status := http.StatusOK
	if data.Error != "" {
		status = http.StatusBadRequest
	}
	h.render(writer, request, "keys", data, status)
}

func (h *Handler) putSecret(session string, id int64, secret string) string {
	digest := sha256.Sum256([]byte(strconv.FormatInt(id, 10) + "\x00" + secret))
	nonce := base64.RawURLEncoding.EncodeToString(digest[:16])
	key := session + "\x00" + nonce
	now := time.Now()
	expiresAt := now.Add(secretFlashTTL)
	h.secretMu.Lock()
	for existingKey, flash := range h.secrets {
		if !flash.expiresAt.After(now) {
			flash.timer.Stop()
			delete(h.secrets, existingKey)
		}
	}
	if len(h.secrets) >= maxSecretFlashes {
		var oldestKey string
		var oldestAt time.Time
		for existingKey, flash := range h.secrets {
			if oldestKey == "" || flash.expiresAt.Before(oldestAt) {
				oldestKey, oldestAt = existingKey, flash.expiresAt
			}
		}
		if oldestKey != "" {
			h.secrets[oldestKey].timer.Stop()
			delete(h.secrets, oldestKey)
		}
	}
	flash := secretFlash{value: secret, expiresAt: expiresAt}
	flash.timer = time.AfterFunc(secretFlashTTL, func() { h.expireSecret(key, expiresAt) })
	h.secrets[key] = flash
	h.secretMu.Unlock()
	return nonce
}

func (h *Handler) takeSecret(session, nonce string) string {
	if nonce == "" {
		return ""
	}
	key := session + "\x00" + nonce
	h.secretMu.Lock()
	flash, exists := h.secrets[key]
	delete(h.secrets, key)
	if exists {
		flash.timer.Stop()
	}
	h.secretMu.Unlock()
	if !exists || !flash.expiresAt.After(time.Now()) {
		return ""
	}
	return flash.value
}

func (h *Handler) expireSecret(key string, expiresAt time.Time) {
	h.secretMu.Lock()
	if flash, exists := h.secrets[key]; exists && flash.expiresAt.Equal(expiresAt) {
		delete(h.secrets, key)
	}
	h.secretMu.Unlock()
}

func (h *Handler) backends(writer http.ResponseWriter, request *http.Request) {
	data := pageData{Title: "Backends", Active: "Backends"}
	if request.Method == http.MethodPost {
		if err := request.ParseForm(); err != nil {
			data.Error = "Invalid form submission"
		} else {
			switch request.Form.Get("action") {
			case "create_pool", "update_pool":
				input := httpapi.PoolInput{PublicModelName: request.Form.Get("public_model_name"), UpstreamModelName: request.Form.Get("upstream_model_name"), Enabled: request.Form.Get("enabled") == "on"}
				if request.Form.Get("action") == "update_pool" {
					id, err := positiveID(request.Form.Get("id"))
					if err != nil {
						data.Error = err.Error()
					} else {
						_, data.Error = valueError(h.service.UpdatePool(request.Context(), id, input))
					}
				} else {
					_, data.Error = valueError(h.service.CreatePool(request.Context(), input))
				}
			case "drain", "resume":
				id, err := positiveID(request.Form.Get("id"))
				if err != nil {
					data.Error = err.Error()
				} else {
					_, data.Error = valueError(h.service.SetBackendDraining(request.Context(), id, request.Form.Get("action") == "drain"))
				}
			default:
				input, err := backendInput(request)
				if err != nil {
					data.Error = err.Error()
				} else if request.Form.Get("action") == "update_backend" {
					id, parseErr := positiveID(request.Form.Get("id"))
					if parseErr != nil {
						data.Error = parseErr.Error()
					} else {
						_, data.Error = valueError(h.service.UpdateBackend(request.Context(), id, input))
					}
				} else {
					_, data.Error = valueError(h.service.CreateBackend(request.Context(), input))
				}
			}
		}
	} else if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	if rawID := request.URL.Query().Get("edit"); rawID != "" {
		if id, err := positiveID(rawID); err == nil {
			for _, backend := range h.service.View().Backends {
				if backend.ID == id {
					copy := backend
					data.EditBackend = &copy
					break
				}
			}
		}
	}
	status := http.StatusOK
	if data.Error != "" {
		status = http.StatusBadRequest
	}
	h.render(writer, request, "backends", data, status)
}

func (h *Handler) render(writer http.ResponseWriter, request *http.Request, name string, data pageData, status int) {
	data.CSRF = httpapi.AdminCSRFToken(request)
	data.View = h.service.View()
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.WriteHeader(status)
	if err := h.templates[name].ExecuteTemplate(writer, "layout", data); err != nil {
		return
	}
}

func clientInput(request *http.Request) (httpapi.ClientInput, error) {
	priority, err := strconv.Atoi(request.Form.Get("vllm_priority"))
	if err != nil {
		return httpapi.ClientInput{}, fmt.Errorf("vLLM priority must be an integer")
	}
	maxConcurrency, err := strconv.Atoi(request.Form.Get("max_concurrency"))
	if err != nil {
		return httpapi.ClientInput{}, fmt.Errorf("max concurrency must be an integer")
	}
	input := httpapi.ClientInput{Name: request.Form.Get("name"), Enabled: request.Form.Get("enabled") == "on", PriorityClass: domain.PriorityClass(request.Form.Get("priority_class")), VLLMPriority: priority, MaxConcurrency: maxConcurrency}
	for _, raw := range request.Form["model_pool_id"] {
		id, parseErr := positiveID(raw)
		if parseErr != nil {
			return httpapi.ClientInput{}, parseErr
		}
		input.ModelPoolIDs = append(input.ModelPoolIDs, id)
	}
	return input, nil
}

func backendInput(request *http.Request) (httpapi.BackendInput, error) {
	poolID, err := positiveID(request.Form.Get("model_pool_id"))
	if err != nil {
		return httpapi.BackendInput{}, err
	}
	capacity, err := strconv.ParseFloat(request.Form.Get("capacity_hint"), 64)
	if err != nil {
		return httpapi.BackendInput{}, fmt.Errorf("capacity hint must be a number")
	}
	running, err := strconv.ParseFloat(request.Form.Get("running_soft_limit"), 64)
	if err != nil {
		return httpapi.BackendInput{}, fmt.Errorf("running soft limit must be a number")
	}
	return httpapi.BackendInput{ModelPoolID: poolID, Name: request.Form.Get("name"), BaseURL: request.Form.Get("base_url"), Enabled: request.Form.Get("enabled") == "on", Draining: request.Form.Get("draining") == "on", CapacityHint: capacity, RunningSoftLimit: running, UpstreamAPIKeyEnv: request.Form.Get("upstream_api_key_env")}, nil
}

func positiveID(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("resource ID must be a positive integer")
	}
	return id, nil
}
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func valueError[T any](_ T, err error) (T, string) {
	var zero T
	if err != nil {
		return zero, err.Error()
	}
	return zero, ""
}
func timeValue(value *time.Time) string {
	if value == nil {
		return "Never"
	}
	return value.UTC().Format("2006-01-02 15:04 UTC")
}
func methodNotAllowed(writer http.ResponseWriter) {
	writer.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
	http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
}
func withPath(request *http.Request, path string) *http.Request {
	clone := request.Clone(request.Context())
	clone.URL.Path = path
	return clone
}
