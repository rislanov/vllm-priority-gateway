package web

import (
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
)

type Handler struct {
	service   *httpapi.AdminService
	templates map[string]*template.Template
	static    http.Handler
}

type pageData struct {
	Title       string
	Active      string
	CSRF        string
	View        httpapi.AdminView
	EditClient  *httpapi.AdminClient
	EditBackend *httpapi.AdminBackend
	Secret      string
	Error       string
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
	templates := make(map[string]*template.Template, 4)
	for _, page := range []string{"dashboard", "clients", "keys", "backends"} {
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
	return &Handler{service: service, templates: templates, static: http.FileServer(http.FS(staticFS))}, nil
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
	case "/admin/keys":
		h.keys(writer, request)
	case "/admin/backends":
		h.backends(writer, request)
	default:
		http.NotFound(writer, request)
	}
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
					data.Secret = created.Secret
				}
			}
		}
	} else if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	status := http.StatusOK
	if data.Error != "" {
		status = http.StatusBadRequest
	}
	h.render(writer, request, "keys", data, status)
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
