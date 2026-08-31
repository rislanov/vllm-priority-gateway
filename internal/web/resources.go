package web

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/httpapi"
)

func (h *Handler) clients(writer http.ResponseWriter, request *http.Request) {
	data := pageData{Title: "Clients", Active: "Clients"}
	if request.Method == http.MethodPost {
		if err := request.ParseForm(); err != nil {
			data.Error = "Invalid form submission"
		} else {
			data.Error = h.mutateClient(request)
		}
	} else if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet, http.MethodPost)
		return
	}
	if rawID := request.URL.Query().Get("edit"); rawID != "" {
		data.EditClient = editClient(h.service.View(), rawID)
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
			redirect, errorText := h.mutateKey(request)
			if redirect != "" {
				http.Redirect(writer, request, redirect, http.StatusSeeOther)
				return
			}
			data.Error = errorText
		}
	} else if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet, http.MethodPost)
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

func (h *Handler) backends(writer http.ResponseWriter, request *http.Request) {
	data := pageData{Title: "Backends", Active: "Backends"}
	if request.Method == http.MethodPost {
		if err := request.ParseForm(); err != nil {
			data.Error = "Invalid form submission"
		} else {
			data.Error = h.mutateBackend(request)
		}
	} else if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet, http.MethodPost)
		return
	}
	if editPoolID, editBackendID := request.URL.Query().Get("edit_pool"), request.URL.Query().Get("edit"); editPoolID != "" || editBackendID != "" {
		view := h.service.View()
		data.EditPool = editPool(view, editPoolID)
		data.EditBackend = editBackend(view, editBackendID)
	}
	status := http.StatusOK
	if data.Error != "" {
		status = http.StatusBadRequest
	}
	h.render(writer, request, "backends", data, status)
}

func (h *Handler) mutateClient(request *http.Request) string {
	input, err := clientInput(request)
	if err != nil {
		return err.Error()
	}
	if request.Form.Get("action") == "update" {
		id, err := positiveID(request.Form.Get("id"))
		if err != nil {
			return err.Error()
		}
		_, errorText := valueError(h.service.UpdateClient(request.Context(), id, input))
		return errorText
	}
	_, errorText := valueError(h.service.CreateClient(request.Context(), input))
	return errorText
}

func (h *Handler) mutateKey(request *http.Request) (redirect string, errorText string) {
	id, err := positiveID(request.Form.Get(map[bool]string{true: "key_id", false: "client_id"}[request.Form.Get("action") == "revoke"]))
	if err != nil {
		return "", err.Error()
	}
	if request.Form.Get("action") == "revoke" {
		if err := h.service.RevokeKey(request.Context(), id); err != nil {
			return "", err.Error()
		}
		return "", ""
	}
	input := httpapi.KeyInput{}
	if raw := request.Form.Get("expires_at"); raw != "" {
		expires, err := time.Parse("2006-01-02", raw)
		if err != nil {
			return "", "Expiry must be a valid date"
		}
		input.ExpiresAt = &expires
	}
	created, err := h.service.CreateKey(request.Context(), id, input)
	if err != nil {
		return "", err.Error()
	}
	nonce := h.putSecret(httpapi.AdminCSRFToken(request), created.ID, created.Secret)
	return "/admin/keys?flash=" + nonce, ""
}

func (h *Handler) mutateBackend(request *http.Request) string {
	switch request.Form.Get("action") {
	case "create_pool", "update_pool":
		input, err := poolInput(request)
		if err != nil {
			return err.Error()
		}
		if request.Form.Get("action") == "update_pool" {
			id, err := positiveID(request.Form.Get("id"))
			if err != nil {
				return err.Error()
			}
			_, errorText := valueError(h.service.UpdatePool(request.Context(), id, input))
			return errorText
		}
		_, errorText := valueError(h.service.CreatePool(request.Context(), input))
		return errorText
	case "drain", "resume":
		id, err := positiveID(request.Form.Get("id"))
		if err != nil {
			return err.Error()
		}
		_, errorText := valueError(h.service.SetBackendDraining(request.Context(), id, request.Form.Get("action") == "drain"))
		return errorText
	default:
		input, err := backendInput(request)
		if err != nil {
			return err.Error()
		}
		if request.Form.Get("action") == "update_backend" {
			id, err := positiveID(request.Form.Get("id"))
			if err != nil {
				return err.Error()
			}
			_, errorText := valueError(h.service.UpdateBackend(request.Context(), id, input))
			return errorText
		}
		_, errorText := valueError(h.service.CreateBackend(request.Context(), input))
		return errorText
	}
}

func editClient(view httpapi.AdminView, rawID string) *httpapi.AdminClient {
	id, err := positiveID(rawID)
	if err != nil {
		return nil
	}
	for _, client := range view.Clients {
		if client.ID == id {
			copy := client
			return &copy
		}
	}
	return nil
}

func editPool(view httpapi.AdminView, rawID string) *httpapi.AdminPool {
	id, err := positiveID(rawID)
	if err != nil {
		return nil
	}
	for _, pool := range view.Pools {
		if pool.ID == id {
			copy := pool
			return &copy
		}
	}
	return nil
}

func editBackend(view httpapi.AdminView, rawID string) *httpapi.AdminBackend {
	id, err := positiveID(rawID)
	if err != nil {
		return nil
	}
	for _, backend := range view.Backends {
		if backend.ID == id {
			copy := backend
			return &copy
		}
	}
	return nil
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
		id, err := positiveID(raw)
		if err != nil {
			return httpapi.ClientInput{}, err
		}
		input.ModelPoolIDs = append(input.ModelPoolIDs, id)
	}
	return input, nil
}

func poolInput(request *http.Request) (httpapi.PoolInput, error) {
	maxGatewayInflight, err := optionalFormInt(request.Form.Get("max_gateway_inflight"), "max gateway inflight")
	if err != nil {
		return httpapi.PoolInput{}, err
	}
	maxWaiting, err := optionalFormInt(request.Form.Get("max_waiting"), "max waiting")
	if err != nil {
		return httpapi.PoolInput{}, err
	}
	return httpapi.PoolInput{
		PublicModelName: request.Form.Get("public_model_name"), UpstreamModelName: request.Form.Get("upstream_model_name"),
		Enabled: request.Form.Get("enabled") == "on", MaxGatewayInflight: maxGatewayInflight, MaxWaiting: maxWaiting,
	}, nil
}

func optionalFormInt(raw, label string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", label)
	}
	return value, nil
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
