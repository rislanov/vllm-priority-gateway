package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

type PublicHandler struct {
	service    *gateway.Service
	bodyLimit  int64
	generateID IDGenerator
	router     http.Handler
}

type InferenceReadinessProvider interface {
	InferenceReadiness() gateway.InferenceReadiness
}

func NewInferenceReadinessHandler(provider InferenceReadinessProvider) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		readiness := provider.InferenceReadiness()
		status := http.StatusOK
		if readiness.PoolAvailability == 0 && readiness.BackendAvailability == 0 {
			status = http.StatusServiceUnavailable
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(readiness)
	})
}

func NewPublicHandler(service *gateway.Service, bodyLimit int64, generateID IDGenerator) http.Handler {
	if generateID == nil {
		generateID = generateRequestID
	}
	handler := &PublicHandler{service: service, bodyLimit: bodyLimit, generateID: generateID}
	router := chi.NewRouter()
	router.Get("/v1/models", handler.models)
	router.Post("/v1/chat/completions", handler.forward)
	router.Post("/v1/completions", handler.forward)
	router.Post("/v1/responses", handler.forward)
	router.NotFound(handler.unsupported)
	router.MethodNotAllowed(handler.unsupported)
	handler.router = router
	return handler
}

func (h *PublicHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.router.ServeHTTP(writer, request)
}

func (h *PublicHandler) models(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	requestID, ok := h.begin(writer)
	if !ok {
		h.completePublic(started, gateway.RequestEvent{Status: http.StatusInternalServerError, Reason: "internal_error"})
		return
	}
	event := gateway.RequestEvent{RequestID: requestID, ParentRequestID: validParentRequestID(request.Header.Get("X-Request-Id"))}
	rawKey, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		apiError := &gateway.APIError{HTTPStatus: 401, Message: "Invalid API key", Type: "authentication_error", Code: "invalid_api_key"}
		writeGatewayError(writer, apiError)
		event.Status, event.Reason = apiError.HTTPStatus, apiError.Code
		h.completePublic(started, event)
		return
	}
	models, client, gatewayError := h.service.Models(request.Context(), rawKey)
	if gatewayError != nil {
		writeGatewayError(writer, gatewayError)
		event.Status, event.Reason = gatewayError.HTTPStatus, gatewayError.Code
		h.completePublic(started, event)
		return
	}
	event.Client = client.Name
	event.PriorityClass = client.PriorityClass
	event.VLLMPriority = client.VLLMPriority
	event.Status = http.StatusOK
	defer h.completePublic(started, event)
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	data := make([]model, 0, len(models))
	for _, item := range models {
		data = append(data, model{ID: item.PublicModelName, Object: "model", Created: time.Now().Unix(), OwnedBy: "llmgw"})
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("X-Request-Id", requestID)
	_ = json.NewEncoder(writer).Encode(map[string]any{"object": "list", "data": data})
}

func (h *PublicHandler) forward(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	requestID, ok := h.begin(writer)
	if !ok {
		h.completePublic(started, gateway.RequestEvent{Status: http.StatusInternalServerError, Reason: "internal_error"})
		return
	}
	var reservation gateway.ResponseCompleteReservation
	defer func() { h.service.ResponseComplete(reservation) }()
	var gatewayError *gateway.APIError
	event := gateway.RequestEvent{RequestID: requestID, ParentRequestID: validParentRequestID(request.Header.Get("X-Request-Id"))}
	rawKey, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		apiError := &gateway.APIError{HTTPStatus: 401, Message: "Invalid API key", Type: "authentication_error", Code: "invalid_api_key"}
		writeGatewayError(writer, apiError)
		event.Status, event.Reason = apiError.HTTPStatus, apiError.Code
		h.completePublic(started, event)
		return
	}
	client, authErr := h.service.ValidateAPIKey(rawKey)
	if authErr != nil {
		writeGatewayError(writer, authErr)
		event.Status, event.Reason = authErr.HTTPStatus, authErr.Code
		h.completePublic(started, event)
		return
	}
	event.Client = client.Name
	event.PriorityClass = client.PriorityClass
	event.VLLMPriority = client.VLLMPriority
	request.Body = http.MaxBytesReader(writer, request.Body, h.bodyLimit)
	body, err := io.ReadAll(request.Body)
	if err != nil {
		apiError := invalidRequest("Request body is invalid or exceeds the configured limit")
		writeGatewayError(writer, apiError)
		event.Status, event.Reason = apiError.HTTPStatus, apiError.Code
		h.completePublic(started, event)
		return
	}
	_, reservation, gatewayError = h.service.Forward(request.Context(), writer, gateway.ForwardRequest{
		Method: request.Method, Path: request.URL.Path, Headers: request.Header.Clone(), Body: body,
		APIKey: rawKey, RequestID: requestID, ParentRequestID: validParentRequestID(request.Header.Get("X-Request-Id")),
	})
	if gatewayError != nil {
		writeGatewayError(writer, gatewayError)
	}
}

func (h *PublicHandler) unsupported(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	requestID, ok := h.begin(writer)
	if !ok {
		h.completePublic(started, gateway.RequestEvent{Status: http.StatusInternalServerError, Reason: "internal_error"})
		return
	}
	apiError := unsupportedEndpoint()
	writeGatewayError(writer, apiError)
	h.completePublic(started, gateway.RequestEvent{
		RequestID: requestID, ParentRequestID: validParentRequestID(request.Header.Get("X-Request-Id")),
		Status: apiError.HTTPStatus, Reason: apiError.Code,
	})
}

func (h *PublicHandler) completePublic(started time.Time, event gateway.RequestEvent) {
	event.Duration = time.Since(started)
	h.service.CompletePublic(event)
}

func (h *PublicHandler) begin(writer http.ResponseWriter) (string, bool) {
	requestID, err := h.generateID()
	if err != nil {
		writeGatewayError(writer, &gateway.APIError{HTTPStatus: 500, Message: "Failed to generate request ID", Type: "server_error", Code: "internal_error"})
		return "", false
	}
	writer.Header().Set("X-Request-Id", requestID)
	return requestID, true
}
