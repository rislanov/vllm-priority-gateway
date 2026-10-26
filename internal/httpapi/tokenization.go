package httpapi

import (
	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
	"io"
	"net/http"
	"time"
)

func (h *PublicHandler) tokenize(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	id, ok := h.begin(w)
	event := gateway.TokenizationEvent{RequestID: id}
	early := func(e *gateway.APIError) {
		writeGatewayError(w, e)
		event.Status = e.HTTPStatus
		event.Reason = e.Code
		event.Duration = time.Since(started)
		h.service.ObserveTokenization(event)
	}
	if !ok {
		event.Status = 500
		event.Reason = "internal_error"
		event.Duration = time.Since(started)
		h.service.ObserveTokenization(event)
		return
	}
	raw, err := bearerToken(r.Header.Get("Authorization"))
	if err != nil {
		early(&gateway.APIError{HTTPStatus: 401, Message: "Invalid API key", Type: "authentication_error", Code: "invalid_api_key"})
		return
	}
	client, e := h.service.ValidateAPIKey(raw)
	if e != nil {
		early(e)
		return
	}
	event.Client = client.Name
	r.Body = http.MaxBytesReader(w, r.Body, h.bodyLimit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		early(invalidRequest("Request body is invalid or exceeds the configured limit"))
		return
	}
	result, e := h.service.Tokenize(r.Context(), w, gateway.ForwardRequest{Method: r.Method, Path: r.URL.Path, Headers: r.Header.Clone(), Body: body, APIKey: raw, RequestID: id})
	if e != nil {
		writeGatewayError(w, e)
	}
	if result.Err != nil && result.ResponseStarted {
		panic(http.ErrAbortHandler)
	}
}
