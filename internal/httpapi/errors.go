package httpapi

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"

	"github.com/rislanov/vllm-priority-gateway/internal/gateway"
)

type ErrorEnvelope struct {
	Error APIError `json:"error"`
}

type APIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

func writeGatewayError(writer http.ResponseWriter, gatewayError *gateway.APIError) {
	writer.Header().Set("Content-Type", "application/json")
	if gatewayError.RetryAfter > 0 {
		seconds := int(math.Ceil(gatewayError.RetryAfter.Seconds()))
		writer.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
	writer.WriteHeader(gatewayError.HTTPStatus)
	_ = json.NewEncoder(writer).Encode(ErrorEnvelope{Error: APIError{
		Message: gatewayError.Message, Type: gatewayError.Type, Code: gatewayError.Code,
	}})
}

func unsupportedEndpoint() *gateway.APIError {
	return &gateway.APIError{
		HTTPStatus: http.StatusNotFound, Message: "This OpenAI-compatible endpoint is not supported by the gateway",
		Type: "invalid_request_error", Code: "unsupported_endpoint",
	}
}

func invalidRequest(message string) *gateway.APIError {
	return &gateway.APIError{
		HTTPStatus: http.StatusBadRequest, Message: message,
		Type: "invalid_request_error", Code: "invalid_request_error",
	}
}
