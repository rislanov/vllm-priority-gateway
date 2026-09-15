package gateway

import (
	"net/http"
	"strings"
)

const (
	piSessionAffinityFallbackHeader = "X-Client-Request-Id"
	piUserAgentPrefix               = "pi ("
)

// Ordered from an explicit gateway override to provider-specific aliases.
// Keep extraction and stripping in sync.
var sessionAffinityHeaders = [...]string{
	SessionAffinityHeader,
	"X-Opencode-Session",
	"X-Claude-Code-Session-Id",
	"Session-Id",
	"Session_id",
	"X-Session-Affinity",
	"X-Session-Id",
}

func sessionAffinityID(headers http.Header) (string, *APIError) {
	var selected string
	for _, name := range sessionAffinityHeaders {
		identifier, apiErr := validatedSessionAffinityHeader(headers, name)
		if apiErr != nil {
			return "", apiErr
		}
		if selected == "" {
			selected = identifier
		}
	}
	if isPiCodingAgent(headers) {
		identifier, apiErr := validatedSessionAffinityHeader(headers, piSessionAffinityFallbackHeader)
		if apiErr != nil {
			return "", apiErr
		}
		if selected == "" {
			selected = identifier
		}
	}
	return selected, nil
}

func validatedSessionAffinityHeader(headers http.Header, name string) (string, *APIError) {
	var identifier string
	for _, raw := range headers.Values(name) {
		value := strings.TrimSpace(raw)
		if len(value) > MaxSessionAffinityIDBytes {
			return "", invalidRequest(name + " must not exceed 256 bytes")
		}
		if value == "" {
			continue
		}
		if identifier != "" && identifier != value {
			return "", invalidRequest(name + " must not contain conflicting session identifiers")
		}
		identifier = value
	}
	return identifier, nil
}

func stripSessionAffinityHeaders(headers http.Header) {
	for _, name := range sessionAffinityHeaders {
		headers.Del(name)
	}
	if isPiCodingAgent(headers) {
		headers.Del(piSessionAffinityFallbackHeader)
	}
}

func isPiCodingAgent(headers http.Header) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(headers.Get("User-Agent"))), piUserAgentPrefix)
}
