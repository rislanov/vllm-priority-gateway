package proxy

import (
	"net/http"
	"strconv"
	"strings"
)

var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

func PrepareUpstreamHeaders(source http.Header, requestID string, priority int, upstreamAPIKey string) http.Header {
	headers := cloneEndToEndHeaders(source)
	headers.Del("Authorization")
	headers.Set("X-Request-Id", requestID)
	headers.Set("X-Vllm-Priority", strconv.Itoa(priority))
	if upstreamAPIKey != "" {
		headers.Set("Authorization", "Bearer "+upstreamAPIKey)
	}
	return headers
}

func CopyResponseHeaders(destination, source http.Header) {
	clean := cloneEndToEndHeaders(source)
	for key, values := range clean {
		destination.Del(key)
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func cloneEndToEndHeaders(source http.Header) http.Header {
	destination := make(http.Header, len(source))
	connectionTokens := make(map[string]struct{})
	for _, value := range source.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			token = http.CanonicalHeaderKey(strings.TrimSpace(token))
			if token != "" {
				connectionTokens[token] = struct{}{}
			}
		}
	}
	hop := make(map[string]struct{}, len(hopByHopHeaders)+len(connectionTokens))
	for _, key := range hopByHopHeaders {
		hop[http.CanonicalHeaderKey(key)] = struct{}{}
	}
	for key := range connectionTokens {
		hop[key] = struct{}{}
	}
	for key, values := range source {
		canonical := http.CanonicalHeaderKey(key)
		if _, blocked := hop[canonical]; blocked {
			continue
		}
		destination[canonical] = append([]string(nil), values...)
	}
	return destination
}
