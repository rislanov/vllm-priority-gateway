package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"strconv"
	"strings"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

const (
	maxUsageCaptureSize = 64 << 10
	maxJSONNestingDepth = 256
)

type usageInspector struct {
	format    string
	disabled  bool
	failed    bool
	finalized bool
	usage     *domain.TokenUsage

	jsonValidator         incrementalJSONValidator
	jsonInString          bool
	jsonEscaped           bool
	jsonStringMatches     bool
	jsonStringPosition    int
	jsonStringIsTopKey    bool
	jsonUsageKey          bool
	jsonAwaitingValue     bool
	jsonCapturing         bool
	jsonCaptureBase       int
	jsonCapture           []byte
	invalidUsageCandidate bool

	sseEvent     []byte
	sseLineStart int
}

func newUsageInspector(contentType string) *usageInspector {
	inspector := &usageInspector{}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return inspector
	}
	mediaType = strings.ToLower(mediaType)
	switch {
	case mediaType == "text/event-stream":
		inspector.format = "sse"
		inspector.sseEvent = make([]byte, 0, 1024)
	case mediaType == "application/json" || strings.HasSuffix(mediaType, "+json"):
		inspector.format = "json"
		inspector.jsonCapture = make([]byte, 0, 256)
		inspector.jsonValidator.reset()
	}
	return inspector
}

func (i *usageInspector) Write(data []byte) {
	if i == nil || i.disabled || i.finalized || len(data) == 0 {
		return
	}
	switch i.format {
	case "json":
		i.writeJSON(data)
	case "sse":
		i.writeSSE(data)
	}
}

func (i *usageInspector) Result() (*domain.TokenUsage, string, bool) {
	if i == nil {
		return nil, "", false
	}
	if !i.finalized {
		i.finalized = true
		switch i.format {
		case "json":
			if !i.jsonValidator.eof() {
				i.failed = true
				i.usage = nil
			}
		case "sse":
			if !i.disabled && len(i.sseEvent) > 0 {
				i.consumeSSEEvent(i.sseEvent)
				i.sseEvent = nil
				i.sseLineStart = 0
			}
		}
	}
	if i.failed {
		return i.usage, i.format, true
	}
	return i.usage, "", false
}

func (i *usageInspector) writeJSON(data []byte) {
	const usageKey = "usage"
	for _, current := range data {
		if i.disabled {
			return
		}
		if i.jsonCapturing && !i.appendJSONCapture(current) {
			return
		}
		startsTopLevelKey := !i.jsonInString && current == '"' && i.jsonValidator.expectsTopLevelObjectKey()

		if !i.jsonInString && !i.jsonCapturing && i.jsonUsageKey && !isJSONWhitespace(current) {
			i.jsonUsageKey = false
			if current == ':' {
				i.jsonAwaitingValue = true
			}
		} else if !i.jsonInString && !i.jsonCapturing && i.jsonAwaitingValue && !isJSONWhitespace(current) {
			i.jsonAwaitingValue = false
			if current == '{' {
				i.jsonCapturing = true
				i.jsonCaptureBase = i.jsonValidator.depth
				i.jsonCapture = i.jsonCapture[:0]
				if !i.appendJSONCapture(current) {
					return
				}
			} else {
				i.failed = true
				i.invalidUsageCandidate = true
				i.usage = nil
			}
		}

		op := i.jsonValidator.stepByte(current)
		if op == jsonScanError {
			i.disableInspection()
			return
		}
		if i.jsonInString {
			if i.jsonEscaped {
				i.jsonEscaped = false
				i.jsonStringMatches = false
				continue
			}
			switch current {
			case '\\':
				i.jsonEscaped = true
				i.jsonStringMatches = false
			case '"':
				i.jsonInString = false
				if !i.jsonCapturing && i.jsonStringIsTopKey {
					i.jsonUsageKey = i.jsonStringMatches && i.jsonStringPosition == len(usageKey)
				}
			default:
				if i.jsonStringMatches {
					if i.jsonStringPosition >= len(usageKey) || current != usageKey[i.jsonStringPosition] {
						i.jsonStringMatches = false
					}
				}
				i.jsonStringPosition++
			}
			continue
		}
		if current == '"' {
			i.jsonInString = true
			i.jsonEscaped = false
			i.jsonStringMatches = startsTopLevelKey
			i.jsonStringPosition = 0
			i.jsonStringIsTopKey = startsTopLevelKey
		}
		if i.jsonCapturing && op == jsonScanEndObject && i.jsonValidator.depth == i.jsonCaptureBase {
			i.jsonCapturing = false
			i.consumeUsageObject(i.jsonCapture)
			i.jsonCapture = i.jsonCapture[:0]
		}
	}
}

func (i *usageInspector) appendJSONCapture(current byte) bool {
	if len(i.jsonCapture) == maxUsageCaptureSize {
		i.disableInspection()
		return false
	}
	i.jsonCapture = append(i.jsonCapture, current)
	return true
}

func (i *usageInspector) writeSSE(data []byte) {
	for _, current := range data {
		if len(i.sseEvent) == maxUsageCaptureSize {
			i.disableInspection()
			return
		}
		i.sseEvent = append(i.sseEvent, current)
		if current != '\n' {
			continue
		}
		line := i.sseEvent[i.sseLineStart : len(i.sseEvent)-1]
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) != 0 {
			i.sseLineStart = len(i.sseEvent)
			continue
		}
		i.consumeSSEEvent(i.sseEvent[:i.sseLineStart])
		i.sseEvent = i.sseEvent[:0]
		i.sseLineStart = 0
	}
}

func (i *usageInspector) consumeSSEEvent(event []byte) {
	var dataLines [][]byte
	for _, line := range bytes.Split(event, []byte{'\n'}) {
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 || line[0] == ':' {
			continue
		}
		field, value, found := bytes.Cut(line, []byte{':'})
		if !found {
			field = line
			value = nil
		}
		if !bytes.Equal(field, []byte("data")) {
			continue
		}
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		dataLines = append(dataLines, value)
	}
	if len(dataLines) == 0 {
		return
	}
	payload := bytes.Join(dataLines, []byte{'\n'})
	if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
		return
	}
	value, ok := decodeJSONValue(payload)
	if !ok {
		i.failed = true
		return
	}
	candidate, found := findAuthoritativeSSEUsage(value)
	if !found {
		return
	}
	i.consumeNormalizedUsage(candidate)
}

func (i *usageInspector) consumeUsageObject(object []byte) {
	value, ok := decodeJSONValue(object)
	if !ok {
		i.failed = true
		return
	}
	i.consumeNormalizedUsage(value)
}

func (i *usageInspector) consumeNormalizedUsage(value any) {
	usage, valid, validationFailed := normalizeTokenUsage(value)
	if validationFailed {
		i.failed = true
	}
	if !valid {
		i.invalidUsageCandidate = true
		i.usage = nil
	} else if !i.invalidUsageCandidate {
		i.usage = usage
	}
}

func (i *usageInspector) disableInspection() {
	i.disabled = true
	i.failed = true
	i.usage = nil
	i.jsonCapture = nil
	i.sseEvent = nil
}

func decodeJSONValue(data []byte) (any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, false
	}
	var extra any
	return value, decoder.Decode(&extra) == io.EOF
}

func findAuthoritativeSSEUsage(root any) (any, bool) {
	object, ok := root.(map[string]any)
	if !ok {
		return nil, false
	}
	if usage, found := object["usage"]; found && usage != nil {
		return usage, true
	}
	if eventType, _ := object["type"].(string); eventType != "response.completed" {
		return nil, false
	}
	response, ok := object["response"].(map[string]any)
	if !ok {
		return nil, true
	}
	if usage, found := response["usage"]; found {
		return usage, true
	}
	return nil, true
}

func normalizeTokenUsage(value any) (*domain.TokenUsage, bool, bool) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, false, true
	}
	inputKey, outputKey, detailsKey := "", "", ""
	if _, promptPresent := object["prompt_tokens"]; promptPresent {
		inputKey, outputKey, detailsKey = "prompt_tokens", "completion_tokens", "prompt_tokens_details"
	} else if _, completionPresent := object["completion_tokens"]; completionPresent {
		inputKey, outputKey, detailsKey = "prompt_tokens", "completion_tokens", "prompt_tokens_details"
	} else if _, inputPresent := object["input_tokens"]; inputPresent {
		inputKey, outputKey, detailsKey = "input_tokens", "output_tokens", "input_tokens_details"
	} else if _, outputPresent := object["output_tokens"]; outputPresent {
		inputKey, outputKey, detailsKey = "input_tokens", "output_tokens", "input_tokens_details"
	} else {
		return nil, false, true
	}

	input, inputValid := parseTokenCount(object[inputKey])
	output, outputValid := parseTokenCount(object[outputKey])
	if !inputValid || !outputValid {
		return nil, false, true
	}
	usage := &domain.TokenUsage{InputTokens: input, OutputTokens: output}
	detailsValue, detailsPresent := object[detailsKey]
	if !detailsPresent || detailsValue == nil {
		return usage, true, false
	}
	details, ok := detailsValue.(map[string]any)
	if !ok {
		return usage, true, true
	}
	cacheValue, cachePresent := details["cached_tokens"]
	if !cachePresent || cacheValue == nil {
		return usage, true, false
	}
	cache, cacheValid := parseTokenCount(cacheValue)
	if !cacheValid || cache > input {
		return usage, true, true
	}
	usage.CacheReadTokens = &cache
	return usage, true, false
}

func parseTokenCount(value any) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseInt(number.String(), 10, 64)
	return parsed, err == nil && parsed >= 0
}

func isJSONWhitespace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}
