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

	jsonInString       bool
	jsonEscaped        bool
	jsonStringMatches  bool
	jsonStringPosition int
	jsonUsageKey       bool
	jsonAwaitingValue  bool
	jsonStack          [maxJSONNestingDepth]byte
	jsonDepth          int
	jsonSawData        bool
	jsonCapturing      bool
	jsonCaptureBase    int
	jsonCapture        []byte

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
			if i.jsonSawData && (i.jsonInString || i.jsonEscaped || i.jsonDepth != 0 || i.jsonCapturing) {
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
		if !isJSONWhitespace(current) {
			i.jsonSawData = true
		}
		if i.jsonCapturing && !i.appendJSONCapture(current) {
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
				if !i.jsonCapturing {
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

		if !i.jsonCapturing && i.jsonUsageKey {
			if isJSONWhitespace(current) {
				continue
			}
			i.jsonUsageKey = false
			if current == ':' {
				i.jsonAwaitingValue = true
				continue
			}
		}
		if !i.jsonCapturing && i.jsonAwaitingValue {
			if isJSONWhitespace(current) {
				continue
			}
			i.jsonAwaitingValue = false
			if current == '{' {
				i.jsonCapturing = true
				i.jsonCaptureBase = i.jsonDepth
				i.jsonCapture = i.jsonCapture[:0]
				if !i.appendJSONCapture(current) {
					return
				}
			}
		}

		switch current {
		case '"':
			i.jsonInString = true
			i.jsonEscaped = false
			i.jsonStringMatches = !i.jsonCapturing
			i.jsonStringPosition = 0
		case '{', '[':
			if i.jsonDepth == len(i.jsonStack) {
				i.disableInspection()
				return
			}
			i.jsonStack[i.jsonDepth] = current
			i.jsonDepth++
		case '}', ']':
			if i.jsonDepth == 0 || !matchingJSONDelimiter(i.jsonStack[i.jsonDepth-1], current) {
				i.failed = true
				continue
			}
			i.jsonDepth--
			if i.jsonCapturing && i.jsonDepth == i.jsonCaptureBase {
				i.jsonCapturing = false
				i.consumeUsageObject(i.jsonCapture)
				i.jsonCapture = i.jsonCapture[:0]
			}
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
	candidate, found := findUsageCandidate(value)
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
	if valid {
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

func findUsageCandidate(root any) (any, bool) {
	queue := []any{root}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		switch value := current.(type) {
		case map[string]any:
			if usage, found := value["usage"]; found && usage != nil {
				return usage, true
			}
			for _, child := range value {
				queue = append(queue, child)
			}
		case []any:
			queue = append(queue, value...)
		}
	}
	return nil, false
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

func matchingJSONDelimiter(open, close byte) bool {
	return open == '{' && close == '}' || open == '[' && close == ']'
}

func isJSONWhitespace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}
