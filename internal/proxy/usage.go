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

type jsonScanOp uint8

const (
	jsonScanContinue jsonScanOp = iota
	jsonScanEndObject
	jsonScanError
)

type jsonScanState uint8

const (
	jsonStateBeginValue jsonScanState = iota
	jsonStateBeginValueOrEmpty
	jsonStateBeginStringOrEmpty
	jsonStateBeginString
	jsonStateEndValue
	jsonStateEndTop
	jsonStateInString
	jsonStateInStringEscape
	jsonStateInStringUnicode1
	jsonStateInStringUnicode2
	jsonStateInStringUnicode3
	jsonStateInStringUnicode4
	jsonStateNegative
	jsonStateZero
	jsonStateInteger
	jsonStateDecimalPoint
	jsonStateDecimal
	jsonStateExponent
	jsonStateExponentSign
	jsonStateExponentDigits
	jsonStateTrueR
	jsonStateTrueU
	jsonStateTrueE
	jsonStateFalseA
	jsonStateFalseL
	jsonStateFalseS
	jsonStateFalseE
	jsonStateNullU
	jsonStateNullL1
	jsonStateNullL2
	jsonStateError
)

type jsonParseState uint8

const (
	jsonParseObjectKey jsonParseState = iota
	jsonParseObjectValue
	jsonParseArrayValue
)

type incrementalJSONValidator struct {
	state  jsonScanState
	stack  [maxJSONNestingDepth]jsonParseState
	depth  int
	endTop bool
	failed bool
}

func (v *incrementalJSONValidator) reset() {
	v.state = jsonStateBeginValue
	v.depth = 0
	v.endTop = false
	v.failed = false
}

func (v *incrementalJSONValidator) expectsTopLevelObjectKey() bool {
	if v.depth != 1 || v.stack[0] != jsonParseObjectKey {
		return false
	}
	return v.state == jsonStateBeginStringOrEmpty || v.state == jsonStateBeginString
}

func (v *incrementalJSONValidator) eof() bool {
	if v.failed {
		return false
	}
	if v.endTop {
		return true
	}
	return v.stepByte(' ') != jsonScanError && v.endTop
}

func (v *incrementalJSONValidator) stepByte(current byte) jsonScanOp {
	if v.failed {
		return jsonScanError
	}
	switch v.state {
	case jsonStateBeginValue:
		return v.beginValue(current)
	case jsonStateBeginValueOrEmpty:
		if isJSONWhitespace(current) {
			return jsonScanContinue
		}
		if current == ']' {
			return v.endValue(current)
		}
		return v.beginValue(current)
	case jsonStateBeginStringOrEmpty:
		if isJSONWhitespace(current) {
			return jsonScanContinue
		}
		if current == '}' {
			v.stack[v.depth-1] = jsonParseObjectValue
			return v.endValue(current)
		}
		return v.beginString(current)
	case jsonStateBeginString:
		return v.beginString(current)
	case jsonStateEndValue:
		return v.endValue(current)
	case jsonStateEndTop:
		if !isJSONWhitespace(current) {
			return v.fail()
		}
		return jsonScanContinue
	case jsonStateInString:
		if current == '"' {
			v.state = jsonStateEndValue
			return jsonScanContinue
		}
		if current == '\\' {
			v.state = jsonStateInStringEscape
			return jsonScanContinue
		}
		if current < 0x20 {
			return v.fail()
		}
		return jsonScanContinue
	case jsonStateInStringEscape:
		switch current {
		case 'b', 'f', 'n', 'r', 't', '\\', '/', '"':
			v.state = jsonStateInString
		case 'u':
			v.state = jsonStateInStringUnicode1
		default:
			return v.fail()
		}
		return jsonScanContinue
	case jsonStateInStringUnicode1:
		return v.unicode(current, jsonStateInStringUnicode2)
	case jsonStateInStringUnicode2:
		return v.unicode(current, jsonStateInStringUnicode3)
	case jsonStateInStringUnicode3:
		return v.unicode(current, jsonStateInStringUnicode4)
	case jsonStateInStringUnicode4:
		return v.unicode(current, jsonStateInString)
	case jsonStateNegative:
		if current == '0' {
			v.state = jsonStateZero
			return jsonScanContinue
		}
		if current >= '1' && current <= '9' {
			v.state = jsonStateInteger
			return jsonScanContinue
		}
		return v.fail()
	case jsonStateInteger:
		if current >= '0' && current <= '9' {
			return jsonScanContinue
		}
		return v.afterInteger(current)
	case jsonStateZero:
		return v.afterInteger(current)
	case jsonStateDecimalPoint:
		if current < '0' || current > '9' {
			return v.fail()
		}
		v.state = jsonStateDecimal
		return jsonScanContinue
	case jsonStateDecimal:
		if current >= '0' && current <= '9' {
			return jsonScanContinue
		}
		if current == 'e' || current == 'E' {
			v.state = jsonStateExponent
			return jsonScanContinue
		}
		return v.endValue(current)
	case jsonStateExponent:
		if current == '+' || current == '-' {
			v.state = jsonStateExponentSign
			return jsonScanContinue
		}
		return v.exponentDigit(current)
	case jsonStateExponentSign:
		return v.exponentDigit(current)
	case jsonStateExponentDigits:
		if current >= '0' && current <= '9' {
			return jsonScanContinue
		}
		return v.endValue(current)
	case jsonStateTrueR:
		return v.literal(current, 'r', jsonStateTrueU)
	case jsonStateTrueU:
		return v.literal(current, 'u', jsonStateTrueE)
	case jsonStateTrueE:
		return v.literal(current, 'e', jsonStateEndValue)
	case jsonStateFalseA:
		return v.literal(current, 'a', jsonStateFalseL)
	case jsonStateFalseL:
		return v.literal(current, 'l', jsonStateFalseS)
	case jsonStateFalseS:
		return v.literal(current, 's', jsonStateFalseE)
	case jsonStateFalseE:
		return v.literal(current, 'e', jsonStateEndValue)
	case jsonStateNullU:
		return v.literal(current, 'u', jsonStateNullL1)
	case jsonStateNullL1:
		return v.literal(current, 'l', jsonStateNullL2)
	case jsonStateNullL2:
		return v.literal(current, 'l', jsonStateEndValue)
	default:
		return v.fail()
	}
}

func (v *incrementalJSONValidator) beginValue(current byte) jsonScanOp {
	if isJSONWhitespace(current) {
		return jsonScanContinue
	}
	switch current {
	case '{':
		if !v.push(jsonParseObjectKey) {
			return jsonScanError
		}
		v.state = jsonStateBeginStringOrEmpty
	case '[':
		if !v.push(jsonParseArrayValue) {
			return jsonScanError
		}
		v.state = jsonStateBeginValueOrEmpty
	case '"':
		v.state = jsonStateInString
	case '-':
		v.state = jsonStateNegative
	case '0':
		v.state = jsonStateZero
	case 't':
		v.state = jsonStateTrueR
	case 'f':
		v.state = jsonStateFalseA
	case 'n':
		v.state = jsonStateNullU
	default:
		if current >= '1' && current <= '9' {
			v.state = jsonStateInteger
		} else {
			return v.fail()
		}
	}
	return jsonScanContinue
}

func (v *incrementalJSONValidator) beginString(current byte) jsonScanOp {
	if isJSONWhitespace(current) {
		return jsonScanContinue
	}
	if current != '"' {
		return v.fail()
	}
	v.state = jsonStateInString
	return jsonScanContinue
}

func (v *incrementalJSONValidator) endValue(current byte) jsonScanOp {
	if v.depth == 0 {
		v.state = jsonStateEndTop
		v.endTop = true
		if !isJSONWhitespace(current) {
			return v.fail()
		}
		return jsonScanContinue
	}
	if isJSONWhitespace(current) {
		v.state = jsonStateEndValue
		return jsonScanContinue
	}
	switch v.stack[v.depth-1] {
	case jsonParseObjectKey:
		if current != ':' {
			return v.fail()
		}
		v.stack[v.depth-1] = jsonParseObjectValue
		v.state = jsonStateBeginValue
	case jsonParseObjectValue:
		if current == ',' {
			v.stack[v.depth-1] = jsonParseObjectKey
			v.state = jsonStateBeginString
			return jsonScanContinue
		}
		if current != '}' {
			return v.fail()
		}
		v.pop()
		return jsonScanEndObject
	case jsonParseArrayValue:
		if current == ',' {
			v.state = jsonStateBeginValue
			return jsonScanContinue
		}
		if current != ']' {
			return v.fail()
		}
		v.pop()
	}
	return jsonScanContinue
}

func (v *incrementalJSONValidator) afterInteger(current byte) jsonScanOp {
	if current == '.' {
		v.state = jsonStateDecimalPoint
		return jsonScanContinue
	}
	if current == 'e' || current == 'E' {
		v.state = jsonStateExponent
		return jsonScanContinue
	}
	return v.endValue(current)
}

func (v *incrementalJSONValidator) exponentDigit(current byte) jsonScanOp {
	if current < '0' || current > '9' {
		return v.fail()
	}
	v.state = jsonStateExponentDigits
	return jsonScanContinue
}

func (v *incrementalJSONValidator) unicode(current byte, next jsonScanState) jsonScanOp {
	if !(current >= '0' && current <= '9' || current >= 'a' && current <= 'f' || current >= 'A' && current <= 'F') {
		return v.fail()
	}
	v.state = next
	return jsonScanContinue
}

func (v *incrementalJSONValidator) literal(current, expected byte, next jsonScanState) jsonScanOp {
	if current != expected {
		return v.fail()
	}
	v.state = next
	return jsonScanContinue
}

func (v *incrementalJSONValidator) push(state jsonParseState) bool {
	if v.depth == len(v.stack) {
		v.fail()
		return false
	}
	v.stack[v.depth] = state
	v.depth++
	return true
}

func (v *incrementalJSONValidator) pop() {
	v.depth--
	if v.depth == 0 {
		v.state = jsonStateEndTop
		v.endTop = true
	} else {
		v.state = jsonStateEndValue
	}
}

func (v *incrementalJSONValidator) fail() jsonScanOp {
	v.failed = true
	v.state = jsonStateError
	return jsonScanError
}
