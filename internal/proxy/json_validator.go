package proxy

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
	if op, handled := v.stepString(current); handled {
		return op
	}
	if op, handled := v.stepNumber(current); handled {
		return op
	}
	if op, handled := v.stepLiteral(current); handled {
		return op
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
	default:
		return v.fail()
	}
}

func (v *incrementalJSONValidator) stepString(current byte) (jsonScanOp, bool) {
	switch v.state {
	case jsonStateInString:
		if current == '"' {
			v.state = jsonStateEndValue
			return jsonScanContinue, true
		}
		if current == '\\' {
			v.state = jsonStateInStringEscape
			return jsonScanContinue, true
		}
		if current < 0x20 {
			return v.fail(), true
		}
		return jsonScanContinue, true
	case jsonStateInStringEscape:
		switch current {
		case 'b', 'f', 'n', 'r', 't', '\\', '/', '"':
			v.state = jsonStateInString
		case 'u':
			v.state = jsonStateInStringUnicode1
		default:
			return v.fail(), true
		}
		return jsonScanContinue, true
	case jsonStateInStringUnicode1:
		return v.unicode(current, jsonStateInStringUnicode2), true
	case jsonStateInStringUnicode2:
		return v.unicode(current, jsonStateInStringUnicode3), true
	case jsonStateInStringUnicode3:
		return v.unicode(current, jsonStateInStringUnicode4), true
	case jsonStateInStringUnicode4:
		return v.unicode(current, jsonStateInString), true
	default:
		return jsonScanContinue, false
	}
}

func (v *incrementalJSONValidator) stepNumber(current byte) (jsonScanOp, bool) {
	switch v.state {
	case jsonStateNegative:
		if current == '0' {
			v.state = jsonStateZero
			return jsonScanContinue, true
		}
		if current >= '1' && current <= '9' {
			v.state = jsonStateInteger
			return jsonScanContinue, true
		}
		return v.fail(), true
	case jsonStateInteger:
		if current >= '0' && current <= '9' {
			return jsonScanContinue, true
		}
		return v.afterInteger(current), true
	case jsonStateZero:
		return v.afterInteger(current), true
	case jsonStateDecimalPoint:
		if current < '0' || current > '9' {
			return v.fail(), true
		}
		v.state = jsonStateDecimal
		return jsonScanContinue, true
	case jsonStateDecimal:
		if current >= '0' && current <= '9' {
			return jsonScanContinue, true
		}
		if current == 'e' || current == 'E' {
			v.state = jsonStateExponent
			return jsonScanContinue, true
		}
		return v.endValue(current), true
	case jsonStateExponent:
		if current == '+' || current == '-' {
			v.state = jsonStateExponentSign
			return jsonScanContinue, true
		}
		return v.exponentDigit(current), true
	case jsonStateExponentSign:
		return v.exponentDigit(current), true
	case jsonStateExponentDigits:
		if current >= '0' && current <= '9' {
			return jsonScanContinue, true
		}
		return v.endValue(current), true
	default:
		return jsonScanContinue, false
	}
}

func (v *incrementalJSONValidator) stepLiteral(current byte) (jsonScanOp, bool) {
	switch v.state {
	case jsonStateTrueR:
		return v.literal(current, 'r', jsonStateTrueU), true
	case jsonStateTrueU:
		return v.literal(current, 'u', jsonStateTrueE), true
	case jsonStateTrueE:
		return v.literal(current, 'e', jsonStateEndValue), true
	case jsonStateFalseA:
		return v.literal(current, 'a', jsonStateFalseL), true
	case jsonStateFalseL:
		return v.literal(current, 'l', jsonStateFalseS), true
	case jsonStateFalseS:
		return v.literal(current, 's', jsonStateFalseE), true
	case jsonStateFalseE:
		return v.literal(current, 'e', jsonStateEndValue), true
	case jsonStateNullU:
		return v.literal(current, 'u', jsonStateNullL1), true
	case jsonStateNullL1:
		return v.literal(current, 'l', jsonStateNullL2), true
	case jsonStateNullL2:
		return v.literal(current, 'l', jsonStateEndValue), true
	default:
		return jsonScanContinue, false
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
