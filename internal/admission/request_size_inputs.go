package admission

import (
	"bytes"
	"encoding/json"
	"strconv"
)

func prepareChatInputs(payload map[string]json.RawMessage) (PreparedRequestSize, error) {
	var messages []map[string]json.RawMessage
	if err := json.Unmarshal(payload["messages"], &messages); err != nil || len(messages) == 0 {
		return PreparedRequestSize{}, invalidRequestSize("messages must be a nonempty array of objects")
	}
	fallback := false
	for _, message := range messages {
		var role string
		if err := json.Unmarshal(message["role"], &role); err != nil {
			return PreparedRequestSize{}, invalidRequestSize("Each message requires a role")
		}
		switch role {
		case "system", "developer", "user", "assistant":
		case "tool", "function":
			fallback = true
		default:
			return PreparedRequestSize{}, unsupportedRequestSize()
		}
		for name := range message {
			switch name {
			case "role", "content", "name":
			case "tool_calls", "tool_call_id", "function_call", "reasoning", "reasoning_content":
				fallback = true
			default:
				return PreparedRequestSize{}, unsupportedRequestSize()
			}
		}
		content := message["content"]
		if !isNull(content) && !isJSONString(content) && !isTextParts(content, "text") {
			return PreparedRequestSize{}, unsupportedRequestSize()
		}
	}

	// Apply identical formatting defaults to the inference and tokenizer payloads.
	continueFinal, err := materializeBool(payload, "continue_final_message", false)
	if err != nil {
		return PreparedRequestSize{}, err
	}
	generationPrompt, err := materializeBool(payload, "add_generation_prompt", !continueFinal)
	if err != nil {
		return PreparedRequestSize{}, err
	}
	if continueFinal && generationPrompt {
		return PreparedRequestSize{}, invalidRequestSize("continue_final_message conflicts with add_generation_prompt")
	}
	if _, err := materializeBool(payload, "add_special_tokens", false); err != nil {
		return PreparedRequestSize{}, err
	}

	if !isNull(payload["tools"]) && !hasOnlyFunctionTools(payload["tools"]) {
		return PreparedRequestSize{}, unsupportedRequestSize()
	}

	fields := []string{"messages", "add_generation_prompt", "continue_final_message", "add_special_tokens", "chat_template", "chat_template_kwargs"}
	for _, field := range []string{"tools", "tool_choice", "response_format", "function_call", "functions"} {
		if !isNull(payload[field]) {
			fallback = true
			fields = append(fields, field)
		}
	}
	input, err := canonicalInput(payload, fields...)
	return PreparedRequestSize{Inputs: []json.RawMessage{input}, UseFallback: fallback}, err
}

func prepareCompletionInputs(payload map[string]json.RawMessage) (PreparedRequestSize, error) {
	raw := payload["prompt"]
	if isNull(raw) {
		return PreparedRequestSize{}, invalidRequestSize("prompt is required")
	}
	if _, err := materializeBool(payload, "add_special_tokens", true); err != nil {
		return PreparedRequestSize{}, err
	}
	prompts := []json.RawMessage{raw}
	if !isJSONString(raw) && !isTokenIDs(raw) {
		// A fresh slice prevents RawMessage.UnmarshalJSON from reusing the
		// backing array of the original prompt that still needs forwarding.
		prompts = nil
		if err := json.Unmarshal(raw, &prompts); err != nil || len(prompts) < 1 || len(prompts) > maxRequestFanout {
			return PreparedRequestSize{}, invalidRequestSize("prompt must contain at most 32 text or token-ID prompts")
		}
	}
	text := isJSONString(prompts[0])
	prepared := PreparedRequestSize{
		Inputs:   make([]json.RawMessage, 0, len(prompts)),
		TokenIDs: make([]int64, len(prompts)),
	}
	for i, prompt := range prompts {
		if text {
			if !isJSONString(prompt) {
				return PreparedRequestSize{}, invalidRequestSize("A prompt batch must use a single input type")
			}
		} else {
			count := countTokenIDs(prompt)
			if count == 0 {
				return PreparedRequestSize{}, invalidRequestSize("Token-ID prompts must contain nonnegative integers")
			}
			prepared.TokenIDs[i] = count
		}
		input, err := canonicalInput(map[string]json.RawMessage{
			"prompt": prompt, "add_special_tokens": payload["add_special_tokens"],
		}, "prompt", "add_special_tokens")
		if err != nil {
			return PreparedRequestSize{}, err
		}
		prepared.Inputs = append(prepared.Inputs, input)
	}
	return prepared, nil
}

func prepareResponsesInputs(payload map[string]json.RawMessage) (PreparedRequestSize, error) {
	if !isNull(payload["background"]) && !bytes.Equal(payload["background"], []byte("false")) {
		return PreparedRequestSize{}, unsupportedRequestSize()
	}
	if !isNull(payload["truncation"]) && !bytes.Equal(payload["truncation"], []byte(`"disabled"`)) {
		return PreparedRequestSize{}, unsupportedRequestSize()
	}
	if !isNull(payload["tools"]) && !hasOnlyFunctionTools(payload["tools"]) {
		return PreparedRequestSize{}, unsupportedRequestSize()
	}
	raw := payload["input"]
	if !isJSONString(raw) {
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil || len(items) == 0 {
			return PreparedRequestSize{}, invalidRequestSize("input must be text or a nonempty array of inline items")
		}
		for _, item := range items {
			var kind string
			if !isNull(item["type"]) && json.Unmarshal(item["type"], &kind) != nil {
				return PreparedRequestSize{}, unsupportedRequestSize()
			}
			switch kind {
			case "", "message":
				if !isJSONString(item["content"]) && !isTextParts(item["content"], "input_text", "output_text") {
					return PreparedRequestSize{}, unsupportedRequestSize()
				}
			case "function_call":
				if !isJSONString(item["arguments"]) {
					return PreparedRequestSize{}, unsupportedRequestSize()
				}
			case "function_call_output":
				if !isJSONString(item["output"]) {
					return PreparedRequestSize{}, unsupportedRequestSize()
				}
			default:
				return PreparedRequestSize{}, unsupportedRequestSize()
			}
		}
	}
	input, err := canonicalInput(payload, "input", "instructions", "tools", "tool_choice", "text", "reasoning", "chat_template_kwargs")
	return PreparedRequestSize{Inputs: []json.RawMessage{input}, UseFallback: true}, err
}

func hasOnlyFunctionTools(raw json.RawMessage) bool {
	var tools []struct{ Type string }
	if err := json.Unmarshal(raw, &tools); err != nil {
		return false
	}
	for _, tool := range tools {
		if tool.Type != "function" {
			return false
		}
	}
	return true
}

func materializeBool(payload map[string]json.RawMessage, name string, defaultValue bool) (bool, error) {
	raw := payload[name]
	if isNull(raw) {
		payload[name] = json.RawMessage(strconv.FormatBool(defaultValue))
		return defaultValue, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, invalidRequestSize(name + " must be a boolean")
	}
	return value, nil
}

func isJSONString(raw json.RawMessage) bool {
	return len(raw) > 0 && raw[0] == '"'
}

func isTextParts(raw json.RawMessage, kinds ...string) bool {
	var parts []struct {
		Type string
		Text *string
	}
	if err := json.Unmarshal(raw, &parts); err != nil || parts == nil {
		return false
	}
	for _, part := range parts {
		allowed := false
		for _, kind := range kinds {
			allowed = allowed || part.Type == kind
		}
		if !allowed || part.Text == nil {
			return false
		}
	}
	return true
}

func isTokenIDs(raw json.RawMessage) bool {
	return countTokenIDs(raw) > 0
}

func countTokenIDs(raw json.RawMessage) int64 {
	var ids []json.RawMessage
	if err := json.Unmarshal(raw, &ids); err != nil {
		return 0
	}
	for _, id := range ids {
		value, err := strconv.ParseInt(string(id), 10, 64)
		if err != nil || value < 0 {
			return 0
		}
	}
	return int64(len(ids))
}

func canonicalInput(payload map[string]json.RawMessage, fields ...string) (json.RawMessage, error) {
	input := make(map[string]any, len(fields))
	for _, field := range fields {
		if raw := payload[field]; !isNull(raw) {
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			var value any
			if err := decoder.Decode(&value); err != nil {
				return nil, invalidRequestSize("Invalid input field")
			}
			input[field] = value
		}
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(input); err != nil {
		return nil, invalidRequestSize("Invalid input structure")
	}
	return bytes.TrimSuffix(output.Bytes(), []byte("\n")), nil
}
