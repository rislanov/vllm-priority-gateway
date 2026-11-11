package admission

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

const maxRequestFanout = 32

const (
	RequestSizeInvalid        = "invalid_request_error"
	RequestSizeUnsupported    = "token_estimation_unsupported"
	RequestSizeInputExceeded  = "input_token_limit_exceeded"
	RequestSizeOutputExceeded = "output_token_limit_exceeded"
)

type RequestSizeError struct {
	Code    string
	Message string
}

func (e *RequestSizeError) Error() string { return e.Message }

type PreparedRequestSize struct {
	Payload              []byte
	Inputs               []json.RawMessage
	PromptCount, Samples int64
	OutputAllowance      int64

	// TokenIDs holds counts for prompts already supplied as token IDs.
	// A zero entry requires an estimator call unless UseFallback is set.
	TokenIDs    []int64
	UseFallback bool
}

var requestSizeCommonFields = strings.Fields(`
	model stream stream_options min_tokens temperature top_p top_k min_p seed stop stop_token_ids
	frequency_penalty presence_penalty repetition_penalty ignore_eos logprobs top_logprobs logit_bias
	echo skip_special_tokens spaces_between_special_tokens include_stop_str_in_output return_token_ids
	return_tokens_as_token_ids user metadata store service_tier
`)

func PrepareRequestSize(path string, body []byte, policy domain.EffectiveRequestSizePolicy) (PreparedRequestSize, error) {
	if !policy.Enabled {
		return PreparedRequestSize{Payload: body}, nil
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		return PreparedRequestSize{}, invalidRequestSize("Request body must be a JSON object")
	}

	fields := []string{"max_tokens", "max_completion_tokens", "max_output_tokens"}
	switch path {
	case "/v1/chat/completions":
		fields = append(fields, "messages", "n", "add_generation_prompt", "continue_final_message", "add_special_tokens", "chat_template", "chat_template_kwargs", "tools", "tool_choice", "response_format", "function_call", "functions")
	case "/v1/completions":
		fields = append(fields, "prompt", "n", "add_special_tokens")
	case "/v1/responses":
		fields = append(fields, "input", "instructions", "tools", "tool_choice", "text", "reasoning", "chat_template_kwargs", "background", "truncation")
	default:
		return PreparedRequestSize{}, unsupportedRequestSize()
	}

	delete(payload, "priority")
	allowed := make(map[string]bool, len(fields)+len(requestSizeCommonFields))
	for _, group := range [][]string{fields, requestSizeCommonFields} {
		for _, field := range group {
			allowed[field] = true
		}
	}
	for field := range payload {
		if !allowed[field] {
			return PreparedRequestSize{}, unsupportedRequestSize()
		}
	}
	output, err := applyOutputAllowance(path, payload, policy.Limits)
	if err != nil {
		return PreparedRequestSize{}, err
	}
	samples, err := readSamples(path, payload)
	if err != nil {
		return PreparedRequestSize{}, err
	}

	var prepared PreparedRequestSize
	switch path {
	case "/v1/chat/completions":
		prepared, err = prepareChatInputs(payload)
	case "/v1/completions":
		prepared, err = prepareCompletionInputs(payload)
	case "/v1/responses":
		prepared, err = prepareResponsesInputs(payload)
	}
	if err != nil {
		return PreparedRequestSize{}, err
	}
	if prepared.UseFallback && policy.FallbackMode != domain.TokenFallbackUTF8Bytes {
		return PreparedRequestSize{}, unsupportedRequestSize()
	}
	prepared.PromptCount = int64(len(prepared.Inputs))
	prepared.Samples = samples
	prepared.OutputAllowance = output
	prepared.Payload, err = json.Marshal(payload)
	return prepared, err
}

func applyOutputAllowance(path string, payload map[string]json.RawMessage, limits domain.RequestSizeLimits) (int64, error) {
	var canonical string
	switch path {
	case "/v1/chat/completions":
		canonical = "max_completion_tokens"
	case "/v1/completions":
		canonical = "max_tokens"
	case "/v1/responses":
		canonical = "max_output_tokens"
	}
	var allowance int64
	for _, name := range []string{"max_tokens", "max_completion_tokens", "max_output_tokens"} {
		raw, present := payload[name]
		if !present {
			continue
		}
		if name != canonical && !(path == "/v1/chat/completions" && name == "max_tokens") {
			return 0, invalidRequestSize("Output limit is not supported on this endpoint")
		}
		if isNull(raw) {
			continue
		}
		value, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil || value <= 0 {
			return 0, invalidRequestSize("Output allowance must be a positive integer")
		}
		if allowance != 0 && allowance != value {
			return 0, invalidRequestSize("Conflicting output allowances")
		}
		allowance = value
	}
	if allowance == 0 {
		allowance = limits.DefaultOutputTokens
	}
	if allowance > limits.MaxOutputTokens {
		return 0, &RequestSizeError{RequestSizeOutputExceeded, "Output allowance exceeds the configured maximum"}
	}
	if raw := payload["min_tokens"]; len(raw) != 0 && !isNull(raw) {
		minimum, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil || minimum < 0 || minimum > allowance {
			return 0, invalidRequestSize("min_tokens must be an integer within the output allowance")
		}
	}
	if path == "/v1/chat/completions" {
		delete(payload, "max_tokens")
	}
	payload[canonical] = json.RawMessage(strconv.FormatInt(allowance, 10))
	return allowance, nil
}

func readSamples(path string, payload map[string]json.RawMessage) (int64, error) {
	raw := payload["n"]
	if len(raw) == 0 {
		return 1, nil
	}
	if isNull(raw) && path == "/v1/chat/completions" {
		return 1, nil
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n < 1 || n > maxRequestFanout {
		return 0, invalidRequestSize("n must be an integer from 1 to 32")
	}
	return n, nil
}

func ReservationTokens(inputEstimates []int64, outputAllowance, samples int64) (int64, error) {
	const limit = domain.MaxRequestSizeTokens
	if len(inputEstimates) < 1 || len(inputEstimates) > maxRequestFanout || samples < 1 || samples > maxRequestFanout ||
		outputAllowance < 1 || outputAllowance > limit {
		return 0, invalidRequestSize("Invalid token reservation dimensions")
	}
	total := int64(0)
	for _, estimate := range inputEstimates {
		if estimate < 0 || estimate > limit || limit-total < max(estimate, 1) {
			return 0, invalidRequestSize("Token reservation exceeds the supported range")
		}
		total += max(estimate, 1)
		if limit-total < outputAllowance {
			return 0, invalidRequestSize("Token reservation exceeds the supported range")
		}
		total += outputAllowance
	}
	if total > limit/samples {
		return 0, invalidRequestSize("Token reservation exceeds the supported range")
	}
	return total * samples, nil
}

func isNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}

func invalidRequestSize(message string) *RequestSizeError {
	return &RequestSizeError{RequestSizeInvalid, message}
}

func unsupportedRequestSize() *RequestSizeError {
	return &RequestSizeError{RequestSizeUnsupported, "This input is unsupported by the configured token estimator"}
}
