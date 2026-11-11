package config

import (
	"fmt"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/coordination"
)

func (c *Config) loadRequestSize(lookup LookupFunc) error {
	const timeoutKey = "LLMGW_ADMISSION_ESTIMATE_TIMEOUT"
	const responseKey = "LLMGW_ADMISSION_ESTIMATE_MAX_RESPONSE_BYTES"
	timeout, err := durationValue(lookup, timeoutKey, c.AdmissionEstimateTimeout)
	if err != nil {
		return err
	}
	if timeout <= 0 || timeout > 30*time.Second {
		return fmt.Errorf("%s must be greater than zero and at most 30s", timeoutKey)
	}
	maxBytes, err := intValue(lookup, responseKey, int(c.AdmissionEstimateMaxResponseBytes))
	if err != nil {
		return err
	}
	if maxBytes < 1<<10 || maxBytes > 64<<20 {
		return fmt.Errorf("%s must be between 1 KiB and 64 MiB", responseKey)
	}
	const emergencyPrefix = "LLMGW_ADMISSION_EMERGENCY_ESTIMATE_"
	clientCap, err := intValue(lookup, emergencyPrefix+"CLIENT_MAX_INFLIGHT", 1)
	if err != nil {
		return err
	}
	backendCap, err := intValue(lookup, emergencyPrefix+"BACKEND_MAX_INFLIGHT", 1)
	if err != nil {
		return err
	}
	rpm, err := intValue(lookup, emergencyPrefix+"CLIENT_RPM", 30)
	if err != nil {
		return err
	}
	emergency := coordination.TokenizationPolicy{
		ClientMaxInflight: clientCap, BackendMaxInflight: backendCap, ClientRPM: int64(rpm),
		Timeout: min(2*time.Second, timeout),
	}
	if err := emergency.Validate(); err != nil {
		return fmt.Errorf("LLMGW_ADMISSION_EMERGENCY_ESTIMATE settings: %w", err)
	}
	c.EmergencyTokenization = emergency
	c.AdmissionEstimateTimeout = timeout
	c.AdmissionEstimateMaxResponseBytes = int64(maxBytes)
	return nil
}
