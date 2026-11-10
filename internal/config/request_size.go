package config

import (
	"fmt"
	"time"
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
	c.AdmissionEstimateTimeout = timeout
	c.AdmissionEstimateMaxResponseBytes = int64(maxBytes)
	return nil
}
