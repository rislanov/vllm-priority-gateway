package config

import "fmt"

func (c *Config) loadTokenization(lookup LookupFunc) error {
	var err error
	if c.Tokenization.ClientMaxInflight, err = intValue(lookup, "LLMGW_TOKENIZE_CLIENT_MAX_INFLIGHT", c.Tokenization.ClientMaxInflight); err != nil {
		return err
	}
	if c.Tokenization.BackendMaxInflight, err = intValue(lookup, "LLMGW_TOKENIZE_BACKEND_MAX_INFLIGHT", c.Tokenization.BackendMaxInflight); err != nil {
		return err
	}
	rpm, e := intValue(lookup, "LLMGW_TOKENIZE_CLIENT_RPM", int(c.Tokenization.ClientRPM))
	if e != nil {
		return e
	}
	c.Tokenization.ClientRPM = int64(rpm)
	if c.Tokenization.Timeout, err = durationValue(lookup, "LLMGW_TOKENIZE_TIMEOUT", c.Tokenization.Timeout); err != nil {
		return err
	}
	if err = c.Tokenization.Validate(); err != nil {
		return fmt.Errorf("LLMGW_TOKENIZE settings: %w", err)
	}
	return nil
}
