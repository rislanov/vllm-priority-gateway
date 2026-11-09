package config_test

import (
	"strings"
	"testing"

	"github.com/rislanov/vllm-priority-gateway/internal/config"
)

func TestLoadCoordinationResetValidation(t *testing.T) {
	for _, tc := range []struct {
		name, driver, value string
		wantErr             bool
	}{
		{"postgres enabled", "postgres", "true", false},
		{"postgres disabled", "postgres", "false", false},
		{"invalid boolean", "postgres", "yes-please", true},
		{"empty boolean", "postgres", "", true},
		{"sqlite enabled", "sqlite", "true", true},
		{"sqlite disabled", "sqlite", "false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvironment()
			env["LLMGW_DATABASE_DRIVER"] = tc.driver
			if tc.driver == "postgres" {
				env["LLMGW_DATABASE_URL"] = "postgres://gateway:secret@db.example/llmgw"
			}
			env["LLMGW_COORDINATION_RESET_ON_START"] = tc.value
			_, err := config.Load(lookup(env))
			if (err != nil) != tc.wantErr {
				t.Fatalf("Load() error = %v, want error = %t", err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "LLMGW_COORDINATION_RESET_ON_START") {
				t.Fatalf("error does not identify the toggle: %v", err)
			}
		})
	}
}

func TestLoadCoordinationStartupWaitValidation(t *testing.T) {
	for _, value := range []string{"-1s", "invalid"} {
		env := validEnvironment()
		env["LLMGW_DATABASE_DRIVER"] = "postgres"
		env["LLMGW_DATABASE_URL"] = "postgres://gateway:secret@db.example/llmgw"
		env["LLMGW_COORDINATION_STARTUP_WAIT_TIMEOUT"] = value
		if _, err := config.Load(lookup(env)); err == nil {
			t.Fatalf("accepted wait timeout %q", value)
		}
	}
}
