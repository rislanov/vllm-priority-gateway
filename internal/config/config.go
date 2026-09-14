package config

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type LookupFunc func(string) (string, bool)

type Config struct {
	postgresSettingsExplicit      bool
	ListenAddress                 string
	DatabaseDriver                string
	DatabasePath                  string
	DatabaseURL                   string
	DatabaseMigrationURL          string
	PostgresConfigMaxConns        int
	PostgresAnalyticsMaxConns     int
	PostgresCoordinationMaxConns  int
	ConfigPollInterval            time.Duration
	CoordinationTimeout           time.Duration
	LeaseTTL                      time.Duration
	LeaseRenewInterval            time.Duration
	CoordinationCompletionBacklog int
	EmergencyCriticalMaxInflight  int
	EmergencyHighMaxInflight      int
	AdminUsername                 string
	AdminPassword                 string
	APIKeyHMACSecret              []byte
	HealthInterval                time.Duration
	HealthTimeout                 time.Duration
	MetricsInterval               time.Duration
	MetricsTimeout                time.Duration
	MetricsStaleAfter             time.Duration
	UnhealthyAfter                int
	RecoveryAfter                 int
	CircuitFailureThreshold       int
	CircuitFailureWindow          time.Duration
	CircuitOpenCooldown           time.Duration
	CircuitHalfOpenMaxProbes      int
	QueueSoftLimit                float64
	KVSoftLimit                   float64
	KVHardLimit                   float64
	EWMAWindow                    time.Duration
	BusyThreshold                 float64
	SaturatedThreshold            float64
	EmergencyThreshold            float64
	BusyRecoveryThreshold         float64
	SaturatedRecoveryThreshold    float64
	EmergencyRecoveryThreshold    float64
	OverloadEnterWindow           time.Duration
	OverloadRecoveryWindow        time.Duration
	RequestBodyLimit              int64
	RetryAfter                    time.Duration
	RoutingPressureEpsilon        float64
	SessionAffinityMaxPressure    float64
	DialTimeout                   time.Duration
	TLSHandshakeTimeout           time.Duration
	ResponseHeaderTimeout         time.Duration
	ShutdownGracePeriod           time.Duration
	AnalyticsRetention            time.Duration
}

func Load(lookup LookupFunc) (Config, error) {
	cfg := Config{
		ListenAddress:                 ":8080",
		DatabaseDriver:                "sqlite",
		DatabasePath:                  "./data/llmgw.db",
		PostgresConfigMaxConns:        4,
		PostgresAnalyticsMaxConns:     4,
		PostgresCoordinationMaxConns:  16,
		ConfigPollInterval:            5 * time.Second,
		CoordinationTimeout:           50 * time.Millisecond,
		LeaseTTL:                      90 * time.Second,
		LeaseRenewInterval:            30 * time.Second,
		CoordinationCompletionBacklog: 4096,
		EmergencyCriticalMaxInflight:  4,
		EmergencyHighMaxInflight:      2,
		HealthInterval:                2 * time.Second,
		HealthTimeout:                 time.Second,
		MetricsInterval:               time.Second,
		MetricsTimeout:                time.Second,
		MetricsStaleAfter:             5 * time.Second,
		UnhealthyAfter:                3,
		RecoveryAfter:                 2,
		CircuitFailureThreshold:       5,
		CircuitFailureWindow:          30 * time.Second,
		CircuitOpenCooldown:           15 * time.Second,
		CircuitHalfOpenMaxProbes:      1,
		QueueSoftLimit:                2,
		KVSoftLimit:                   .80,
		KVHardLimit:                   .95,
		EWMAWindow:                    4 * time.Second,
		BusyThreshold:                 .70,
		SaturatedThreshold:            1.00,
		EmergencyThreshold:            1.40,
		BusyRecoveryThreshold:         .55,
		SaturatedRecoveryThreshold:    .85,
		EmergencyRecoveryThreshold:    1.20,
		OverloadEnterWindow:           3 * time.Second,
		OverloadRecoveryWindow:        10 * time.Second,
		RequestBodyLimit:              16 << 20,
		RetryAfter:                    2 * time.Second,
		RoutingPressureEpsilon:        .02,
		SessionAffinityMaxPressure:    1.00,
		DialTimeout:                   3 * time.Second,
		TLSHandshakeTimeout:           3 * time.Second,
		ResponseHeaderTimeout:         30 * time.Second,
		ShutdownGracePeriod:           30 * time.Second,
		AnalyticsRetention:            2160 * time.Hour,
	}

	cfg.ListenAddress = stringValue(lookup, "LLMGW_LISTEN_ADDRESS", cfg.ListenAddress)
	for _, key := range []string{"LLMGW_DATABASE_URL", "LLMGW_DATABASE_MIGRATION_URL", "LLMGW_POSTGRES_CONFIG_MAX_CONNS", "LLMGW_POSTGRES_ANALYTICS_MAX_CONNS", "LLMGW_POSTGRES_COORDINATION_MAX_CONNS", "LLMGW_CONFIG_POLL_INTERVAL", "LLMGW_COORDINATION_TIMEOUT"} {
		if _, exists := lookup(key); exists {
			cfg.postgresSettingsExplicit = true
		}
	}
	cfg.DatabaseDriver = strings.ToLower(strings.TrimSpace(stringValue(lookup, "LLMGW_DATABASE_DRIVER", cfg.DatabaseDriver)))
	cfg.DatabasePath = stringValue(lookup, "LLMGW_DATABASE_PATH", cfg.DatabasePath)
	cfg.DatabaseURL = stringValue(lookup, "LLMGW_DATABASE_URL", "")
	cfg.DatabaseMigrationURL = stringValue(lookup, "LLMGW_DATABASE_MIGRATION_URL", "")
	cfg.AdminUsername = stringValue(lookup, "LLMGW_ADMIN_USERNAME", "")
	cfg.AdminPassword = stringValue(lookup, "LLMGW_ADMIN_PASSWORD", "")
	cfg.APIKeyHMACSecret = []byte(stringValue(lookup, "LLMGW_API_KEY_HMAC_SECRET", ""))

	var err error
	if cfg.PostgresConfigMaxConns, err = intValue(lookup, "LLMGW_POSTGRES_CONFIG_MAX_CONNS", cfg.PostgresConfigMaxConns); err != nil {
		return Config{}, err
	}
	if cfg.PostgresAnalyticsMaxConns, err = intValue(lookup, "LLMGW_POSTGRES_ANALYTICS_MAX_CONNS", cfg.PostgresAnalyticsMaxConns); err != nil {
		return Config{}, err
	}
	if cfg.PostgresCoordinationMaxConns, err = intValue(lookup, "LLMGW_POSTGRES_COORDINATION_MAX_CONNS", cfg.PostgresCoordinationMaxConns); err != nil {
		return Config{}, err
	}
	if cfg.ConfigPollInterval, err = durationValue(lookup, "LLMGW_CONFIG_POLL_INTERVAL", cfg.ConfigPollInterval); err != nil {
		return Config{}, err
	}
	if cfg.CoordinationTimeout, err = durationValue(lookup, "LLMGW_COORDINATION_TIMEOUT", cfg.CoordinationTimeout); err != nil {
		return Config{}, err
	}
	if cfg.LeaseTTL, err = durationValue(lookup, "LLMGW_LEASE_TTL", cfg.LeaseTTL); err != nil {
		return Config{}, err
	}
	if cfg.LeaseRenewInterval, err = durationValue(lookup, "LLMGW_LEASE_RENEW_INTERVAL", cfg.LeaseRenewInterval); err != nil {
		return Config{}, err
	}
	if cfg.CoordinationCompletionBacklog, err = intValue(lookup, "LLMGW_COORDINATION_COMPLETION_BACKLOG", cfg.CoordinationCompletionBacklog); err != nil {
		return Config{}, err
	}
	if cfg.EmergencyCriticalMaxInflight, err = intValue(lookup, "LLMGW_COORDINATION_EMERGENCY_CRITICAL_MAX_INFLIGHT", cfg.EmergencyCriticalMaxInflight); err != nil {
		return Config{}, err
	}
	if cfg.EmergencyHighMaxInflight, err = intValue(lookup, "LLMGW_COORDINATION_EMERGENCY_HIGH_MAX_INFLIGHT", cfg.EmergencyHighMaxInflight); err != nil {
		return Config{}, err
	}
	if cfg.HealthInterval, err = durationValue(lookup, "LLMGW_HEALTH_INTERVAL", cfg.HealthInterval); err != nil {
		return Config{}, err
	}
	if cfg.HealthTimeout, err = durationValue(lookup, "LLMGW_HEALTH_TIMEOUT", cfg.HealthTimeout); err != nil {
		return Config{}, err
	}
	if cfg.MetricsInterval, err = durationValue(lookup, "LLMGW_METRICS_INTERVAL", cfg.MetricsInterval); err != nil {
		return Config{}, err
	}
	if cfg.MetricsTimeout, err = durationValue(lookup, "LLMGW_METRICS_TIMEOUT", cfg.MetricsTimeout); err != nil {
		return Config{}, err
	}
	if cfg.MetricsStaleAfter, err = durationValue(lookup, "LLMGW_METRICS_STALE_AFTER", cfg.MetricsStaleAfter); err != nil {
		return Config{}, err
	}
	if cfg.UnhealthyAfter, err = intValue(lookup, "LLMGW_UNHEALTHY_AFTER", cfg.UnhealthyAfter); err != nil {
		return Config{}, err
	}
	if cfg.RecoveryAfter, err = intValue(lookup, "LLMGW_RECOVERY_AFTER", cfg.RecoveryAfter); err != nil {
		return Config{}, err
	}
	if cfg.CircuitFailureThreshold, err = intValue(lookup, "LLMGW_CIRCUIT_FAILURE_THRESHOLD", cfg.CircuitFailureThreshold); err != nil {
		return Config{}, err
	}
	if cfg.CircuitFailureWindow, err = durationValue(lookup, "LLMGW_CIRCUIT_FAILURE_WINDOW", cfg.CircuitFailureWindow); err != nil {
		return Config{}, err
	}
	if cfg.CircuitOpenCooldown, err = durationValue(lookup, "LLMGW_CIRCUIT_OPEN_COOLDOWN", cfg.CircuitOpenCooldown); err != nil {
		return Config{}, err
	}
	if cfg.CircuitHalfOpenMaxProbes, err = intValue(lookup, "LLMGW_CIRCUIT_HALF_OPEN_MAX_PROBES", cfg.CircuitHalfOpenMaxProbes); err != nil {
		return Config{}, err
	}
	if cfg.QueueSoftLimit, err = floatValue(lookup, "LLMGW_QUEUE_SOFT_LIMIT", cfg.QueueSoftLimit); err != nil {
		return Config{}, err
	}
	if cfg.KVSoftLimit, err = floatValue(lookup, "LLMGW_KV_SOFT_LIMIT", cfg.KVSoftLimit); err != nil {
		return Config{}, err
	}
	if cfg.KVHardLimit, err = floatValue(lookup, "LLMGW_KV_HARD_LIMIT", cfg.KVHardLimit); err != nil {
		return Config{}, err
	}
	if cfg.EWMAWindow, err = durationValue(lookup, "LLMGW_EWMA_WINDOW", cfg.EWMAWindow); err != nil {
		return Config{}, err
	}
	if cfg.BusyThreshold, err = floatValue(lookup, "LLMGW_BUSY_THRESHOLD", cfg.BusyThreshold); err != nil {
		return Config{}, err
	}
	if cfg.SaturatedThreshold, err = floatValue(lookup, "LLMGW_SATURATED_THRESHOLD", cfg.SaturatedThreshold); err != nil {
		return Config{}, err
	}
	if cfg.EmergencyThreshold, err = floatValue(lookup, "LLMGW_EMERGENCY_THRESHOLD", cfg.EmergencyThreshold); err != nil {
		return Config{}, err
	}
	if cfg.BusyRecoveryThreshold, err = floatValue(lookup, "LLMGW_BUSY_RECOVERY_THRESHOLD", cfg.BusyRecoveryThreshold); err != nil {
		return Config{}, err
	}
	if cfg.SaturatedRecoveryThreshold, err = floatValue(lookup, "LLMGW_SATURATED_RECOVERY_THRESHOLD", cfg.SaturatedRecoveryThreshold); err != nil {
		return Config{}, err
	}
	if cfg.EmergencyRecoveryThreshold, err = floatValue(lookup, "LLMGW_EMERGENCY_RECOVERY_THRESHOLD", cfg.EmergencyRecoveryThreshold); err != nil {
		return Config{}, err
	}
	if cfg.OverloadEnterWindow, err = durationValue(lookup, "LLMGW_OVERLOAD_ENTER_WINDOW", cfg.OverloadEnterWindow); err != nil {
		return Config{}, err
	}
	if cfg.OverloadRecoveryWindow, err = durationValue(lookup, "LLMGW_OVERLOAD_RECOVERY_WINDOW", cfg.OverloadRecoveryWindow); err != nil {
		return Config{}, err
	}
	if cfg.RequestBodyLimit, err = int64Value(lookup, "LLMGW_REQUEST_BODY_LIMIT", cfg.RequestBodyLimit); err != nil {
		return Config{}, err
	}
	if cfg.RetryAfter, err = durationValue(lookup, "LLMGW_RETRY_AFTER", cfg.RetryAfter); err != nil {
		return Config{}, err
	}
	if cfg.RoutingPressureEpsilon, err = floatValue(lookup, "LLMGW_ROUTING_PRESSURE_EPSILON", cfg.RoutingPressureEpsilon); err != nil {
		return Config{}, err
	}
	if cfg.SessionAffinityMaxPressure, err = floatValue(lookup, "LLMGW_SESSION_AFFINITY_MAX_PRESSURE", cfg.SessionAffinityMaxPressure); err != nil {
		return Config{}, err
	}
	if cfg.DialTimeout, err = durationValue(lookup, "LLMGW_DIAL_TIMEOUT", cfg.DialTimeout); err != nil {
		return Config{}, err
	}
	if cfg.TLSHandshakeTimeout, err = durationValue(lookup, "LLMGW_TLS_HANDSHAKE_TIMEOUT", cfg.TLSHandshakeTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ResponseHeaderTimeout, err = durationValue(lookup, "LLMGW_RESPONSE_HEADER_TIMEOUT", cfg.ResponseHeaderTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownGracePeriod, err = durationValue(lookup, "LLMGW_SHUTDOWN_GRACE_PERIOD", cfg.ShutdownGracePeriod); err != nil {
		return Config{}, err
	}
	if cfg.AnalyticsRetention, err = durationValue(lookup, "LLMGW_ANALYTICS_RETENTION", cfg.AnalyticsRetention); err != nil {
		return Config{}, err
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	if cfg.DatabaseDriver == "postgres" && cfg.DatabaseMigrationURL == "" {
		cfg.DatabaseMigrationURL = cfg.DatabaseURL
	}
	return cfg, nil
}

func (c Config) validate() error {
	if strings.TrimSpace(c.AdminUsername) == "" {
		return errors.New("LLMGW_ADMIN_USERNAME is required")
	}
	if len(c.AdminPassword) < 16 {
		return errors.New("LLMGW_ADMIN_PASSWORD must contain at least 16 bytes")
	}
	if len(c.APIKeyHMACSecret) < 32 {
		return errors.New("LLMGW_API_KEY_HMAC_SECRET must contain at least 32 bytes")
	}
	if strings.TrimSpace(c.ListenAddress) == "" {
		return errors.New("listen address is required")
	}
	if err := c.validateDatabaseProfile(); err != nil {
		return err
	}
	for name, value := range map[string]time.Duration{
		"health interval": c.HealthInterval, "health timeout": c.HealthTimeout,
		"metrics interval": c.MetricsInterval, "metrics timeout": c.MetricsTimeout,
		"metrics stale after": c.MetricsStaleAfter, "EWMA window": c.EWMAWindow,
		"circuit failure window": c.CircuitFailureWindow, "circuit open cooldown": c.CircuitOpenCooldown,
		"overload enter window": c.OverloadEnterWindow, "overload recovery window": c.OverloadRecoveryWindow,
		"retry after": c.RetryAfter, "dial timeout": c.DialTimeout,
		"TLS handshake timeout": c.TLSHandshakeTimeout, "response header timeout": c.ResponseHeaderTimeout,
		"shutdown grace period": c.ShutdownGracePeriod,
		"config poll interval":  c.ConfigPollInterval, "coordination timeout": c.CoordinationTimeout,
		"lease TTL": c.LeaseTTL, "lease renewal interval": c.LeaseRenewInterval,
	} {
		if value <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.UnhealthyAfter <= 0 || c.RecoveryAfter <= 0 {
		return errors.New("health transition counts must be positive")
	}
	if c.AnalyticsRetention < 0 {
		return errors.New("analytics retention must be non-negative")
	}
	if c.CircuitFailureThreshold <= 0 || c.CircuitHalfOpenMaxProbes <= 0 {
		return errors.New("circuit breaker counts must be positive")
	}
	if c.CircuitFailureWindow > 24*time.Hour-5*time.Minute {
		return errors.New("circuit failure window must not exceed 23h55m")
	}
	if c.LeaseRenewInterval > c.LeaseTTL/3 {
		return errors.New("lease renewal interval must not exceed one third of lease TTL")
	}
	if c.PostgresConfigMaxConns <= 0 || c.PostgresAnalyticsMaxConns <= 0 || c.PostgresCoordinationMaxConns <= 0 {
		return errors.New("PostgreSQL pool limits must be positive")
	}
	if c.CoordinationCompletionBacklog <= 0 || c.EmergencyCriticalMaxInflight < 0 || c.EmergencyHighMaxInflight < 0 {
		return errors.New("coordination backlog must be positive and emergency limits non-negative")
	}
	if !finitePositive(c.QueueSoftLimit) {
		return errors.New("queue soft limit must be positive")
	}
	if math.IsNaN(c.KVSoftLimit) || math.IsInf(c.KVSoftLimit, 0) ||
		math.IsNaN(c.KVHardLimit) || math.IsInf(c.KVHardLimit, 0) ||
		c.KVSoftLimit < 0 || c.KVHardLimit > 1 || c.KVSoftLimit >= c.KVHardLimit {
		return errors.New("KV limits must satisfy 0 <= soft < hard <= 1")
	}
	if !(c.BusyRecoveryThreshold < c.BusyThreshold && c.BusyThreshold < c.SaturatedRecoveryThreshold && c.SaturatedRecoveryThreshold < c.SaturatedThreshold && c.SaturatedThreshold < c.EmergencyRecoveryThreshold && c.EmergencyRecoveryThreshold < c.EmergencyThreshold) {
		return errors.New("pool thresholds and recovery thresholds are out of order")
	}
	if c.RequestBodyLimit < 1024 {
		return errors.New("request body limit must be at least 1024 bytes")
	}
	if c.RoutingPressureEpsilon < 0 || math.IsNaN(c.RoutingPressureEpsilon) || math.IsInf(c.RoutingPressureEpsilon, 0) {
		return errors.New("routing pressure epsilon must be finite and non-negative")
	}
	if !finitePositive(c.SessionAffinityMaxPressure) {
		return errors.New("session affinity max pressure must be finite and positive")
	}
	return nil
}

func (c Config) validateDatabaseProfile() error {
	switch c.DatabaseDriver {
	case "sqlite":
		if strings.TrimSpace(c.DatabasePath) == "" {
			return errors.New("LLMGW_DATABASE_PATH is required for sqlite")
		}
		if c.postgresSettingsExplicit {
			return errors.New("PostgreSQL-only settings require LLMGW_DATABASE_DRIVER=postgres")
		}
		for _, key := range []string{"LLMGW_DATABASE_URL", "LLMGW_DATABASE_MIGRATION_URL"} {
			value := map[string]string{"LLMGW_DATABASE_URL": c.DatabaseURL, "LLMGW_DATABASE_MIGRATION_URL": c.DatabaseMigrationURL}[key]
			if strings.TrimSpace(value) != "" {
				return fmt.Errorf("%s is valid only for postgres", key)
			}
		}
	case "postgres":
		if strings.TrimSpace(c.DatabaseURL) == "" {
			return errors.New("LLMGW_DATABASE_URL is required for postgres")
		}
		for _, raw := range []string{c.DatabaseURL, c.DatabaseMigrationURL} {
			if raw == "" {
				continue
			}
			parsed, err := url.Parse(raw)
			if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
				return errors.New("invalid PostgreSQL connection URL")
			}
			if mode := parsed.Query().Get("default_query_exec_mode"); mode != "" && mode != "exec" {
				return errors.New("PostgreSQL runtime URL requires default_query_exec_mode=exec")
			}
		}
	default:
		return fmt.Errorf("unsupported LLMGW_DATABASE_DRIVER %q", c.DatabaseDriver)
	}
	return nil
}

func stringValue(lookup LookupFunc, key, fallback string) string {
	if value, ok := lookup(key); ok {
		return value
	}
	return fallback
}

func durationValue(lookup LookupFunc, key string, fallback time.Duration) (time.Duration, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func intValue(lookup LookupFunc, key string, fallback int) (int, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func int64Value(lookup LookupFunc, key string, fallback int64) (int64, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func floatValue(lookup LookupFunc, key string, fallback float64) (float64, error) {
	value, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
