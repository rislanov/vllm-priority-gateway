package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
)

func TestWriteAdminErrorMapsPostgreSQLNotFoundAndConflict(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "not found", err: fmt.Errorf("update PostgreSQL client: %w", pgx.ErrNoRows), wantStatus: http.StatusNotFound, wantCode: "not_found"},
		{name: "conflict", err: fmt.Errorf("insert PostgreSQL client: %w", &pgconn.PgError{Code: "23505"}), wantStatus: http.StatusConflict, wantCode: "conflict"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeAdminError(response, test.err)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != test.wantCode {
				t.Fatalf("error code = %q, want %q", body.Error.Code, test.wantCode)
			}
		})
	}
}

func TestAdminStatusRedactsPublicationFailure(t *testing.T) {
	service := &AdminService{registry: registry.New(nil), now: time.Now}
	service.setDegraded(errors.New("connect postgres://operator:private-password@private-db:5432/config"))
	response := httptest.NewRecorder()
	NewAdminAPI(service).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/api/status", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"degraded":`) {
		t.Fatalf("missing degraded status: %d %s", response.Code, response.Body.String())
	}
	for _, forbidden := range []string{"postgres://", "private-password", "private-db"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("database details leaked in status: %s", response.Body.String())
		}
	}
}

func TestWriteAdminErrorRedactsDatabaseFailures(t *testing.T) {
	const secret = "postgres://operator:private-password@private-db:5432/config"
	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "deadline", err: context.DeadlineExceeded, code: "configuration_unavailable"},
		{name: "canceled", err: context.Canceled, code: "configuration_unavailable"},
		{name: "connection", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New(secret)}, code: "configuration_unavailable"},
		{name: "postgres connection error", err: &pgconn.PgError{Code: "08006", Message: secret}, code: "configuration_unavailable"},
		{name: "postgres statement timeout", err: &pgconn.PgError{Code: "57014", Message: secret}, code: "configuration_unavailable"},
		{name: "wrapped unknown database error", err: fmt.Errorf("update PostgreSQL backend: %w", errors.New(secret)), code: "configuration_unavailable"},
		{name: "snapshot publication", err: fmt.Errorf("publish configuration: %w", errors.New(secret)), code: "configuration_degraded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeAdminError(response, test.err)
			if response.Code != http.StatusServiceUnavailable {
				t.Errorf("status=%d, want retryable HTTP 503", response.Code)
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != test.code {
				t.Errorf("code=%s, want %s", body.Error.Code, test.code)
			}
			for _, forbidden := range []string{"postgres://", "private-password", "private-db"} {
				if strings.Contains(response.Body.String(), forbidden) {
					t.Errorf("database details leaked in response: %s", response.Body.String())
				}
			}
		})
	}
}
