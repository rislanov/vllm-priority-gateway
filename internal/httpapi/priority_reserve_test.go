package httpapi_test

import (
	"net/http"
	"strconv"
	"testing"
)

func TestAdminPoolPriorityReserveRoundTripAndValidation(t *testing.T) {
	h, _, _ := newAdminFixture(t)
	csrf := fetchCSRF(t, h)
	payload := map[string]any{"publicModelName": "reserved", "upstreamModelName": "upstream", "enabled": true, "maxGatewayInflight": 20, "highPriorityReserve": 6}
	created := adminJSON(t, h, csrf, http.MethodPost, "/admin/api/pools", payload, http.StatusCreated)
	assertJSONNumber(t, created, "highPriorityReserve", 6)
	id := jsonInt64(t, created, "id")
	url := "/admin/api/pools/" + strconv.FormatInt(id, 10)
	for _, tt := range []struct{ limit, reserve int }{{20, -1}, {20, 21}, {0, 1}, {5, 6}} {
		payload["maxGatewayInflight"] = tt.limit
		payload["highPriorityReserve"] = tt.reserve
		adminJSON(t, h, csrf, http.MethodPut, url, payload, http.StatusBadRequest)
	}
	payload["maxGatewayInflight"] = 20
	payload["highPriorityReserve"] = 20
	updated := adminJSON(t, h, csrf, http.MethodPut, url, payload, http.StatusOK)
	assertJSONNumber(t, updated, "highPriorityReserve", 20)
	delete(payload, "highPriorityReserve")
	updated = adminJSON(t, h, csrf, http.MethodPut, url, payload, http.StatusOK)
	assertJSONNumber(t, updated, "highPriorityReserve", 0)
}
