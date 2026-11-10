package httpapi_test

import (
	"net/http"
	"strconv"
	"testing"
)

func TestRequestSizePolicyRoundTripAndLegacyUpdate(t *testing.T) {
	handler, reg, _ := newAdminFixture(t)
	csrf := fetchCSRF(t, handler)
	poolInput := map[string]any{"publicModelName": "bounded", "upstreamModelName": "upstream", "enabled": true}
	created := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/pools", poolInput, http.StatusCreated)
	poolID := jsonInt64(t, created, "id")
	poolPath := "/admin/api/pools/" + strconv.FormatInt(poolID, 10)
	policy, ok := created["requestSizeAdmission"].(map[string]any)
	if !ok || policy["enabled"] != false {
		t.Fatalf("legacy create policy = %#v, want disabled policy", created["requestSizeAdmission"])
	}

	poolInput["requestSizeAdmission"] = map[string]any{
		"enabled": true, "estimationProfile": "qualified-v1", "maxOutstandingTokens": 32768,
		"maxInputTokens": 8192, "maxOutputTokens": 2048, "defaultOutputTokens": 512,
		"classLimits": map[string]any{"background": map[string]any{"maxOutputTokens": 256}},
	}
	adminJSON(t, handler, csrf, http.MethodPut, poolPath, poolInput, http.StatusOK)
	enabled := reg.Snapshot().PoolsByID[poolID]
	delete(poolInput, "requestSizeAdmission")
	poolInput["publicModelName"] = "renamed"
	adminJSON(t, handler, csrf, http.MethodPut, poolPath, poolInput, http.StatusOK)
	preserved := reg.Snapshot().PoolsByID[poolID]
	if !enabled.RequestSizeAdmission.Enabled || preserved.RequestSizeAdmission != enabled.RequestSizeAdmission || preserved.Revision <= enabled.Revision {
		t.Fatalf("legacy update lost the enabled policy or revision: before=%+v after=%+v", enabled, preserved)
	}
	if preserved.RequestSizeAdmission.ClassLimits.Background.MaxOutputTokens != 256 {
		t.Fatal("Background override was not published")
	}
	poolInput["requestSizeAdmission"] = map[string]any{"enabled": false}
	adminJSON(t, handler, csrf, http.MethodPut, poolPath, poolInput, http.StatusOK)
	if reg.Snapshot().PoolsByID[poolID].RequestSizeAdmission.Enabled {
		t.Fatal("explicit false did not disable request size admission")
	}

	clientInput := map[string]any{
		"name": "client", "enabled": true, "priorityClass": "background", "maxConcurrency": 2,
		"modelPoolIds": []int64{poolID}, "maxOutputTokens": 64, "maxOutstandingTokens": 8192,
	}
	client := adminJSON(t, handler, csrf, http.MethodPost, "/admin/api/clients", clientInput, http.StatusCreated)
	clientPath := "/admin/api/clients/" + strconv.FormatInt(jsonInt64(t, client, "id"), 10)
	delete(clientInput, "maxOutputTokens")
	delete(clientInput, "maxOutstandingTokens")
	clientInput["name"] = "renamed client"
	updated := adminJSON(t, handler, csrf, http.MethodPut, clientPath, clientInput, http.StatusOK)
	assertJSONNumber(t, updated, "maxOutputTokens", 64)
	assertJSONNumber(t, updated, "maxOutstandingTokens", 8192)
	clientInput["maxOutputTokens"] = 0
	updated = adminJSON(t, handler, csrf, http.MethodPut, clientPath, clientInput, http.StatusOK)
	assertJSONNumber(t, updated, "maxOutputTokens", 0)
	assertJSONNumber(t, updated, "maxOutstandingTokens", 8192)
}
