package compose_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

type grafanaDashboard struct {
	UID        string `json:"uid"`
	Templating struct {
		List []struct {
			Name string `json:"name"`
		} `json:"list"`
	} `json:"templating"`
	Panels []struct {
		Targets []struct {
			Datasource struct {
				UID string `json:"uid"`
			} `json:"datasource"`
			Expr string `json:"expr"`
		} `json:"targets"`
	} `json:"panels"`
}

func TestObservabilityOverlayAndDashboardContract(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "compose", "version").Run(); err != nil {
		t.Skip("docker compose is not available")
	}

	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	envPath := filepath.Join(t.TempDir(), "compose.env")
	const environment = "LLMGW_ADMIN_PASSWORD=compose-contract-admin-password\n" +
		"LLMGW_API_KEY_HMAC_SECRET=compose-contract-hmac-secret-at-least-32-bytes\n" +
		"LLMGW_PROMETHEUS_PORT=19090\n" +
		"LLMGW_GRAFANA_PORT=13000\n"
	if err := os.WriteFile(envPath, []byte(environment), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(
		"docker", "compose",
		"--project-name", "llmgw-observability-contract",
		"--project-directory", repositoryRoot,
		"--env-file", envPath,
		"-f", filepath.Join(repositoryRoot, "compose.yaml"),
		"-f", filepath.Join(repositoryRoot, "compose.observability.yaml"),
		"config", "--format", "json",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render observability Compose overlay: %v\n%s", err, output)
	}
	var config renderedCompose
	if err := json.Unmarshal(output, &config); err != nil {
		t.Fatalf("decode observability Compose overlay: %v\n%s", err, output)
	}

	prometheus := config.Services["prometheus"]
	if prometheus.Image != "prom/prometheus:v3.14.0" {
		t.Errorf("Prometheus image = %q", prometheus.Image)
	}
	if len(prometheus.Ports) != 1 || prometheus.Ports[0].HostIP != "127.0.0.1" || prometheus.Ports[0].Published != "19090" || prometheus.Ports[0].Target != 9090 {
		t.Errorf("Prometheus ports = %#v", prometheus.Ports)
	}
	if prometheus.DependsOn["gateway"].Condition != "service_healthy" {
		t.Errorf("Prometheus gateway dependency = %#v", prometheus.DependsOn["gateway"])
	}
	assertVolume(t, prometheus.Volumes, "bind", filepath.Join(repositoryRoot, "deploy", "observability", "prometheus.yml"), "/etc/prometheus/prometheus.yml", true, "")
	assertVolume(t, prometheus.Volumes, "volume", "prometheus-data", "/prometheus", false, "")

	grafana := config.Services["grafana"]
	if grafana.Image != "grafana/grafana:13.2.0" {
		t.Errorf("Grafana image = %q", grafana.Image)
	}
	if len(grafana.Ports) != 1 || grafana.Ports[0].HostIP != "127.0.0.1" || grafana.Ports[0].Published != "13000" || grafana.Ports[0].Target != 3000 {
		t.Errorf("Grafana ports = %#v", grafana.Ports)
	}
	if _, ok := grafana.DependsOn["prometheus"]; !ok {
		t.Error("Grafana does not depend on Prometheus")
	}
	assertVolume(t, grafana.Volumes, "bind", filepath.Join(repositoryRoot, "deploy", "observability", "grafana", "provisioning"), "/etc/grafana/provisioning", true, "")
	assertVolume(t, grafana.Volumes, "bind", filepath.Join(repositoryRoot, "deploy", "observability", "grafana", "dashboards"), "/var/lib/grafana/dashboards", true, "")
	assertVolume(t, grafana.Volumes, "volume", "grafana-data", "/var/lib/grafana", false, "")
	for _, name := range []string{"prometheus-data", "grafana-data"} {
		if _, ok := config.Volumes[name]; !ok {
			t.Errorf("Compose overlay is missing named volume %q", name)
		}
	}

	dashboardPath := filepath.Join(repositoryRoot, "deploy", "observability", "grafana", "dashboards", "gateway-decisions.json")
	dashboardData, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatal(err)
	}
	var dashboard grafanaDashboard
	if err := json.Unmarshal(dashboardData, &dashboard); err != nil {
		t.Fatalf("decode dashboard JSON: %v", err)
	}
	if dashboard.UID != "llmgw-gateway-decisions" {
		t.Errorf("dashboard UID = %q", dashboard.UID)
	}
	variables := make([]string, 0, len(dashboard.Templating.List))
	for _, variable := range dashboard.Templating.List {
		variables = append(variables, variable.Name)
	}
	for _, required := range []string{"model", "backend", "client", "priority"} {
		if !slices.Contains(variables, required) {
			t.Errorf("dashboard variables = %v, missing %q", variables, required)
		}
	}

	wantQueries := []string{
		`max by (model) (llmgw_pool_pressure{model=~"$model"})`,
		`sum by (reason) (rate(llmgw_requests_rejected_total{model=~"$model",priority_class="background"}[$__rate_interval]))`,
		`histogram_quantile(0.95, sum by (le) (rate(llmgw_request_duration_seconds_bucket{model=~"$model",priority_class="high",status_class="2xx"}[$__rate_interval])))`,
		`histogram_quantile(0.95, sum by (le) (rate(llmgw_queue_wait_seconds_bucket{model=~"$model",priority_class="high",outcome="selected"}[$__rate_interval])))`,
	}
	var queries []string
	for panelIndex, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			queries = append(queries, target.Expr)
			if target.Datasource.UID != "llmgw-prometheus" {
				t.Errorf("panel %d target datasource UID = %q", panelIndex, target.Datasource.UID)
			}
		}
	}
	for _, required := range wantQueries {
		if !slices.Contains(queries, required) {
			t.Errorf("dashboard is missing causal query %q", required)
		}
	}
}
