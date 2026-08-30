package compose_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type renderedCompose struct {
	Services map[string]renderedService `json:"services"`
	Volumes  map[string]any             `json:"volumes"`
}

type renderedService struct {
	Build       json.RawMessage               `json:"build"`
	Command     []string                      `json:"command"`
	DependsOn   map[string]renderedDependency `json:"depends_on"`
	Deploy      renderedDeploy                `json:"deploy"`
	Environment map[string]string             `json:"environment"`
	Healthcheck renderedHealthcheck           `json:"healthcheck"`
	Image       string                        `json:"image"`
	Networks    map[string]any                `json:"networks"`
	Ports       []renderedPort                `json:"ports"`
	Profiles    []string                      `json:"profiles"`
	PullPolicy  string                        `json:"pull_policy"`
	Volumes     []renderedVolume              `json:"volumes"`
}

type renderedDependency struct {
	Condition string `json:"condition"`
}

type renderedDeploy struct {
	Resources struct {
		Reservations struct {
			Devices []renderedDevice `json:"devices"`
		} `json:"reservations"`
	} `json:"resources"`
}

type renderedDevice struct {
	Capabilities []string `json:"capabilities"`
	Count        int      `json:"count"`
	Driver       string   `json:"driver"`
}

type renderedHealthcheck struct {
	Test []string `json:"test"`
}

type renderedVolume struct {
	Type     string `json:"type"`
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
	Bind     struct {
		SELinux string `json:"selinux"`
	} `json:"bind"`
}

type renderedPort struct {
	HostIP    string `json:"host_ip"`
	Published string `json:"published"`
	Target    int    `json:"target"`
}

func TestComposeQuickStartRendersPortableStack(t *testing.T) {
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
	const environment = "LLMGW_ADMIN_USERNAME=operator\n" +
		"LLMGW_ADMIN_PASSWORD=compose-contract-admin-password\n" +
		"LLMGW_API_KEY_HMAC_SECRET=compose-contract-hmac-secret-at-least-32-bytes\n" +
		"LLMGW_PORT=18080\n"
	if err := os.WriteFile(envPath, []byte(environment), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(
		"docker", "compose",
		"--project-name", "llmgw-compose-contract",
		"--project-directory", repositoryRoot,
		"--env-file", envPath,
		"-f", filepath.Join(repositoryRoot, "compose.yaml"),
		"--profile", "tools",
		"config", "--format", "json",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render compose quick start: %v\n%s", err, output)
	}

	var config renderedCompose
	if err := json.Unmarshal(output, &config); err != nil {
		t.Fatalf("decode rendered compose configuration: %v\n%s", err, output)
	}
	for _, name := range []string{"gateway", "probe", "vllm-a", "vllm-b"} {
		if _, ok := config.Services[name]; !ok {
			t.Errorf("rendered Compose stack is missing %q", name)
		}
	}

	gateway := config.Services["gateway"]
	if gateway.Image != "vllm-priority-gateway:local" {
		t.Errorf("gateway image = %q, want vllm-priority-gateway:local", gateway.Image)
	}
	if len(gateway.Build) == 0 || string(gateway.Build) == "null" {
		t.Error("gateway service must build the repository Dockerfile")
	}
	for _, dependency := range []string{"vllm-a", "vllm-b"} {
		if gateway.DependsOn[dependency].Condition != "service_healthy" {
			t.Errorf("gateway dependency %s condition = %q, want service_healthy", dependency, gateway.DependsOn[dependency].Condition)
		}
	}
	if len(gateway.Ports) != 1 || gateway.Ports[0].HostIP != "127.0.0.1" || gateway.Ports[0].Target != 8080 || gateway.Ports[0].Published != "18080" {
		t.Errorf("gateway ports = %#v, want 127.0.0.1:18080 -> 8080", gateway.Ports)
	}
	if !slices.Equal(gateway.Healthcheck.Test, []string{"CMD", "/gateway", "healthcheck"}) {
		t.Errorf("gateway healthcheck = %#v, want gateway healthcheck command", gateway.Healthcheck.Test)
	}
	if gateway.Environment["LLMGW_ADMIN_PASSWORD"] != "compose-contract-admin-password" {
		t.Error("gateway did not receive the Admin password from the env file")
	}
	if gateway.Environment["LLMGW_API_KEY_HMAC_SECRET"] != "compose-contract-hmac-secret-at-least-32-bytes" {
		t.Error("gateway did not receive the HMAC secret from the env file")
	}
	assertVolume(t, gateway.Volumes, "volume", "gateway-data", "/data", false, "")

	for _, name := range []string{"vllm-a", "vllm-b"} {
		service := config.Services[name]
		if service.Image != "vllm/vllm-openai:v0.28.0" {
			t.Errorf("%s image = %q, want pinned vLLM 0.28.0", name, service.Image)
		}
		if len(service.Ports) != 0 {
			t.Errorf("%s publishes host ports: %#v", name, service.Ports)
		}
		if len(service.Healthcheck.Test) == 0 {
			t.Errorf("%s has no healthcheck", name)
		}
		for _, required := range []string{"Qwen/Qwen3-0.6B", "1024", "1", "qwen-test", "0.32", "--scheduling-policy", "priority", "--enable-prefix-caching", "--enable-prompt-tokens-details", "--enable-request-id-headers"} {
			if !slices.Contains(service.Command, required) {
				t.Errorf("%s command does not contain %q: %#v", name, required, service.Command)
			}
		}
		if service.Environment["VLLM_USE_V2_MODEL_RUNNER"] != "0" {
			t.Errorf("%s compatibility runner = %q, want 0", name, service.Environment["VLLM_USE_V2_MODEL_RUNNER"])
		}
		devices := service.Deploy.Resources.Reservations.Devices
		if len(devices) != 1 || devices[0].Driver != "nvidia" || devices[0].Count != -1 || !slices.Equal(devices[0].Capabilities, []string{"gpu"}) {
			t.Errorf("%s GPU reservation = %#v, want all NVIDIA GPUs with gpu capability", name, devices)
		}
		assertVolume(t, service.Volumes, "volume", "huggingface-cache", "/root/.cache/huggingface", false, "")
	}
	if config.Services["vllm-b"].DependsOn["vllm-a"].Condition != "service_healthy" {
		t.Error("vllm-b must wait for a healthy vllm-a before using the shared cache")
	}
	if len(config.Services["probe"].Profiles) != 1 || config.Services["probe"].Profiles[0] != "tools" {
		t.Errorf("probe profiles = %#v, want [tools]", config.Services["probe"].Profiles)
	}
	if config.Services["probe"].Image != "curlimages/curl:8.17.0" {
		t.Errorf("probe image = %q, want pinned curl image", config.Services["probe"].Image)
	}
	if config.Services["probe"].DependsOn["gateway"].Condition != "service_healthy" {
		t.Error("probe must wait for a healthy gateway when dependencies are enabled")
	}
	assertVolume(t, config.Services["probe"].Volumes, "bind", filepath.Join(repositoryRoot, "examples", "quickstart-chat.json"), "/requests/chat.json", true, "z")
	for _, name := range []string{"gateway-data", "huggingface-cache"} {
		if _, ok := config.Volumes[name]; !ok {
			t.Errorf("rendered Compose stack is missing named volume %q", name)
		}
	}

	sharedNetworks := make(map[string]bool)
	for name := range config.Services["gateway"].Networks {
		if _, a := config.Services["vllm-a"].Networks[name]; a {
			if _, b := config.Services["vllm-b"].Networks[name]; b {
				sharedNetworks[name] = true
			}
		}
	}
	if len(sharedNetworks) == 0 {
		t.Error("gateway and both vLLM services do not share a Compose-managed network")
	}
}

func TestReleaseComposeUsesPublishedGatewayImageWithoutBuildingSource(t *testing.T) {
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
	const environment = "LLMGW_ADMIN_USERNAME=operator\n" +
		"LLMGW_ADMIN_PASSWORD=compose-contract-admin-password\n" +
		"LLMGW_API_KEY_HMAC_SECRET=compose-contract-hmac-secret-at-least-32-bytes\n" +
		"LLMGW_PORT=18080\n"
	if err := os.WriteFile(envPath, []byte(environment), 0o600); err != nil {
		t.Fatal(err)
	}

	config := renderReleaseCompose(t, repositoryRoot, envPath)
	gateway := config.Services["gateway"]
	if gateway.Image != "ghcr.io/rislanov/vllm-priority-gateway:0.2.0" {
		t.Errorf("gateway image = %q, want published v0.2.0 image", gateway.Image)
	}
	if len(gateway.Build) != 0 && string(gateway.Build) != "null" {
		t.Errorf("release gateway unexpectedly has a build configuration: %s", gateway.Build)
	}
	if gateway.PullPolicy != "always" {
		t.Errorf("release gateway pull policy = %q, want always", gateway.PullPolicy)
	}
	if len(gateway.Ports) != 1 || gateway.Ports[0].HostIP != "127.0.0.1" || gateway.Ports[0].Target != 8080 || gateway.Ports[0].Published != "18080" {
		t.Errorf("release gateway ports = %#v, want 127.0.0.1:18080 -> 8080", gateway.Ports)
	}
	for _, dependency := range []string{"vllm-a", "vllm-b"} {
		if gateway.DependsOn[dependency].Condition != "service_healthy" {
			t.Errorf("gateway dependency %s condition = %q, want service_healthy", dependency, gateway.DependsOn[dependency].Condition)
		}
	}
	if !slices.Equal(gateway.Healthcheck.Test, []string{"CMD", "/gateway", "healthcheck"}) {
		t.Errorf("gateway healthcheck = %#v, want gateway healthcheck command", gateway.Healthcheck.Test)
	}
	assertVolume(t, gateway.Volumes, "volume", "gateway-data", "/data", false, "")
	for _, name := range []string{"vllm-a", "vllm-b"} {
		if ports := config.Services[name].Ports; len(ports) != 0 {
			t.Errorf("release %s publishes host ports: %#v", name, ports)
		}
	}
}

func TestReleaseExampleSelectsCurrentPublishedVersion(t *testing.T) {
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
	example, err := os.ReadFile(filepath.Join(repositoryRoot, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	environment := strings.Replace(string(example), "LLMGW_ADMIN_PASSWORD=", "LLMGW_ADMIN_PASSWORD=compose-contract-admin-password", 1)
	environment = strings.Replace(environment, "LLMGW_API_KEY_HMAC_SECRET=", "LLMGW_API_KEY_HMAC_SECRET=compose-contract-hmac-secret-at-least-32-bytes", 1)
	envPath := filepath.Join(t.TempDir(), "compose.env")
	if err := os.WriteFile(envPath, []byte(environment), 0o600); err != nil {
		t.Fatal(err)
	}
	config := renderReleaseCompose(t, repositoryRoot, envPath)
	if image := config.Services["gateway"].Image; image != "ghcr.io/rislanov/vllm-priority-gateway:0.2.0" {
		t.Errorf("gateway image from .env.example = %q, want published v0.2.0 image", image)
	}
}

func renderReleaseCompose(t *testing.T, repositoryRoot, envPath string) renderedCompose {
	t.Helper()
	command := exec.Command(
		"docker", "compose",
		"--project-name", "llmgw-release-contract",
		"--project-directory", repositoryRoot,
		"--env-file", envPath,
		"-f", filepath.Join(repositoryRoot, "compose.yaml"),
		"-f", filepath.Join(repositoryRoot, "compose.release.yaml"),
		"config", "--format", "json",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render release Compose stack: %v\n%s", err, output)
	}

	var config renderedCompose
	if err := json.Unmarshal(output, &config); err != nil {
		t.Fatalf("decode rendered release Compose configuration: %v\n%s", err, output)
	}
	return config
}

func TestNativeLinuxComposePublishesVLLMOnlyOnLoopback(t *testing.T) {
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
	const environment = "LLMGW_ADMIN_USERNAME=operator\n" +
		"LLMGW_ADMIN_PASSWORD=compose-contract-admin-password\n" +
		"LLMGW_API_KEY_HMAC_SECRET=compose-contract-hmac-secret-at-least-32-bytes\n"
	if err := os.WriteFile(envPath, []byte(environment), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(
		"docker", "compose",
		"--project-name", "llmgw-native-contract",
		"--project-directory", repositoryRoot,
		"--env-file", envPath,
		"-f", filepath.Join(repositoryRoot, "compose.yaml"),
		"-f", filepath.Join(repositoryRoot, "compose.native.yaml"),
		"config", "--format", "json",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render native Linux Compose support: %v\n%s", err, output)
	}

	var config renderedCompose
	if err := json.Unmarshal(output, &config); err != nil {
		t.Fatalf("decode rendered native Linux Compose configuration: %v\n%s", err, output)
	}
	for serviceName, published := range map[string]string{"vllm-a": "8001", "vllm-b": "8002"} {
		ports := config.Services[serviceName].Ports
		if len(ports) != 1 || ports[0].HostIP != "127.0.0.1" || ports[0].Published != published || ports[0].Target != 8000 {
			t.Errorf("%s ports = %#v, want 127.0.0.1:%s -> 8000", serviceName, ports, published)
		}
	}
}

func TestDockerBuildContextExcludesLocalSecrets(t *testing.T) {
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.ReplaceAll(string(contents), "\r\n", "\n"), "\n")
	positions := make(map[string]int)
	for index, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			positions[line] = index
		}
	}
	for _, pattern := range []string{".env", ".env.*"} {
		if _, ok := positions[pattern]; !ok {
			t.Errorf(".dockerignore does not exclude %s", pattern)
		}
	}
	if positions["!.env.example"] <= positions[".env.*"] {
		t.Error(".dockerignore must re-include .env.example after excluding .env variants")
	}
}

func assertVolume(t *testing.T, volumes []renderedVolume, volumeType, source, target string, readOnly bool, selinux string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Type == volumeType && volume.Source == source && volume.Target == target && volume.ReadOnly == readOnly && volume.Bind.SELinux == selinux {
			return
		}
	}
	t.Errorf("volume %s:%s -> %s readOnly=%t selinux=%q not found in %#v", volumeType, source, target, readOnly, selinux, volumes)
}
