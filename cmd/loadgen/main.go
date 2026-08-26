package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/loadgen"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	url := flags.String("url", "http://127.0.0.1:8080", "gateway base URL")
	key := flags.String("key", "", "single client API key")
	classKeys := flags.String("class-keys", "", "comma-separated class=key mappings")
	mix := flags.String("mix", "", "comma-separated class=weight traffic mix")
	model := flags.String("model", "", "public model name")
	parallelism := flags.Int("parallelism", 8, "concurrent workers")
	requests := flags.Int("requests", 100, "total requests")
	promptSize := flags.Int("prompt-size", 64, "prompt length in bytes")
	maxTokens := flags.Int("max-tokens", 16, "requested completion tokens")
	stream := flags.Bool("stream", false, "request SSE streaming")
	seed := flags.Uint64("seed", 1, "traffic shuffle seed")
	jsonOutput := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	keys, err := parseClassStrings(*classKeys)
	if err != nil {
		return err
	}
	weights, err := parseWeights(*mix)
	if err != nil {
		return err
	}
	result, err := loadgen.Run(context.Background(), loadgen.Config{
		URL: *url, Key: *key, Keys: keys, Mix: weights, Model: *model,
		Parallelism: *parallelism, Requests: *requests, PromptSize: *promptSize,
		MaxTokens: *maxTokens, Stream: *stream, Seed: *seed,
	})
	if *jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(result)
	} else {
		fmt.Printf("requests=%d success=%d overloaded=%d server_errors=%d failures=%d transport=%d\n", result.Total, result.Successes, result.Overloaded, result.ServerErrors, result.Failures, result.TransportFailures)
		fmt.Printf("ttft    p50=%s p95=%s p99=%s\n", result.TTFT.P50, result.TTFT.P95, result.TTFT.P99)
		fmt.Printf("latency p50=%s p95=%s p99=%s\n", result.Latency.P50, result.Latency.P95, result.Latency.P99)
	}
	return err
}

func parseClassStrings(raw string) (map[domain.PriorityClass]string, error) {
	output := make(map[domain.PriorityClass]string)
	if strings.TrimSpace(raw) == "" {
		return output, nil
	}
	for _, pair := range strings.Split(raw, ",") {
		parts := strings.SplitN(pair, "=", 2)
		class := domain.PriorityClass(strings.TrimSpace(parts[0]))
		if len(parts) != 2 || !class.Valid() || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("invalid class mapping %q", pair)
		}
		output[class] = strings.TrimSpace(parts[1])
	}
	return output, nil
}

func parseWeights(raw string) (map[domain.PriorityClass]int, error) {
	stringsByClass, err := parseClassStrings(raw)
	if err != nil {
		return nil, err
	}
	output := make(map[domain.PriorityClass]int, len(stringsByClass))
	for class, value := range stringsByClass {
		weight, parseErr := strconv.Atoi(value)
		if parseErr != nil || weight < 0 {
			return nil, fmt.Errorf("invalid weight for %s", class)
		}
		output[class] = weight
	}
	return output, nil
}
