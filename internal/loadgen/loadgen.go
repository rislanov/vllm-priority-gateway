package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rislanov/vllm-priority-gateway/internal/domain"
)

type Config struct {
	URL         string
	Key         string
	Keys        map[domain.PriorityClass]string
	Mix         map[domain.PriorityClass]int
	Model       string
	Parallelism int
	Requests    int
	PromptSize  int
	MaxTokens   int
	Stream      bool
	Seed        uint64
	HTTPClient  *http.Client
}

type Result struct {
	Total             int             `json:"total"`
	Successes         int             `json:"successes"`
	Overloaded        int             `json:"overloaded"`
	ServerErrors      int             `json:"serverErrors"`
	Failures          int             `json:"failures"`
	TransportFailures int             `json:"transportFailures"`
	TTFT              DurationSummary `json:"ttft"`
	Latency           DurationSummary `json:"latency"`
}

var priorityOrder = []domain.PriorityClass{
	domain.PriorityCritical, domain.PriorityHigh, domain.PriorityNormal, domain.PriorityBackground,
}

func (c Config) Validate() error {
	parsed, err := url.Parse(strings.TrimSpace(c.URL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("gateway URL must be an absolute http(s) URL")
	}
	if c.Requests <= 0 {
		return errors.New("request count must be positive")
	}
	if c.Parallelism <= 0 {
		return errors.New("parallelism must be positive")
	}
	if strings.TrimSpace(c.Model) == "" {
		return errors.New("model is required")
	}
	if c.PromptSize < 0 || c.MaxTokens < 0 {
		return errors.New("prompt size and max tokens cannot be negative")
	}
	mixWeight := 0
	for class, weight := range c.Mix {
		if !class.Valid() {
			return fmt.Errorf("invalid traffic class %q", class)
		}
		if weight < 0 {
			return fmt.Errorf("traffic weight for %s cannot be negative", class)
		}
		if weight > 0 && strings.TrimSpace(c.Keys[class]) == "" {
			return fmt.Errorf("traffic class %s has non-zero weight but no mapped key", class)
		}
		mixWeight += weight
	}
	if mixWeight == 0 && strings.TrimSpace(c.Key) == "" {
		return errors.New("one API key or a non-zero class mix is required")
	}
	return nil
}

type identity struct {
	class domain.PriorityClass
	key   string
}

type observation struct {
	status  int
	ttft    time.Duration
	latency time.Duration
	err     error
}

func Run(ctx context.Context, config Config) (Result, error) {
	if config.PromptSize == 0 {
		config.PromptSize = 64
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = 16
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if err := config.Validate(); err != nil {
		return Result{}, err
	}
	identities := identitySequence(config)
	jobs := make(chan identity)
	observations := make(chan observation, config.Requests)
	workers := min(config.Parallelism, config.Requests)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for identity := range jobs {
				observations <- execute(ctx, config, identity)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, identity := range identities {
			select {
			case jobs <- identity:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		group.Wait()
		close(observations)
	}()

	var result Result
	var ttft, latency []time.Duration
	for observation := range observations {
		result.Total++
		if observation.err != nil {
			result.TransportFailures++
			continue
		}
		switch {
		case observation.status >= 200 && observation.status < 300:
			result.Successes++
		case observation.status == http.StatusTooManyRequests:
			result.Overloaded++
		case observation.status >= 500:
			result.ServerErrors++
		default:
			result.Failures++
		}
		if observation.ttft > 0 {
			ttft = append(ttft, observation.ttft)
		}
		latency = append(latency, observation.latency)
	}
	result.TTFT = Summarize(ttft)
	result.Latency = Summarize(latency)
	if result.TransportFailures > 0 {
		return result, fmt.Errorf("%d request(s) failed at the transport layer", result.TransportFailures)
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	return result, nil
}

func identitySequence(config Config) []identity {
	weighted := make([]identity, 0)
	for _, class := range priorityOrder {
		for range config.Mix[class] {
			weighted = append(weighted, identity{class: class, key: config.Keys[class]})
		}
	}
	if len(weighted) == 0 {
		weighted = append(weighted, identity{key: config.Key})
	}
	sequence := make([]identity, config.Requests)
	for index := range sequence {
		sequence[index] = weighted[index%len(weighted)]
	}
	random := rand.New(rand.NewPCG(config.Seed, config.Seed^0x9e3779b97f4a7c15))
	random.Shuffle(len(sequence), func(i, j int) { sequence[i], sequence[j] = sequence[j], sequence[i] })
	return sequence
}

func execute(ctx context.Context, config Config, identity identity) observation {
	payload, err := json.Marshal(map[string]any{
		"model": config.Model, "prompt": strings.Repeat("x", config.PromptSize),
		"max_tokens": config.MaxTokens, "stream": config.Stream,
	})
	if err != nil {
		return observation{err: err}
	}
	endpoint := strings.TrimRight(config.URL, "/") + "/v1/completions"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return observation{err: err}
	}
	request.Header.Set("Authorization", "Bearer "+identity.key)
	request.Header.Set("Content-Type", "application/json")
	started := time.Now()
	response, err := config.HTTPClient.Do(request)
	if err != nil {
		return observation{err: err}
	}
	defer response.Body.Close()
	buffer := make([]byte, 32<<10)
	firstByte := time.Duration(0)
	for {
		count, readErr := response.Body.Read(buffer)
		if count > 0 && firstByte == 0 {
			firstByte = time.Since(started)
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return observation{err: readErr}
			}
			break
		}
	}
	return observation{status: response.StatusCode, ttft: firstByte, latency: time.Since(started)}
}
