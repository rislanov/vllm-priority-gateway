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
	"sort"
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
	Total             int                                  `json:"total"`
	Successes         int                                  `json:"successes"`
	Overloaded        int                                  `json:"overloaded"`
	ServerErrors      int                                  `json:"serverErrors"`
	Failures          int                                  `json:"failures"`
	TransportFailures int                                  `json:"transportFailures"`
	TTFT              DurationSummary                      `json:"ttft"`
	Latency           DurationSummary                      `json:"latency"`
	ByClass           map[domain.PriorityClass]ClassResult `json:"byClass,omitempty"`
}

type ClassResult struct {
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
	class   domain.PriorityClass
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
	accumulator := resultAccumulator{
		summary:      Result{ByClass: make(map[domain.PriorityClass]ClassResult)},
		classTTFT:    make(map[domain.PriorityClass][]time.Duration),
		classLatency: make(map[domain.PriorityClass][]time.Duration),
	}
	for observation := range observe(ctx, config, identities) {
		accumulator.add(observation)
	}
	return accumulator.finish(ctx)
}

func observe(ctx context.Context, config Config, identities []identity) <-chan observation {
	jobs := make(chan identity)
	observations := make(chan observation, config.Requests)
	workers := min(config.Parallelism, config.Requests)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			for identity := range jobs {
				observations <- execute(ctx, config, identity)
			}
		})
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
	return observations
}

type resultAccumulator struct {
	summary      Result
	ttft         []time.Duration
	latency      []time.Duration
	classTTFT    map[domain.PriorityClass][]time.Duration
	classLatency map[domain.PriorityClass][]time.Duration
}

func (accumulator *resultAccumulator) add(observation observation) {
	accumulator.summary.Total++
	classResult := accumulator.summary.ByClass[observation.class]
	if observation.class.Valid() {
		classResult.Total++
	}
	if observation.err != nil {
		accumulator.summary.TransportFailures++
		if observation.class.Valid() {
			classResult.TransportFailures++
			accumulator.summary.ByClass[observation.class] = classResult
		}
		return
	}
	switch {
	case observation.status >= 200 && observation.status < 300:
		accumulator.summary.Successes++
		if observation.class.Valid() {
			classResult.Successes++
		}
		if observation.ttft > 0 {
			accumulator.ttft = append(accumulator.ttft, observation.ttft)
			if observation.class.Valid() {
				accumulator.classTTFT[observation.class] = append(accumulator.classTTFT[observation.class], observation.ttft)
			}
		}
		accumulator.latency = append(accumulator.latency, observation.latency)
		if observation.class.Valid() {
			accumulator.classLatency[observation.class] = append(accumulator.classLatency[observation.class], observation.latency)
		}
	case observation.status == http.StatusTooManyRequests:
		accumulator.summary.Overloaded++
		classResult.Overloaded++
	case observation.status >= 500:
		accumulator.summary.ServerErrors++
		classResult.ServerErrors++
	default:
		accumulator.summary.Failures++
		classResult.Failures++
	}
	if observation.class.Valid() {
		accumulator.summary.ByClass[observation.class] = classResult
	}
}

func (accumulator *resultAccumulator) finish(ctx context.Context) (Result, error) {
	accumulator.summary.TTFT = Summarize(accumulator.ttft)
	accumulator.summary.Latency = Summarize(accumulator.latency)
	for class, classResult := range accumulator.summary.ByClass {
		classResult.TTFT = Summarize(accumulator.classTTFT[class])
		classResult.Latency = Summarize(accumulator.classLatency[class])
		accumulator.summary.ByClass[class] = classResult
	}
	if len(accumulator.summary.ByClass) == 0 {
		accumulator.summary.ByClass = nil
	}
	if accumulator.summary.TransportFailures > 0 {
		return accumulator.summary, fmt.Errorf("%d request(s) failed at the transport layer", accumulator.summary.TransportFailures)
	}
	if ctx.Err() != nil {
		return accumulator.summary, ctx.Err()
	}
	return accumulator.summary, nil
}

func identitySequence(config Config) []identity {
	random := rand.New(rand.NewPCG(config.Seed, config.Seed^0x9e3779b97f4a7c15))
	totalWeight := 0
	for _, class := range priorityOrder {
		totalWeight += config.Mix[class]
	}
	if totalWeight == 0 {
		sequence := make([]identity, config.Requests)
		for index := range sequence {
			sequence[index] = identity{key: config.Key}
		}
		return sequence
	}
	type remainder struct {
		class domain.PriorityClass
		value int
	}
	counts := make(map[domain.PriorityClass]int, len(priorityOrder))
	remainders := make([]remainder, 0, len(priorityOrder))
	assigned := 0
	for _, class := range priorityOrder {
		product := config.Requests * config.Mix[class]
		counts[class] = product / totalWeight
		assigned += counts[class]
		if config.Mix[class] > 0 {
			remainders = append(remainders, remainder{class: class, value: product % totalWeight})
		}
	}
	random.Shuffle(len(remainders), func(i, j int) { remainders[i], remainders[j] = remainders[j], remainders[i] })
	sort.SliceStable(remainders, func(i, j int) bool { return remainders[i].value > remainders[j].value })
	for index := 0; index < config.Requests-assigned; index++ {
		counts[remainders[index].class]++
	}
	sequence := make([]identity, 0, config.Requests)
	for _, class := range priorityOrder {
		for range counts[class] {
			sequence = append(sequence, identity{class: class, key: config.Keys[class]})
		}
	}
	random.Shuffle(len(sequence), func(i, j int) { sequence[i], sequence[j] = sequence[j], sequence[i] })
	return sequence
}

func execute(ctx context.Context, config Config, identity identity) observation {
	payload, err := json.Marshal(map[string]any{
		"model": config.Model, "prompt": strings.Repeat("x", config.PromptSize),
		"max_tokens": config.MaxTokens, "stream": config.Stream,
	})
	if err != nil {
		return observation{class: identity.class, err: err}
	}
	endpoint := strings.TrimRight(config.URL, "/") + "/v1/completions"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return observation{class: identity.class, err: err}
	}
	request.Header.Set("Authorization", "Bearer "+identity.key)
	request.Header.Set("Content-Type", "application/json")
	started := time.Now()
	response, err := config.HTTPClient.Do(request)
	if err != nil {
		return observation{class: identity.class, err: err}
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
				return observation{class: identity.class, err: readErr}
			}
			break
		}
	}
	return observation{class: identity.class, status: response.StatusCode, ttft: firstByte, latency: time.Since(started)}
}
