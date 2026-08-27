GO ?= go
DIST ?= dist

.PHONY: test test-race test-real-vllm vet build build-linux-amd64 build-e2e-linux-amd64 container-smoke fake-vllm loadgen clean

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-real-vllm:

ifeq ($(filter smoke priority,$(LLMGW_E2E_MODE)),)
	@printf '%s\n' 'LLMGW_E2E_MODE must be smoke or priority' >&2
	@exit 2
else
	$(GO) test -count=1 -v -timeout 10m ./tests/e2e
endif

vet:
	$(GO) vet ./...

build:
	$(GO) build ./cmd/...

build-linux-amd64:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o $(DIST)/gateway-linux-amd64 ./cmd/gateway
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o $(DIST)/fake-vllm-linux-amd64 ./cmd/fake-vllm
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o $(DIST)/loadgen-linux-amd64 ./cmd/loadgen

build-e2e-linux-amd64:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) test -c -o $(DIST)/llmgw-e2e-linux-amd64 ./tests/e2e

container-smoke:
	./scripts/container-smoke.sh

fake-vllm:
	$(GO) run ./cmd/fake-vllm

loadgen:
	$(GO) run ./cmd/loadgen $(ARGS)

clean:
	rm -f $(DIST)/gateway-linux-amd64 $(DIST)/fake-vllm-linux-amd64 $(DIST)/loadgen-linux-amd64 $(DIST)/llmgw-e2e-linux-amd64
