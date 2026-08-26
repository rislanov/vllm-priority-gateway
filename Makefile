GO ?= go
DIST ?= dist

.PHONY: test test-race vet build build-linux-amd64 container-smoke fake-vllm loadgen clean

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

build:
	$(GO) build ./cmd/...

build-linux-amd64:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o $(DIST)/gateway-linux-amd64 ./cmd/gateway
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o $(DIST)/fake-vllm-linux-amd64 ./cmd/fake-vllm
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o $(DIST)/loadgen-linux-amd64 ./cmd/loadgen

container-smoke:
	./scripts/container-smoke.sh

fake-vllm:
	$(GO) run ./cmd/fake-vllm

loadgen:
	$(GO) run ./cmd/loadgen $(ARGS)

clean:
	rm -f $(DIST)/gateway-linux-amd64 $(DIST)/fake-vllm-linux-amd64 $(DIST)/loadgen-linux-amd64
