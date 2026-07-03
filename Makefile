# Chronicle — Durable Streams on Redis 8
#
# Common entry points:
#   make build         compile the server binary into bin/
#   make run           start a local server (expects redis; `make redis-up` first)
#   make test          unit + integration tests (integration needs redis)
#   make conformance   full protocol conformance suite against a live server
#   make lint fmt      static analysis / formatting
#   make redis-up      start Redis 8 via docker compose

GO        ?= go
BINARY    := bin/chronicle
REDIS_URL ?= redis://localhost:6379

.PHONY: all build run test test-unit test-integration conformance lint fmt tidy redis-up redis-down clean spec-check backlog-check backlog-autofix

all: build

build:
	$(GO) build -o $(BINARY) ./cmd/chronicle

run: build
	$(BINARY) --listen :4437 --redis-url $(REDIS_URL)

test:
	$(GO) test -race -count=1 ./...

test-unit:
	$(GO) test -race -count=1 -short ./...

# Integration tests hit a live Redis (REDIS_URL) and are skipped under -short.
test-integration: redis-up
	$(GO) test -race -count=1 ./store/redis/ ./webhook/

conformance:
	./scripts/conformance.sh

# Filtered conformance run, e.g.: make conformance-filter FILTER="Idempotent Producer"
conformance-filter:
	./scripts/conformance.sh -t "$(FILTER)"

redis-flush:
	docker compose exec -T redis redis-cli -n 15 flushdb

# golangci-lint pinned to match the CI action (issue #97). It runs via `go run`
# under GOTOOLCHAIN set to go.mod's Go version, so the linter is BUILT with a Go
# toolchain >= the module target. Otherwise golangci-lint refuses to run ("the Go
# language version used to build golangci-lint is lower than the targeted Go
# version") — which is why a system/`latest` golangci-lint built with an older Go
# could not lint this go1.26 module locally.
GOLANGCI_LINT_VERSION ?= v2.12.2
GO_TOOLCHAIN := $(shell awk '/^go /{print "go" $$2; exit}' go.mod)

lint:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

# Verify SPEC_VERSION.md is consistent with the conformance pin and flag upstream
# spec drift (issue #98). CI runs this in the `spec-version` job.
spec-check:
	./scripts/spec-version-check.sh

# Verify docs/BACKLOG.md still mirrors the set of open issues (drift guard;
# provenance contract stated at the top of the doc). CI runs this in the
# `backlog-drift` job.
backlog-check:
	./scripts/backlog-check.sh

# Remove backlog rows for issues that are no longer open and renumber ranks
# (the mechanical half of drift repair; run by the backlog-autofix workflow).
backlog-autofix:
	./scripts/backlog-autofix.sh

fmt:
	gofumpt -l -w .
	$(GO) mod tidy

tidy:
	$(GO) mod tidy

redis-up:
	docker compose up -d --wait redis

redis-down:
	docker compose down

clean:
	rm -rf bin/
