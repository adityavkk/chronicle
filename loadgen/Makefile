.PHONY: build test lint fmt smoke bench-segments clean

build:
	go build -o bin/dsload ./cmd/dsload

test:
	go test -race ./...

lint:
	golangci-lint run ./...

fmt:
	gofumpt -l -w .
	go mod tidy

# Requires a Durable Streams server on :4437.
smoke: build
	bin/dsload run -scenario scenarios/smoke.yaml -label smoke -out results

bench-segments: build
	./scripts/bench-segments-local.sh

clean:
	rm -rf bin results
