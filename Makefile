.PHONY: all build test test-race bench lint tidy clean

all: lint test

build:
	go build ./...

# Integration tests against a real Postgres via testcontainers-go — requires
# a running Docker daemon.
test:
	go test ./...

test-race:
	go test -race ./...

bench:
	go test -run=^$$ -bench=. -benchmem ./...

lint:
	test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...

tidy:
	go mod tidy

clean:
	go clean ./...
