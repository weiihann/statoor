.PHONY: build test lint fmt clean

build:
	go build -o bin/statoor ./cmd/statoor

test:
	go test -race ./...

lint:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -rf bin/
