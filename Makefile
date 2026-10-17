.PHONY: all build test eval fmt vet install clean

BINARY := a2a-mailbox
PREFIX ?= /opt/homebrew/bin

all: fmt vet test build

build:
	go build -trimpath -o $(BINARY) .

test:
	go test -v -count=1 -race ./...

eval:
	go test -v -count=1 -race ./evals/...

fmt:
	gofmt -s -w .

vet:
	go vet ./...

install: build
	cp $(BINARY) $(PREFIX)/$(BINARY)

clean:
	rm -f $(BINARY)
