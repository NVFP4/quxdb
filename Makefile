DEBOUNCE_VAL = 1s

BUILD_ENV =
BUILD_TAGS =
BUILD_LDFLAGS = -s -w

.PHONY: help setup dev run build test bench

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install dependencies
	@go install github.com/bokwoon95/wgo@latest

dev: ## Run the server with hot reloading
	exec wgo run -debounce $(DEBOUNCE_VAL) ./cmd/server/

run: ## Run the server without hot reloading
	$(BUILD_ENV) go run ./cmd/server/

build: ## Build the server
	@mkdir -p .out
	$(BUILD_ENV) go build -ldflags "$(BUILD_LDFLAGS)" -tags "$(BUILD_TAGS)" -o .out/server ./cmd/server/

test: ## Run tests
	$(BUILD_ENV) go test -v ./...

bench: ## Run benchmark tests
	$(BUILD_ENV) go test -bench=. ./...
