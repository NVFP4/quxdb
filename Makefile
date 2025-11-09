DEBOUNCE_VAL = 1s

# build
BUILD_ENV =
BUILD_TAGS =
BUILD_LDFLAGS = -s -w

# monitoring
MONITORING_DIR = ./infra/monitoring
MONITORING_COMPOSE = $(MONITORING_DIR)/compose.yaml
PROMETHEUS_URL = http://localhost:9090
GRAFANA_URL = http://localhost:3000


.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Install dependencies
	@go install github.com/bokwoon95/wgo@latest

.PHONY: dev
dev: ## Run the server with hot reloading
	exec wgo run -debounce $(DEBOUNCE_VAL) ./cmd/server/

.PHONY: run
run: ## Run the server without hot reloading
	$(BUILD_ENV) go run ./cmd/server/

.PHONY: build
build: ## Build the server
	@mkdir -p .out
	$(BUILD_ENV) go build -ldflags "$(BUILD_LDFLAGS)" -tags "$(BUILD_TAGS)" -o .out/server ./cmd/server/

.PHONY: test
test: ## Run tests
	$(BUILD_ENV) go test -v ./...

.PHONY: bench
bench: ## Run benchmark tests
	$(BUILD_ENV) go test -bench=. ./...

.PHONY: start-prom
start-prom: ## Run monitoring services
	# make sure this is docker & podman friendly
	@docker compose -f $(MONITORING_COMPOSE) up -d

	@echo "----------"
	@echo "Waiting for Prometheus and Grafana to become ready..."
	@curl -fsS --retry 60 --retry-all-errors $(PROMETHEUS_URL)/-/ready >/dev/null 2>&1 || { \
		echo "Prometheus did not become ready in $(WAIT_TIMEOUT)s"; \
		exit 1; \
	}
	@curl -fsS --retry 60 --retry-all-errors $(GRAFANA_URL)/api/health >/dev/null 2>&1 || { \
		echo "Grafana did not become ready in $(WAIT_TIMEOUT)s"; \
		exit 1; \
	}
	@echo "----------"
	@echo "Prometheus: $(PROMETHEUS_URL)"
	@echo "Grafana:    $(GRAFANA_URL)"

.PHONY: stop-prom
stop-prom: ## Stop monitoring services
	# make sure this is docker & podman friendly
	docker compose -f $(MONITORING_COMPOSE) down

.PHONY: reset-prom
reset-prom: ## Reset monitoring services
	# make sure this is docker & podman friendly
	docker compose -f $(MONITORING_COMPOSE) down --remove-orphans --volumes
