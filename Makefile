.PHONY: build test run clean fmt lint help integration-test integration-up integration-down

# Variables
BINARY_NAME=kafka-mqtt-bridge
BINARY_PATH=./bin/$(BINARY_NAME)
CMD_PATH=./cmd/bridge
GO=go
GOFLAGS=-v

help:
	@echo "Available targets:"
	@echo "  build            - Build the application"
	@echo "  run              - Run the application"
	@echo "  test             - Run unit tests"
	@echo "  integration-test - Run integration tests (requires Docker)"
	@echo "  integration-up   - Start integration test infrastructure"
	@echo "  integration-down - Stop integration test infrastructure"
	@echo "  clean            - Clean build artifacts"
	@echo "  fmt              - Format code"
	@echo "  lint             - Run linter"
	@echo "  deps             - Download and verify dependencies"

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	@$(GO) build $(GOFLAGS) -o $(BINARY_PATH) $(CMD_PATH)
	@echo "Build complete: $(BINARY_PATH)"

run: build
	@echo "Running $(BINARY_NAME)..."
	@$(BINARY_PATH) -config config/config.yaml

test:
	@echo "Running tests..."
	@$(GO) test -v -race -coverprofile=coverage.out ./...
	@$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/ dist/ coverage.out coverage.html
	@$(GO) clean -cache -testcache
	@echo "Clean complete"

fmt:
	@echo "Formatting code..."
	@$(GO) fmt ./...
	@echo "Format complete"

lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@golangci-lint run ./...

deps:
	@echo "Downloading dependencies..."
	@$(GO) mod download
	@$(GO) mod verify
	@echo "Dependencies downloaded and verified"

# Integration test targets
integration-up:
	@echo "Starting integration test infrastructure..."
	@docker compose -f docker-compose.integration.yml up -d
	@echo "Waiting for services to be healthy..."
	@timeout=120; \
	elapsed=0; \
	while [ $$elapsed -lt $$timeout ]; do \
		healthy=$$(docker compose -f docker-compose.integration.yml ps --format json | jq -r 'select(.Health == "healthy") | .Name' | wc -l); \
		total=$$(docker compose -f docker-compose.integration.yml ps --format json | jq -r '.Name' | wc -l); \
		echo "Healthy services: $$healthy/$$total"; \
		if [ "$$healthy" -eq "$$total" ] && [ "$$total" -gt 0 ]; then \
			echo "All services are healthy!"; \
			break; \
		fi; \
		sleep 5; \
		elapsed=$$((elapsed + 5)); \
	done; \
	if [ $$elapsed -ge $$timeout ]; then \
		echo "Timeout waiting for services to be healthy"; \
		docker compose -f docker-compose.integration.yml ps; \
		docker compose -f docker-compose.integration.yml logs; \
		exit 1; \
	fi
	@echo "Integration test infrastructure is ready"

integration-down:
	@echo "Stopping integration test infrastructure..."
	@docker compose -f docker-compose.integration.yml down -v
	@echo "Integration test infrastructure stopped"

integration-test: integration-up
	@echo "Running integration tests..."
	@$(GO) test -v -race -timeout 15m ./test/integration/... || (make integration-down && exit 1)
	@make integration-down
	@echo "Integration tests complete"

integration-test-only:
	@echo "Running integration tests (infrastructure must be running)..."
	@$(GO) test -v -race -timeout 5m ./test/integration/...
