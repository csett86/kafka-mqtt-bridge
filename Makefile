.PHONY: build test run clean fmt lint help integration-test integration-up integration-down eventhubs-test eventhubs-up eventhubs-down eventhubs-test-only

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
	@echo "  eventhubs-test   - Run Event Hubs Emulator tests (requires Docker)"
	@echo "  eventhubs-up     - Start Event Hubs Emulator infrastructure"
	@echo "  eventhubs-down   - Stop Event Hubs Emulator infrastructure"
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
	@docker compose -f docker-compose.integration.yml up -d --wait --wait-timeout 120 || \
		(echo "Timeout waiting for services to be healthy"; \
		docker compose -f docker-compose.integration.yml ps; \
		docker compose -f docker-compose.integration.yml logs; \
		exit 1)
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
	@$(GO) test -v -race -timeout 15m ./test/integration/...

# Azure Event Hubs Emulator test targets
eventhubs-up:
	@echo "Starting Event Hubs Emulator infrastructure..."
	@docker compose -f docker-compose.eventhubs.yml up -d --wait --wait-timeout 180 || \
		(echo "Timeout waiting for services to be healthy"; \
		docker compose -f docker-compose.eventhubs.yml ps; \
		docker compose -f docker-compose.eventhubs.yml logs; \
		exit 1)
	@echo "Event Hubs Emulator infrastructure is ready"

eventhubs-down:
	@echo "Stopping Event Hubs Emulator infrastructure..."
	@docker compose -f docker-compose.eventhubs.yml down -v
	@echo "Event Hubs Emulator infrastructure stopped"

eventhubs-test: eventhubs-up
	@echo "Running Event Hubs Emulator tests..."
	@$(GO) test -v -race -timeout 15m -run TestEventHubs ./test/integration/... || (make eventhubs-down && exit 1)
	@make eventhubs-down
	@echo "Event Hubs Emulator tests complete"

eventhubs-test-only:
	@echo "Running Event Hubs Emulator tests (infrastructure must be running)..."
	@$(GO) test -v -race -timeout 15m -run TestEventHubs ./test/integration/...
