.PHONY: build test run clean fmt lint help

# Variables
BINARY_NAME=kafka-mqtt-bridge
BINARY_PATH=./bin/$(BINARY_NAME)
CMD_PATH=./cmd/bridge
GO=go
GOFLAGS=-v

help:
	@echo "Available targets:"
	@echo "  build       - Build the application"
	@echo "  run         - Run the application"
	@echo "  test        - Run tests"
	@echo "  clean       - Clean build artifacts"
	@echo "  fmt         - Format code"
	@echo "  lint        - Run linter"
	@echo "  deps        - Download and verify dependencies"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run  - Run Docker container"

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

docker-build:
	@echo "Building Docker image..."
	@docker build -t $(BINARY_NAME):latest .
	@echo "Docker image built: $(BINARY_NAME):latest"

docker-run:
	@echo "Running Docker container..."
	@docker run -d \
		--name $(BINARY_NAME) \
		-v $(PWD)/config:/app/config \
		$(BINARY_NAME):latest
	@echo "Container running: $(BINARY_NAME)"

docker-stop:
	@echo "Stopping Docker container..."
	@docker stop $(BINARY_NAME)
	@docker rm $(BINARY_NAME)
	@echo "Container stopped"