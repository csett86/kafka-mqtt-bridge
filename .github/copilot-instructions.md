# Copilot Instructions for Kafka-MQTT Bridge

This document provides context and guidelines for GitHub Copilot when working with this repository.

## Project Overview

This is a Go application that bridges messages between Apache Kafka and MQTT brokers. It provides bidirectional message bridging, allowing messages to flow from Kafka to MQTT and vice versa.

## Technology Stack

- **Language**: Go 1.21+
- **Kafka Client**: `github.com/segmentio/kafka-go`
- **MQTT Client**: `github.com/eclipse/paho.mqtt.golang`
- **Logging**: `go.uber.org/zap` (structured logging)
- **Configuration**: YAML with `gopkg.in/yaml.v3`

## Project Structure

```
kafka-mqtt-bridge/
├── cmd/bridge/         # Application entry point
├── internal/           # Private application code
│   ├── bridge/         # Core bridge logic
│   ├── kafka/          # Kafka client wrapper
│   └── mqtt/           # MQTT client wrapper
├── pkg/config/         # Configuration management (public)
├── test/integration/   # Integration tests
└── config/             # Configuration files
```

## Coding Conventions

### Go Best Practices
- Follow standard Go project layout conventions
- Use `internal/` for private packages, `pkg/` for public packages
- Use `cmd/` for application entry points
- Prefer explicit error handling with wrapped errors using `fmt.Errorf("context: %w", err)`
- Use structured logging with `zap.Logger`

### Naming Conventions
- Use camelCase for private variables and functions
- Use PascalCase for exported types, functions, and variables
- Use descriptive names that indicate purpose

### Error Handling
- Always wrap errors with context using `%w` verb
- Check and handle all errors explicitly
- Log errors with structured fields using `zap.Error(err)`

### Logging
- Use `zap.Logger` for all logging
- Include relevant context using structured fields: `zap.String()`, `zap.Int()`, `zap.Error()`, etc.
- Use appropriate log levels: Debug, Info, Warn, Error, Fatal

## Build and Development

### Commands
```bash
make build              # Build the binary
make test               # Run unit tests
make fmt                # Format code
make lint               # Run golangci-lint
make integration-test   # Run integration tests (requires Docker)
make integration-up     # Start integration test infrastructure
make integration-down   # Stop integration test infrastructure
make integration-test-only # Run tests (infrastructure must be running)
```

### Testing
- Unit tests are in `*_test.go` files alongside source code
- Integration tests are in `test/integration/` and require Docker infrastructure
- Run `make integration-up` before `make integration-test-only` for integration testing
- Use `-race` flag for race condition detection

### Dependencies
- Use `go mod` for dependency management
- Run `make deps` to download and verify dependencies

## Configuration

Configuration is loaded from YAML files with these main sections:

- `kafka`: Kafka broker settings and topics
- `mqtt`: MQTT broker settings and topics
- `bridge`: Bridge-specific settings (name, log level, buffer size)

MQTT supports TLS configuration for secure connections.

## Key Components

### Bridge (`internal/bridge/`)
- Main orchestration logic for bidirectional message bridging
- Manages Kafka→MQTT and MQTT→Kafka message flow
- Handles graceful shutdown

### Kafka Client (`internal/kafka/`)
- Wraps kafka-go library
- Supports connection recovery and retry logic
- Provides manual offset commit for reliable delivery

### MQTT Client (`internal/mqtt/`)
- Wraps paho.mqtt.golang library
- Supports automatic reconnection
- Restores subscriptions after reconnection
- Supports TLS with client certificates

## Testing Approach

- Write table-driven tests where appropriate
- Use subtests with `t.Run()` for related test cases
- Mock external dependencies (Kafka, MQTT) when possible
- Integration tests verify end-to-end functionality with real brokers

## Important Considerations

- Ensure at-least-once delivery semantics
- Handle connection failures gracefully with retry logic
- Support graceful shutdown via signal handling (SIGINT, SIGTERM)
- Maintain backward compatibility with existing configuration format
