# Kafka-MQTT Bridge

A Go application that bridges messages between Apache Kafka and MQTT brokers bidirectionally. This service reads messages from a single Kafka topic and publishes them to a single MQTT topic, and vice versa, enabling seamless integration between these two popular messaging systems. It supports MQTT QoS and only commits reads to Kafka once successfully delivered to MQTT.

## Features

- **Bidirectional** message bridging between Kafka and MQTT
- Configurable Kafka and MQTT connection settings in a single config file
- Environment variable configuration for containerized deployments
- **Avro** serialization/deserialization with Azure Event Hubs Schema Registry
- Static **binary releases for windows**, linux and mac without any additional runtime dependencies
- Implemented in Go with **very low memory footprint** (<10MB)
- Graceful shutdown handling
- Comprehensive logging with Zap
- Production-ready code structure following Go best practices

## Prerequisites

- Apache Kafka 2.8+
- MQTT Broker (e.g., Mosquitto, HiveMQ)
- Go 1.24 or higher (only for building)
- Docker and Docker Compose (optional, for integration tests)

## Known Limitations

- **Kafka topics must exist before starting the bridge**: When bridging from Kafka to MQTT, the source Kafka topic must exist before the bridge is started. If the topic does not exist, the Kafka→MQTT bridge will fail startup with an error. Note that this limitation does not apply to MQTT→Kafka bridging, where the destination Kafka topic is auto-created if it does not exist.

## Installation

### Prebuild binaries

Download the prebuilt binaries from the latest release:
https://github.com/csett86/kafka-mqtt-bridge/releases/latest

### Docker container

Published on Github Container Registry:
```
docker pull ghcr.io/csett86/kafka-mqtt-bridge:latest
```

### Build from source

1. Clone the Repository

```bash
git clone https://github.com/csett86/kafka-mqtt-bridge.git
cd kafka-mqtt-bridge
```

2. Build from Source

```bash
make build
```

This will create a binary in the `bin/` directory.

## Configuration

The bridge supports two configuration methods:
1. **YAML configuration file** - Traditional configuration with optional environment variable overrides
2. **Environment variables only** - Recommended for containerized deployments (Docker, Kubernetes)

### YAML Configuration File

Create a `config/config.yaml` file or copy from the example:

```bash
cp config/config.yaml config/local.yaml
```

Edit the configuration file with your Kafka and MQTT settings:

```yaml
kafka:
  broker: "localhost:9092"
  group_id: "kafka-mqtt-bridge"

mqtt:
  broker: "localhost"
  port: 1883
  username: ""
  password: ""
  client_id: "kafka-mqtt-bridge"
  qos: 1  # QoS level (0, 1, or 2)

bridge:
  name: "kafka-mqtt-bridge"
  log_level: "info"
  buffer_size: 100
  mqtt_to_kafka:
    source_topic: "mqtt/events"
    dest_topic: "mqtt-to-kafka"
  kafka_to_mqtt:
    source_topic: "events"
    dest_topic: "kafka/events"
```

### Environment Variable Configuration

All configuration options can be set via environment variables with the `BRIDGE_` prefix. This is the recommended approach for containerized deployments following the [12-factor app](https://12factor.net/config) methodology.

Environment variables use underscore-separated names that match the YAML structure:

| Environment Variable | YAML Equivalent | Description |
|---------------------|-----------------|-------------|
| `BRIDGE_KAFKA_BROKER` | `kafka.broker` | Kafka broker address |
| `BRIDGE_KAFKA_GROUP_ID` | `kafka.group_id` | Kafka consumer group ID |
| `BRIDGE_KAFKA_SASL_ENABLED` | `kafka.sasl.enabled` | Enable SASL authentication |
| `BRIDGE_KAFKA_SASL_MECHANISM` | `kafka.sasl.mechanism` | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |
| `BRIDGE_KAFKA_SASL_USERNAME` | `kafka.sasl.username` | SASL username |
| `BRIDGE_KAFKA_SASL_PASSWORD` | `kafka.sasl.password` | SASL password |
| `BRIDGE_KAFKA_TLS_ENABLED` | `kafka.tls.enabled` | Enable TLS connection |
| `BRIDGE_KAFKA_TLS_CA_FILE` | `kafka.tls.ca_file` | Path to CA certificate |
| `BRIDGE_KAFKA_TLS_CERT_FILE` | `kafka.tls.cert_file` | Path to client certificate |
| `BRIDGE_KAFKA_TLS_KEY_FILE` | `kafka.tls.key_file` | Path to client key |
| `BRIDGE_KAFKA_TLS_INSECURE_SKIP_VERIFY` | `kafka.tls.insecure_skip_verify` | Skip TLS verification |
| `BRIDGE_MQTT_BROKER` | `mqtt.broker` | MQTT broker hostname |
| `BRIDGE_MQTT_PORT` | `mqtt.port` | MQTT broker port |
| `BRIDGE_MQTT_USERNAME` | `mqtt.username` | MQTT username |
| `BRIDGE_MQTT_PASSWORD` | `mqtt.password` | MQTT password |
| `BRIDGE_MQTT_CLIENT_ID` | `mqtt.client_id` | MQTT client ID |
| `BRIDGE_MQTT_QOS` | `mqtt.qos` | MQTT QoS level (0, 1, or 2) |
| `BRIDGE_MQTT_CLEAN_SESSION` | `mqtt.clean_session` | MQTT clean session flag |
| `BRIDGE_MQTT_TLS_ENABLED` | `mqtt.tls.enabled` | Enable MQTT TLS |
| `BRIDGE_MQTT_TLS_CA_FILE` | `mqtt.tls.ca_file` | Path to CA certificate |
| `BRIDGE_MQTT_TLS_CERT_FILE` | `mqtt.tls.cert_file` | Path to client certificate |
| `BRIDGE_MQTT_TLS_KEY_FILE` | `mqtt.tls.key_file` | Path to client key |
| `BRIDGE_MQTT_TLS_INSECURE_SKIP_VERIFY` | `mqtt.tls.insecure_skip_verify` | Skip TLS verification |
| `BRIDGE_BRIDGE_NAME` | `bridge.name` | Bridge instance name |
| `BRIDGE_BRIDGE_LOG_LEVEL` | `bridge.log_level` | Log level (debug, info, warn, error) |
| `BRIDGE_BRIDGE_BUFFER_SIZE` | `bridge.buffer_size` | Message buffer size |
| `BRIDGE_BRIDGE_MQTT_TO_KAFKA_SOURCE_TOPIC` | `bridge.mqtt_to_kafka.source_topic` | MQTT source topic |
| `BRIDGE_BRIDGE_MQTT_TO_KAFKA_DEST_TOPIC` | `bridge.mqtt_to_kafka.dest_topic` | Kafka destination topic |
| `BRIDGE_BRIDGE_KAFKA_TO_MQTT_SOURCE_TOPIC` | `bridge.kafka_to_mqtt.source_topic` | Kafka source topic |
| `BRIDGE_BRIDGE_KAFKA_TO_MQTT_DEST_TOPIC` | `bridge.kafka_to_mqtt.dest_topic` | MQTT destination topic |
| `BRIDGE_SCHEMA_REGISTRY_FULLY_QUALIFIED_NAMESPACE` | `schema_registry.fully_qualified_namespace` | Azure Schema Registry namespace |
| `BRIDGE_SCHEMA_REGISTRY_CACHE_ENABLED` | `schema_registry.cache_enabled` | Enable schema caching |
| `BRIDGE_SCHEMA_REGISTRY_TENANT_ID` | `schema_registry.tenant_id` | Azure tenant ID |
| `BRIDGE_SCHEMA_REGISTRY_CLIENT_ID` | `schema_registry.client_id` | Azure client ID |
| `BRIDGE_SCHEMA_REGISTRY_CLIENT_SECRET` | `schema_registry.client_secret` | Azure client secret |

#### Example: Docker Compose

```yaml
services:
  kafka-mqtt-bridge:
    image: kafka-mqtt-bridge:latest
    environment:
      - BRIDGE_KAFKA_BROKER=kafka:9092
      - BRIDGE_KAFKA_GROUP_ID=my-bridge
      - BRIDGE_MQTT_BROKER=mqtt
      - BRIDGE_MQTT_PORT=1883
      - BRIDGE_BRIDGE_NAME=my-bridge
      - BRIDGE_BRIDGE_LOG_LEVEL=info
      - BRIDGE_BRIDGE_MQTT_TO_KAFKA_SOURCE_TOPIC=mqtt/events
      - BRIDGE_BRIDGE_MQTT_TO_KAFKA_DEST_TOPIC=kafka-events
      - BRIDGE_BRIDGE_KAFKA_TO_MQTT_SOURCE_TOPIC=kafka-events
      - BRIDGE_BRIDGE_KAFKA_TO_MQTT_DEST_TOPIC=mqtt/events
```

#### Example: Kubernetes ConfigMap/Secret

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-mqtt-bridge-config
data:
  BRIDGE_KAFKA_BROKER: "kafka:9092"
  BRIDGE_MQTT_BROKER: "mqtt"
  BRIDGE_MQTT_PORT: "1883"
  BRIDGE_BRIDGE_NAME: "my-bridge"
  BRIDGE_BRIDGE_LOG_LEVEL: "info"
---
apiVersion: v1
kind: Secret
metadata:
  name: kafka-mqtt-bridge-secrets
stringData:
  BRIDGE_KAFKA_SASL_PASSWORD: "your-password"
  BRIDGE_MQTT_PASSWORD: "your-mqtt-password"
```

### Environment Variable Overrides

When using a YAML configuration file, environment variables can still override any YAML values. This allows you to keep non-sensitive configuration in a file while injecting secrets via environment variables:

```bash
# Start with YAML file but override sensitive values
BRIDGE_KAFKA_SASL_PASSWORD=secret ./kafka-mqtt-bridge -config config/config.yaml
```

### Avro Schema Registry Configuration

The bridge supports optional Avro serialization/deserialization using Azure Event Hubs Schema Registry. Avro can be enabled independently for each bridging direction:

- **MQTT → Kafka**: Add `avro` config to `mqtt_to_kafka` to serialize JSON messages to Avro format before sending to Kafka
- **Kafka → MQTT**: Add `avro` config to `kafka_to_mqtt` to deserialize Avro messages from Kafka to JSON before publishing to MQTT

```yaml
bridge:
  mqtt_to_kafka:
    source_topic: "mqtt/events"
    dest_topic: "mqtt-to-kafka"
    # Optional: Enable Avro serialization
    avro:
      schema_group: "your-schema-group"
      schema_name: "your-schema-name"
  
  kafka_to_mqtt:
    source_topic: "events"
    dest_topic: "kafka/events"
    # Optional: Enable Avro deserialization
    avro:
      schema_group: "your-schema-group"
      schema_name: "your-schema-name"

# Required when avro is configured in any bridge direction
schema_registry:
  fully_qualified_namespace: "your-namespace.servicebus.windows.net"
  cache_enabled: true  # optional, default: true
  # Azure AD authentication (optional - uses DefaultAzureCredential if not provided)
  # tenant_id: "your-tenant-id"
  # client_id: "your-client-id"
  # client_secret: "your-client-secret"
```

**Authentication**: The Schema Registry client uses Azure Identity for authentication. If `tenant_id`, `client_id`, and `client_secret` are provided, it uses Client Secret credentials. Otherwise, it falls back to `DefaultAzureCredential`, which supports managed identities, environment variables, Azure CLI, and other Azure authentication methods.

## Usage

### Running with Configuration File

```bash
./bin/kafka-mqtt-bridge -config config/config.yaml
```

### Running with Environment Variables Only

```bash
# No -config flag needed - configuration loaded from environment variables
export BRIDGE_KAFKA_BROKER=kafka:9092
export BRIDGE_MQTT_BROKER=mqtt
export BRIDGE_BRIDGE_MQTT_TO_KAFKA_SOURCE_TOPIC=mqtt/events
export BRIDGE_BRIDGE_MQTT_TO_KAFKA_DEST_TOPIC=kafka-events
./bin/kafka-mqtt-bridge
```

### Running in Docker

```bash
docker run -e BRIDGE_KAFKA_BROKER=kafka:9092 \
           -e BRIDGE_MQTT_BROKER=mqtt \
           -e BRIDGE_BRIDGE_MQTT_TO_KAFKA_SOURCE_TOPIC=mqtt/events \
           -e BRIDGE_BRIDGE_MQTT_TO_KAFKA_DEST_TOPIC=kafka-events \
           ghcr.io/csett86/kafka-mqtt-bridge:latest
```

### Running Locally

```bash
# With config file
go run ./cmd/bridge/main.go -config config/local.yaml

# With environment variables only
BRIDGE_KAFKA_BROKER=localhost:9092 BRIDGE_MQTT_BROKER=localhost go run ./cmd/bridge/main.go
```

## Development

### Running Unit Tests

```bash
make test
```

### Running Integration Tests

Integration tests require Docker and Docker Compose to run the Kafka and MQTT broker infrastructure. The `jq` utility is also required for health checking.

```bash
# Run integration tests (starts/stops infrastructure automatically)
make integration-test

# Or manually control the infrastructure:
make integration-up          # Start Kafka and MQTT brokers and wait for health checks
make integration-test-only   # Run tests (infrastructure must be running)
make integration-down        # Stop and clean up infrastructure
```

The `integration-up` target starts the Docker containers and waits for all services to become healthy before returning (up to 120 seconds timeout). This ensures reliable test execution.

#### Integration Test Cases

The integration tests cover:

- **Kafka Producer/Consumer**: Tests basic Kafka message round-trip
- **MQTT Publish/Subscribe**: Tests basic MQTT message round-trip
- **Kafka to MQTT Bridge**: Tests message flow from Kafka to MQTT
- **MQTT to Kafka Bridge**: Tests message flow from MQTT to Kafka

#### Test Environment Variables

You can customize test configuration with environment variables:

- `TEST_KAFKA_BROKERS`: Kafka broker address (default: `localhost:9092`)
- `TEST_MQTT_BROKER`: MQTT broker host (default: `localhost`)
- `TEST_MQTT_PORT`: MQTT broker port (default: `1883`)

### Code Formatting

```bash
make fmt
```

### Linting

```bash
make lint
```

### Building

```bash
make build
```

### Clean Up

```bash
make clean
```

## Project Structure

```
kafka-mqtt-bridge/
├── cmd/
│   └── bridge/
│       └── main.go                       # Application entry point
├── internal/
│   ├── avro/
│   │   └── avro.go                       # Avro serialization/deserialization
│   ├── bridge/
│   │   └── bridge.go                     # Core bridge logic
│   ├── kafka/
│   │   └── kafka.go                      # Kafka client
│   ├── mqtt/
│   │   └── mqtt.go                       # MQTT client
│   └── schemaregistry/
│       └── client.go                     # Azure Schema Registry client
├── pkg/
│   └── config/
│       └── config.go                     # Configuration management
├── test/
│   ├── integration/
│   │   └── integration_test.go           # Integration tests
│   └── mosquitto.conf                    # Mosquitto config for tests
├── config/
│   └── config.yaml                       # Configuration file
├── docker-compose.integration.yml        # Docker Compose for integration tests
├── go.mod                                 # Go module definition
├── go.sum                                 # Go module checksums
├── Makefile                               # Build and development tasks
├── Dockerfile                             # Docker image definition
├── .gitignore                             # Git ignore rules
└── README.md                              # This file
```

## Dependencies

- `github.com/segmentio/kafka-go` - Kafka client library
- `github.com/eclipse/paho.mqtt.golang` - MQTT client library
- `github.com/kelseyhightower/envconfig` - Environment variable configuration
- `github.com/linkedin/goavro/v2` - Avro serialization/deserialization
- `github.com/Azure/azure-sdk-for-go/sdk/azidentity` - Azure authentication
- `github.com/Azure/azure-sdk-for-go/sdk/azcore` - Azure core SDK
- `go.uber.org/zap` - Structured logging
- `gopkg.in/yaml.v3` - YAML configuration parsing

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.

## About This Project: A GitHub Copilot Coding Agent Experiment

This project is not only a functional Kafka-MQTT bridge but also serves as an exploration of what's possible with the [GitHub Copilot coding agent](https://docs.github.com/en/copilot/using-github-copilot/using-copilot-coding-agent-to-work-on-tasks). Nearly the entire codebase was developed through natural language prompts to the coding agent, demonstrating its capabilities for autonomous software development.

### Project History

The project was developed almost entirely through the GitHub Copilot coding agent over approximately one week (November 28 - December 5, 2025). Here's a summary of the major development milestones:

**Foundation (November 28-29)**
- Initial integration test suite with Docker-based Kafka and MQTT infrastructure
- GitHub Actions workflows for building, testing, and releasing
- Bidirectional MQTT↔Kafka bridging
- Multi-platform static binary builds (Linux, Windows, macOS for amd64/arm64)
- Dynamic topic mapping with wildcard support
- Performance tests with memory leak detection

**Enterprise Features (November 29 - December 1)**
- TLS and mutual TLS authentication for MQTT
- SASL/PLAIN authentication for Kafka (Azure Event Hubs compatibility)
- Connection recovery with automatic reconnection for both Kafka and MQTT
- MQTT QoS support (including QoS 2 exactly-once delivery)
- Graceful shutdown with proper Kafka offset commits
- Apache License 2.0 based on dependency analysis
- Dependabot configuration for automated dependency updates
- Copilot instructions for improved agent performance

**Advanced Integrations (December 2-5)**
- Azure Event Hubs integration with SAS token authentication
- Azure Event Hubs Emulator for local integration testing
- Avro serialization/deserialization with Azure Schema Registry
- Migration to Apache Kafka in KRaft mode (no Zookeeper)
- Docker Compose demo environment with bidirectional message flow
- Environment variable configuration for containerized deployments
- Docker image publishing to GitHub Container Registry

### Statistics

- **49 pull requests** created by the Copilot coding agent
- **~3,500 lines of Go code** generated
- **Comprehensive test coverage** including unit, integration, performance, and reliability tests
- **Complete CI/CD pipeline** with automated testing, building, and releasing

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
