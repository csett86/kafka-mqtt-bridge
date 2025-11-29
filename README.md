# Kafka-MQTT Bridge

A Go application that bridges messages between Apache Kafka and MQTT brokers. This service reads messages from a Kafka topic and publishes them to an MQTT topic, enabling seamless integration between these two popular messaging systems.

## Features

- Bidirectional message bridging between Kafka and MQTT
- **Dynamic topic mapping with wildcard support** for flexible routing
- Configurable Kafka and MQTT connection settings
- Graceful shutdown handling
- Comprehensive logging with Zap
- Production-ready code structure following Go best practices

## Prerequisites

- Go 1.21 or higher
- Apache Kafka 2.8+
- MQTT Broker (e.g., Mosquitto, HiveMQ)
- Docker and Docker Compose (optional, for integration tests)

## Installation

### Clone the Repository

```bash
git clone https://github.com/csett86/kafka-mqtt-bridge.git
cd kafka-mqtt-bridge
```

### Build from Source

```bash
make build
```

This will create a binary in the `bin/` directory.

## Configuration

Create a `config/config.yaml` file or copy from the example:

```bash
cp config/config.yaml config/local.yaml
```

### Basic Configuration (Static Topics)

Edit the configuration file with your Kafka and MQTT settings:

```yaml
kafka:
  brokers:
    - "localhost:9092"
  # Topic to read from for Kafka→MQTT bridging
  source_topic: "events"
  # Topic to write to for MQTT→Kafka bridging
  dest_topic: "mqtt-to-kafka"
  group_id: "kafka-mqtt-bridge"

mqtt:
  broker: "localhost"
  port: 1883
  username: ""
  password: ""
  # Topic to subscribe to for MQTT→Kafka bridging
  source_topic: "mqtt/events"
  # Topic to publish to for Kafka→MQTT bridging
  dest_topic: "kafka/events"
  client_id: "kafka-mqtt-bridge"

bridge:
  name: "kafka-mqtt-bridge"
  log_level: "info"
  buffer_size: 100
```

### Dynamic Topic Mapping with Wildcards

For more flexible routing, you can use topic mappings with wildcard patterns:

```yaml
kafka:
  brokers:
    - "localhost:9092"
  source_topic: "events"
  group_id: "kafka-mqtt-bridge"
  
  # Dynamic topic mappings for MQTT→Kafka bridging
  topic_mappings:
    - source: "sensors/+/data"      # + matches a single level
      target: "kafka-sensors-{1}"   # {1} references the captured segment
    - source: "devices/#"           # # matches multiple levels
      target: "kafka-devices"

mqtt:
  broker: "localhost"
  port: 1883
  client_id: "kafka-mqtt-bridge"
  
  # Dynamic topic mappings for Kafka→MQTT bridging
  topic_mappings:
    - source: "kafka-events-*"      # * is an alias for single-level wildcard
      target: "mqtt/{1}/events"
```

#### Wildcard Patterns

| Pattern | Description | Example Match |
|---------|-------------|---------------|
| `+` | Matches exactly one topic level | `sensors/+/data` matches `sensors/temp/data` |
| `*` | Same as `+` (alias) | `events-*` matches `events-prod` |
| `#` | Matches zero or more levels | `devices/#` matches `devices/floor1/room2` |

#### Template Placeholders

Use `{1}`, `{2}`, etc. to reference captured wildcard segments in the target topic:

- `sensors/+/data` → `kafka/{1}` transforms `sensors/temp/data` to `kafka/temp`
- `home/+/+/status` → `mqtt/{1}/{2}` transforms `home/floor1/room2/status` to `mqtt/floor1/room2`

## Usage

### Running Locally

```bash
# With default config
go run ./cmd/bridge/main.go

# With custom config
go run ./cmd/bridge/main.go -config config/local.yaml
```

### Running the Binary

```bash
./bin/kafka-mqtt-bridge -config config/config.yaml
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

#### Environment Variables

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
│   ├── bridge/
│   │   └── bridge.go                     # Core bridge logic
│   ├── kafka/
│   │   └── kafka.go                      # Kafka client
│   └── mqtt/
│       └── mqtt.go                       # MQTT client
├── pkg/
│   ├── config/
│   │   └── config.go                     # Configuration management
│   └── topicmapper/
│       └── mapper.go                     # Topic mapping with wildcards
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
- `go.uber.org/zap` - Structured logging
- `gopkg.in/yaml.v3` - YAML configuration parsing

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub or contact the maintainers.
