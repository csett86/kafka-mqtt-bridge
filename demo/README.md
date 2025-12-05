# Kafka-MQTT Bridge Demo

This demo showcases the bidirectional message bridging capabilities of the Kafka-MQTT Bridge.

## Architecture

```
MQTT Publisher (transactions) -> Mosquitto -> Kafka-MQTT Bridge -> Kafka Broker -> Kafka Subscriber (transactions)

MQTT Subscriber (master_data) <- Mosquitto <- Kafka-MQTT Bridge <- Kafka Broker <- Kafka Publisher (master_data)
```

## Message Flow

1. **MQTT → Kafka (transactions)**
   - `mqtt-publisher` publishes JSON messages to MQTT topic `transactions` every second
   - The `bridge` subscribes to MQTT `transactions` and forwards messages to Kafka `transactions`
   - `kafka-subscriber` consumes and displays messages from Kafka `transactions`

2. **Kafka → MQTT (master_data)**
   - `kafka-publisher` publishes JSON messages to Kafka topic `master_data` every 10 seconds
   - The `bridge` consumes from Kafka `master_data` and publishes to MQTT `master_data`
   - `mqtt-subscriber` subscribes to and displays messages from MQTT `master_data`

## Components

| Service | Description |
|---------|-------------|
| `mosquitto` | Eclipse Mosquitto MQTT broker |
| `kafka` | Apache Kafka message broker (KRaft mode) |
| `bridge` | Kafka-MQTT Bridge service (built from source) |
| `mqtt-publisher` | Publishes to MQTT "transactions" every second |
| `kafka-subscriber` | Listens to Kafka "transactions" topic |
| `kafka-publisher` | Publishes to Kafka "master_data" every 10 seconds |
| `mqtt-subscriber` | Listens to MQTT "master_data" topic |

## Usage

### Start the Demo

```bash
cd demo
docker compose up --build
```

### View Logs

In separate terminals, you can view specific service logs:

```bash
# View bridge logs
docker compose logs -f bridge

# View all publisher/subscriber activity
docker compose logs -f mqtt-publisher kafka-subscriber kafka-publisher mqtt-subscriber
```

### Expected Output

You should see output like:

```
mqtt-publisher-1     | [MQTT->Kafka] Published to 'transactions': {"id": 1, "type": "transaction", "amount": 42, "timestamp": "2024-01-15T10:30:00Z"}
kafka-subscriber-1   | [MQTT->Kafka] Received from Kafka 'transactions': {"id": 1, "type": "transaction", "amount": 42, "timestamp": "2024-01-15T10:30:00Z"}

kafka-publisher-1    | [Kafka->MQTT] Published to 'master_data': {"id": 1, "type": "master_data", "name": "Product-1", "timestamp": "2024-01-15T10:30:05Z"}
mqtt-subscriber-1    | [Kafka->MQTT] Received from MQTT: master_data {"id": 1, "type": "master_data", "name": "Product-1", "timestamp": "2024-01-15T10:30:05Z"}
```

### Stop the Demo

```bash
docker compose down -v
```

## Configuration

The bridge configuration is in `config/bridge-config.yaml`:

```yaml
kafka:
  broker: "kafka:9092"
  group_id: "kafka-mqtt-bridge-demo"

mqtt:
  broker: "mosquitto"
  port: 1883
  client_id: "kafka-mqtt-bridge-demo"
  qos: 1

bridge:
  name: "kafka-mqtt-bridge-demo"
  log_level: "info"
  
  mqtt_to_kafka:
    source_topic: "transactions"
    dest_topic: "transactions"
  
  kafka_to_mqtt:
    source_topic: "master_data"
    dest_topic: "master_data"
```

## Customization

You can modify the demo by:

1. Editing `config/bridge-config.yaml` to change topic mappings
2. Adjusting publish intervals in `docker-compose.yml`
3. Changing message formats in the publisher scripts
