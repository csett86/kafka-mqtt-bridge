# Kafka-MQTT Bridge Demo

This demo showcases the bidirectional message bridging capabilities of the Kafka-MQTT Bridge with multiple MQTT brokers and bridge instances working in parallel.

## Architecture

```
MQTT Publisher  -> Mosquitto  -> Bridge  -> Kafka Broker -> Kafka Subscriber (transactions)
MQTT Publisher2 -> Mosquitto2 -> Bridge2 /
MQTT Publisher3 -> Mosquitto3 -> Bridge3 /

MQTT Subscriber  <- Mosquitto  <- Bridge  <- Kafka Broker <- Kafka Publisher (master_data)
MQTT Subscriber2 <- Mosquitto2 <- Bridge2 /
MQTT Subscriber3 <- Mosquitto3 <- Bridge3 /
```

## Message Flow

1. **MQTT → Kafka (transactions)**
   - `mqtt-publisher`, `mqtt-publisher2`, and `mqtt-publisher3` publish JSON messages to MQTT topic `transactions` on their respective mosquitto brokers every second
   - Each `bridge` instance subscribes to MQTT `transactions` on its respective mosquitto broker
   - The bridges forward messages to Kafka `transactions` topic
   - `kafka-subscriber` consumes and displays messages from Kafka `transactions`

2. **Kafka → MQTT (master_data)**
   - `kafka-publisher` publishes JSON messages to Kafka topic `master_data` every 10 seconds
   - All three `bridge` instances consume from Kafka `master_data` (as different consumer group members)
   - The bridges publish messages to MQTT `master_data` on their respective mosquitto brokers
   - `mqtt-subscriber`, `mqtt-subscriber2`, and `mqtt-subscriber3` subscribe to and display messages from MQTT `master_data` on their respective mosquitto brokers

## Components

| Service | Description |
|---------|-------------|
| `mosquitto` | Eclipse Mosquitto MQTT broker (port 1883) |
| `mosquitto2` | Eclipse Mosquitto MQTT broker (port 1884) |
| `mosquitto3` | Eclipse Mosquitto MQTT broker (port 1885) |
| `kafka` | Apache Kafka message broker (KRaft mode) |
| `bridge` | Kafka-MQTT Bridge service connecting mosquitto and Kafka |
| `bridge2` | Kafka-MQTT Bridge service connecting mosquitto2 and Kafka |
| `bridge3` | Kafka-MQTT Bridge service connecting mosquitto3 and Kafka |
| `mqtt-publisher` | Publishes to MQTT "transactions" on mosquitto every second |
| `mqtt-publisher2` | Publishes to MQTT "transactions" on mosquitto2 every second |
| `mqtt-publisher3` | Publishes to MQTT "transactions" on mosquitto3 every second |
| `kafka-subscriber` | Listens to Kafka "transactions" topic |
| `kafka-publisher` | Publishes to Kafka "master_data" every 10 seconds |
| `mqtt-subscriber` | Listens to MQTT "master_data" topic on mosquitto |
| `mqtt-subscriber2` | Listens to MQTT "master_data" topic on mosquitto2 |
| `mqtt-subscriber3` | Listens to MQTT "master_data" topic on mosquitto3 |

## Usage

### Start the Demo

```bash
cd demo
docker compose up
```

### View Logs

In separate terminals, you can view specific service logs:

```bash
# View all bridge logs
docker compose logs -f bridge bridge2 bridge3

# View a specific bridge
docker compose logs -f bridge

# View all MQTT publishers
docker compose logs -f mqtt-publisher mqtt-publisher2 mqtt-publisher3

# View all MQTT subscribers
docker compose logs -f mqtt-subscriber mqtt-subscriber2 mqtt-subscriber3

# View all publisher/subscriber activity
docker compose logs -f mqtt-publisher mqtt-publisher2 mqtt-publisher3 kafka-subscriber kafka-publisher mqtt-subscriber mqtt-subscriber2 mqtt-subscriber3
```

### Expected Output

You should see output like:

```
mqtt-publisher-1     | [MQTT->Kafka] Published to mosquitto 'transactions': {"id": 1, "type": "transaction", "amount": 42, "timestamp": "2024-01-15T10:30:00Z"}
mqtt-publisher2-1    | [MQTT->Kafka] Published to mosquitto2 'transactions': {"id": 1, "type": "transaction", "amount": 123, "timestamp": "2024-01-15T10:30:00Z"}
mqtt-publisher3-1    | [MQTT->Kafka] Published to mosquitto3 'transactions': {"id": 1, "type": "transaction", "amount": 789, "timestamp": "2024-01-15T10:30:00Z"}
kafka-subscriber-1   | [MQTT->Kafka] Received from Kafka 'transactions': {"id": 1, "type": "transaction", "amount": 42, "timestamp": "2024-01-15T10:30:00Z"}

kafka-publisher-1    | [Kafka->MQTT] Published to 'master_data': {"id": 1, "type": "master_data", "name": "Product-1", "timestamp": "2024-01-15T10:30:05Z"}
mqtt-subscriber-1    | [Kafka->MQTT] Received from mosquitto: master_data {"id": 1, "type": "master_data", "name": "Product-1", "timestamp": "2024-01-15T10:30:05Z"}
mqtt-subscriber2-1   | [Kafka->MQTT] Received from mosquitto2: master_data {"id": 1, "type": "master_data", "name": "Product-1", "timestamp": "2024-01-15T10:30:05Z"}
mqtt-subscriber3-1   | [Kafka->MQTT] Received from mosquitto3: master_data {"id": 1, "type": "master_data", "name": "Product-1", "timestamp": "2024-01-15T10:30:05Z"}
```

### Stop the Demo

```bash
docker compose down -v
```

## Configuration

The bridge configurations are in the `config/` directory:

### bridge-config.yaml (Bridge 1)

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

### bridge-config2.yaml (Bridge 2)

Similar configuration but connects to `mosquitto2` with `client_id: "kafka-mqtt-bridge-demo-2"` and `group_id: "kafka-mqtt-bridge-demo-2"`.

### bridge-config3.yaml (Bridge 3)

Similar configuration but connects to `mosquitto3` with `client_id: "kafka-mqtt-bridge-demo-3"` and `group_id: "kafka-mqtt-bridge-demo-3"`.

All three bridges bridge the same topics:
- MQTT `transactions` → Kafka `transactions`
- Kafka `master_data` → MQTT `master_data`

## Customization

You can modify the demo by:

1. Editing `config/bridge-config.yaml` to change topic mappings
2. Adjusting publish intervals in `docker-compose.yml`
3. Changing message formats in the publisher scripts
