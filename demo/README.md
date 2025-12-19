# Kafka-MQTT Bridge Demo

This demo showcases bidirectional message movement between multiple MQTT brokers and Redpanda using Redpanda Connect MQTT source and sink connectors.

## Architecture

```
MQTT Publisher  -> Mosquitto  -> Redpanda Connect (MQTT Source) -> Redpanda -> Kafka Subscriber (transactions)
MQTT Publisher2 -> Mosquitto2 -> Redpanda Connect (MQTT Source) /
MQTT Publisher3 -> Mosquitto3 -> Redpanda Connect (MQTT Source) /

MQTT Subscriber  <- Mosquitto  <- Redpanda Connect (MQTT Sink) <- Redpanda <- Kafka Publisher (master_data)
MQTT Subscriber2 <- Mosquitto2 <- Redpanda Connect (MQTT Sink) /
MQTT Subscriber3 <- Mosquitto3 <- Redpanda Connect (MQTT Sink) /
```

## Message Flow

1. **MQTT → Redpanda (transactions)**
   - `mqtt-publisher`, `mqtt-publisher2`, and `mqtt-publisher3` publish JSON messages to MQTT topic `transactions` on their respective mosquitto brokers every second
   - Redpanda Connect MQTT source connectors subscribe to `transactions` on each mosquitto broker
   - The connectors forward messages to Redpanda `transactions` topic
   - `kafka-subscriber` consumes and displays messages from Redpanda `transactions`

2. **Redpanda → MQTT (master_data)**
   - `kafka-publisher` publishes JSON messages to Redpanda topic `master_data` every 10 seconds
   - Redpanda Connect MQTT sink connectors consume from Redpanda `master_data`
   - The connectors publish messages to MQTT `master_data` on their respective mosquitto brokers
   - `mqtt-subscriber`, `mqtt-subscriber2`, and `mqtt-subscriber3` subscribe to and display messages from MQTT `master_data` on their respective mosquitto brokers

## Components

| Service | Description |
|---------|-------------|
| `mosquitto` | Eclipse Mosquitto MQTT broker (port 1883) |
| `mosquitto2` | Eclipse Mosquitto MQTT broker (port 1884) |
| `mosquitto3` | Eclipse Mosquitto MQTT broker (port 1885) |
| `redpanda` | Redpanda broker (Kafka API compatible) |
| `connect` | Redpanda Connect worker running MQTT source/sink connectors |
| `configure-connect` | One-shot job that applies MQTT connector configs at startup |
| `mqtt-publisher` | Publishes to MQTT "transactions" on mosquitto every second |
| `mqtt-publisher2` | Publishes to MQTT "transactions" on mosquitto2 every second |
| `mqtt-publisher3` | Publishes to MQTT "transactions" on mosquitto3 every second |
| `kafka-subscriber` | Listens to Redpanda "transactions" topic |
| `kafka-publisher` | Publishes to Redpanda "master_data" every 10 seconds |
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
# View all Connect logs
docker compose logs -f connect configure-connect

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
kafka-subscriber-1   | [MQTT->Kafka] Received from Redpanda 'transactions': {"id": 1, "type": "transaction", "amount": 42, "timestamp": "2024-01-15T10:30:00Z"}

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

Redpanda Connect loads MQTT connector definitions from `connectors/*.json`:

- `transactions-source-*.json`: MQTT source connectors that read `transactions` from each mosquitto broker and write to the Redpanda `transactions` topic.
- `master-data-sink-*.json`: MQTT sink connectors that read `master_data` from Redpanda and publish to each mosquitto broker.

You can customize the demo by:

1. Editing the JSON connector files to change topics, QoS, or target brokers
2. Adjusting publish intervals in `docker-compose.yml`
3. Changing message formats in the publisher scripts
