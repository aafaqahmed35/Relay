# Relay

A small Kafka lab I built to understand event processing, retries, ordering, consumer groups, and handling duplicate or failed messages.

## Current Stack
- Java 21
- Spring Boot 3.4.3
- Apache Kafka (KRaft mode via Docker Compose)
- Spring for Apache Kafka
- Maven
- Docker Compose

## Architecture & Configuration (Increment 2)
- **Topic:** `relay.events` (3 partitions, replication factor 1)
- **Kafka Record Key:** `aggregateId` (used for partition routing)
- **Consumer Group:** `relay-consumers`
- **Event Model (`RelayEvent`):**
  - `eventId`: Unique event identifier
  - `aggregateId`: Aggregate identifier used as Kafka message key
  - `type`: Event domain type name
  - `payload`: Event JSON string payload

## REST API
- `POST /events`: Accepts a `RelayEvent` JSON payload, publishes it to `relay.events` with `aggregateId` as the Kafka key, and returns `202 Accepted`.

Example Request:
```json
{
  "eventId": "evt-1001",
  "aggregateId": "order-42",
  "type": "OrderPlaced",
  "payload": "{\"amount\": 99.99}"
}
```

## Local Kafka Setup

Start local Kafka broker:
```bash
docker compose up -d
```

Stop local Kafka broker:
```bash
docker compose down
```

## Running & Verification Commands

Run tests (includes WebMvc controller test & EmbeddedKafka integration test):
```bash
./mvnw test
```

Package application:
```bash
./mvnw package
```

Run application:
```bash
./mvnw spring-boot:run
```

Happy Path Demo:
```bash
curl -i -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-demo-1","aggregateId":"order-101","type":"OrderPlaced","payload":"{\"amount\":250.00}"}'
```
