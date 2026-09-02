# Relay

A small Kafka lab I built to understand event processing, retries, ordering, consumer groups, and handling duplicate or failed messages.

## Current Stack
- Java 21
- Spring Boot 3.4.3
- Apache Kafka (KRaft mode via Docker Compose)
- Spring for Apache Kafka
- Maven
- Docker Compose

## Architecture & Configuration (Increment 2 & 3)
- **Topic:** `relay.events` (3 partitions, replication factor 1)
- **Kafka Record Key:** `aggregateId` (used for partition routing)
- **Consumer Group:** `relay-consumers`
- **Event Model (`RelayEvent`):**
  - `eventId`: Unique event identifier
  - `aggregateId`: Aggregate identifier used as Kafka message key
  - `type`: Event domain type name
  - `payload`: Event JSON string payload

## Kafka Key, Partitioning, and Ordering Semantics
- **Kafka Key and Partitioning:**
  - Relay passes `aggregateId` as the record key to `KafkaTemplate.send(...)`.
  - Kafka routes records with the same key consistently to the same partition (so long as the partition topology of 3 partitions remains unchanged).
- **Ordering Guarantees:**
  - Kafka guarantees record ordering **only within a partition**.
  - Kafka does **NOT** provide global ordering across the entire topic or across different partitions.
- **Integration Test Proof:**
  - Relay's integration tests (`RelayKafkaIntegrationTest.sameKeyEvents_routedToSamePartition_andPreserveProducerOrdering`) demonstrate that a sequential series of events with the same `aggregateId` are produced to one single partition and consumed from that partition in exact producer sequence order (with strictly increasing offsets).
  - Note: This test demonstrates single-producer sequential ordering for a shared key within its assigned partition; it does not claim global topic ordering or ordering across un-synchronized concurrent producers.

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

Run tests (includes WebMvc controller test & EmbeddedKafka partition/ordering integration tests):
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
