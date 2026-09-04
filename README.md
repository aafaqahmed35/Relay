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

## Idempotent Consumption (Increment 4)
- **At-Least-Once Delivery & Deduplication:** Kafka can redeliver records under at-least-once processing conditions (network retries, rebalances, or consumer restarts). Relay uses the unique `eventId` property of each event to suppress duplicate logical processing.
- **Thread-Safe In-Memory Coordination:** `IdempotentEventProcessor` uses a JDK `ConcurrentHashMap<String, CompletableFuture<Void>>` structure to atomically coordinate in-flight attempts and remember completed event IDs.
- **Failure Recovery:** If processing an event throws an exception, the processor releases the claim and completes exceptionally so that waiting or subsequent attempts can retry and process the event.
- **Scope & Limitations:**
  - Duplicate physical records may still exist on the Kafka topic (Relay does not attempt producer-side deduplication).
  - Deduplication state is stored strictly in memory (`ConcurrentHashMap`).
  - Application restart loses processed IDs; therefore, Relay does **NOT** claim end-to-end persistent exactly-once processing across application restarts.

## Retry Behavior & Dead-Letter Handling (Increment 5)
- **Bounded Retry Mechanism:** When event processing throws an exception, the failure propagates to Spring Kafka's `DefaultErrorHandler`. Relay configures a `FixedBackOff(100L, 2L)` strategy which enforces **1 initial delivery attempt + 2 retries = 3 maximum processing attempts total**. Retries are bounded and not infinite.
- **Dead-Letter Topic (`relay.events.DLT`):** After 3 failed attempts, `DeadLetterPublishingRecoverer` automatically publishes the exhausted record to `relay.events.DLT`.
- **Preserved Record Attributes:** The recoverer preserves the original message key (`aggregateId`), value (`RelayEvent`), and target partition index (3 partitions on DLT matching source topic).
- **Poison Message Isolation:** Dead-lettering prevents unrecoverable records from endlessly blocking partition consumption, allowing subsequent valid events to continue processing normally.
- **Scope & Limitations:**
  - Retry and DLT mechanisms do **NOT** establish exactly-once business processing.
  - At-least-once delivery duplicates remain possible.
  - In-memory deduplication state is cleared on application restart.
  - Relay does not claim production-grade distributed transaction handling or persistent multi-node deduplication.

## Consumer Groups & Rebalance Behavior (Increment 6)
- **Consumer Group Cooperation:** Multiple consumers configured with the same group ID share the work of consuming records from a topic. Kafka's group coordinator dynamically divides topic partitions among active group members.
- **Single-Consumer Partition Ownership:** Within a consumer group, Kafka assigns each partition to **at most one active consumer** at a time. This guarantees that messages within a single partition are processed sequentially by a single worker without concurrent offset collisions.
- **Parallelism & Partition Bounding:** Because `relay.events` has 3 partitions, useful consumer parallelism for a single consumer group is bounded by 3 active partition owners. Adding more than 3 consumers to the same group will leave extra consumers idle without partition assignments.
- **Rebalance Triggering & Redistribution:** Adding a new consumer to a group or shutting down an existing consumer triggers group rebalancing. The group coordinator reassigns partition ownership, redistributing partitions among active members (e.g., transitioning from 1 consumer owning all 3 partitions to 2 consumers non-overlappingly sharing 3 partitions, and back upon consumer leave).
- **Ordering Semantics & Rebalance Impact:**
  - Message ordering remains strictly a **partition-level guarantee**. Consumer group partition division does **not** provide global ordering across different partitions.
  - Group rebalances may briefly pause record processing while partition ownership transfers between consumers.
- **Application Listener & Integration Verification:**
  - Relay's application listener (`RelayEventConsumer`) uses group ID `relay-consumers`. Running multiple instances of the application with this group ID would naturally participate in group assignment.
  - Relay's integration tests (`RelayKafkaIntegrationTest.consumerGroup_dividesPartitionOwnership_withoutOverlap` and `RelayKafkaIntegrationTest.consumerMembershipChange_triggersRebalanceAndRedistributesPartitions`) use real Embedded Kafka group coordination to verify disjoint partition assignment, complete group topic coverage, and dynamic reassignment during membership changes.
- **Scope & Limitations:**
  - Relay does **NOT** claim zero-downtime rebalancing, fixed partition-to-consumer mappings, global topic ordering, or production-grade dynamic autoscaling framework.

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
