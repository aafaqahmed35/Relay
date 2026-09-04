# Relay

A small Kafka lab I built to understand event processing, retries, ordering, consumer groups, and handling duplicate or failed messages.

## Purpose

Relay is intentionally a small Kafka behavior lab designed to explore core messaging semantics—such as partition keying, consumer group coordination, idempotent consumption, bounded retries, and dead-letter publishing—rather than a full-scale distributed business application.

## Architecture

The end-to-end flow of Relay is structured as follows:

```
POST /events -> Producer -> relay.events -> Consumer -> success OR retry -> exhausted -> relay.events.DLT
```

* **Spring Boot Producer:** Accepts event payloads via REST API and publishes them to Kafka.
* **Kafka Topic (`relay.events`):** Topic configured with 3 partitions and replication factor 1.
* **Application Consumer:** Single application listener (`relay-consumers` group) processing events.
* **In-Memory Duplicate-Safe Processing:** Deduplication engine preventing double-processing of identical event IDs within a running process.
* **Bounded Spring Kafka Retries:** Automatically retries failed event processing up to 3 total attempts.
* **Dead-Letter Topic (`relay.events.DLT`):** Topic with 3 partitions where exhausted failed messages are published for isolation.

## Event Model

Relay processes events defined by the `RelayEvent` model:

* `eventId`: Unique string identifier for each event instance (used for deduplication).
* `aggregateId`: Aggregate domain identifier passed as the Kafka record key.
* `type`: Event type name (e.g., `OrderPlaced`, or `FAIL_ALWAYS` for testing error paths).
* `payload`: Event payload content string.

The `aggregateId` serves as the Kafka message key.

## Kafka Key + Partitioning

* **Key Assignment:** The producer explicitly supplies `aggregateId` as the Kafka record key when sending to `relay.events`.
* **Partition Routing:** Records sharing the same key are routed consistently to the same partition under Relay's 3-partition topic configuration.
* **Observed Partitioning:** Sequential sends using the same key were experimentally demonstrated to land on the exact same partition.
* **Ordering Scope:** Kafka guarantees message ordering **only within a single partition**.
* **Global Ordering Disclaimer:** There is no global ordering across different partitions or topic-wide. If partition counts or topic configurations change, fixed partition index mapping may also shift.

## Ordering

Integration tests demonstrate partition-level ordering:

* Sequential events sent with the same `aggregateId` land on the same partition with strictly increasing record offsets.
* The application consumer receives and processes these records in their original send order.

**Explicit Scope Boundary:** This partition ordering demonstration does **not** prove ordering across:
* Different partitions;
* Unrelated aggregate keys;
* Arbitrary concurrent producers publishing to the same topic.

## Consumer Groups

Relay implements standard Apache Kafka consumer group semantics:

* **Partition Ownership Division:** Consumers sharing a group ID (`relay-consumers`) dynamically divide topic partition ownership among active members.
* **Single Active Owner Per Partition:** Within a single consumer group, each topic partition is assigned to at most one active consumer at a time.
* **Parallelism Bound:** With 3 topic partitions, at most 3 consumers in one group can actively own partitions simultaneously. Any additional consumers joining the group remain idle.
* **Rebalance & Reassignment:** Consumers joining or leaving the group trigger a Kafka rebalance, redistributing partition assignments across active members. Rebalancing can temporarily interrupt consumption while ownership transfers.
* **Test Verification:** Integration tests verify group behavior using real Embedded Kafka group coordination via `subscribe(...)` (not manual `assign(...)`).

**Processing Semantics Wording:**
Kafka delivers records from each assigned partition in partition order to the consumer. Relay’s current listener processes records synchronously, but application-side concurrency choices could change processing behavior.

## Delivery Semantics

* **At-Least-Once Behavior:** Relay demonstrates at-least-once-style consumer behavior. If processing fails before the record is successfully handled, the same Kafka record may be delivered to the consumer again under the configured retry/recovery behavior. Consumer restarts, rebalances, or failures around offset progress can also lead to a previously seen record being processed again.
* **Physical vs. Logical Duplicates:** Producers or upstream systems may also publish multiple physical Kafka records representing the same logical event. Relay’s duplicate-event test deliberately publishes the same `RelayEvent` twice and demonstrates that both physical records can exist in Kafka while the in-process idempotency layer performs the logical operation once for that `eventId`.
* **No Exactly-Once Claim:** Relay does **not** claim end-to-end exactly-once business processing. Broker delivery semantics and application-side business effects are strictly separate concerns.

## Idempotent Consumption

* **Event ID Deduplication:** Duplicate suppression is based on the unique `eventId`.
* **In-Memory Coordination:** `IdempotentEventProcessor` manages claims using a thread-safe `ConcurrentHashMap<String, CompletableFuture<Void>>`.
* **Concurrency Handling:** The first attempt acquires ownership of the `eventId`. Concurrent duplicate requests wait on the active attempt's completion.
* **State Lifecycle:** Successful processing leaves the `eventId` marked in memory so future duplicates are ignored. Processing failure releases the claim so subsequent retries can re-attempt processing.
* **Logical Side Effects:** Duplicate physical Kafka records may arrive, but logical processing occurs once per `eventId` during the execution of the process.

**Prominent Limitations:**
* Deduplication state is stored entirely in memory (`ConcurrentHashMap`).
* Restarting the application clears all processed event IDs.
* Duplicates received after an application restart will be re-processed.
* Relay provides no persistent or multi-node distributed deduplication guarantee.

## Retry + Dead-Letter Handling

* **Configuration:** `FixedBackOff(100L, 2L)` enforces **1 original attempt + 2 retries = 3 maximum processing attempts**.
* **Synthetic Failure Event:** The `FAIL_ALWAYS` event type deterministically throws an exception to demonstrate retry and recovery behavior.
* **Dead-Letter Publishing:** When all 3 attempts are exhausted, `DeadLetterPublishingRecoverer` publishes the record to `relay.events.DLT`.
* **DLT Configuration:** `relay.events.DLT` is configured with 3 partitions. Relay's demonstrated configuration preserves the original Kafka key (`aggregateId`), `RelayEvent` value, and target partition index when routing to the DLT.
* **Poison Record Isolation:** Moving the exhausted poison record to the DLT allows subsequent valid events on the same partition to continue processing normally.
* **Non-Resolution:** The DLT does not resolve or fix the failed business event; it merely parks the record for inspection and offline analysis.

## Running Locally

### Prerequisites
* Docker & Docker Compose
* Java 21+

### 1. Start Kafka Broker
```bash
docker compose up -d
```

### 2. Run Spring Boot Application
```bash
./mvnw spring-boot:run
```

### 3. Stop Services
```bash
docker compose down
```

## Demo

### Successful Event Processing
```bash
curl -i -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-demo-1","aggregateId":"order-101","type":"OrderPlaced","payload":"{\"amount\":250.00}"}'
```

### Failure & Retry / DLT Demo
```bash
curl -i -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-fail-1","aggregateId":"order-999","type":"FAIL_ALWAYS","payload":"{\"error\":\"test\"}"}'
```

## Tests

Relay's test suite uses Spring Boot tests and `EmbeddedKafka` to verify real messaging behavior:

* `POST /events` endpoint accepts requests and returns HTTP 202 Accepted.
* Producer attaches `aggregateId` as the record key.
* Happy-path event consumption and storage in `EventStore`.
* Same-key sequential events land on the same partition in producer order.
* Duplicate `eventId` suppression prevents double-processing.
* Events with the same `aggregateId` but different `eventId`s are both processed.
* Concurrent duplicate requests wait on the active attempt.
* Failed processing releases the `eventId` claim so retries can acquire it.
* Retry count is strictly bounded to 3 processing attempts.
* Exhausted records are published to `relay.events.DLT` preserving key, value, and partition index.
* Valid records are excluded from the DLT.
* Same-partition consumption continues after a poison record is sent to the DLT.
* Consumer group members non-overlappingly divide 3 partitions.
* Group rebalancing redistributes partition ownership when members join or leave.

*Note:* `EmbeddedKafka` provides integration test evidence within a single JVM process, but it is not a replacement for full production cluster failure testing.

## Guarantees

Relay strictly proves the following technical behaviors:

* `aggregateId` is used as the Kafka message key.
* Sequential sends with the same key land on the same partition in original send order.
* Partition-level message ordering is preserved.
* Duplicate `eventId`s are suppressed within a single running process under tested concurrency.
* Processing retries are bounded to 3 total attempts.
* Exhausted records are published to `relay.events.DLT`.
* Consumer group partition ownership division and rebalance redistribution are verified.

## Limitations

* **No Global Ordering:** Ordering is guaranteed only per partition, not across topics or different keys.
* **No Persistent Deduplication:** Deduplication state is in-memory; restarting the application clears state.
* **No Exactly-Once Guarantees:** Relay does not claim end-to-end exactly-once business processing.
* **No Database / Storage:** Event storage is in-memory for lab purposes.
* **No Kafka Transactions:** Producer/consumer transactions are not enabled.
* **No Distributed Deduplication:** Deduplication is single-process, not shared across multi-node instances.
* **No Production Auth / Security:** Plaintext Kafka listeners, no TLS or SASL.
* **No Schema Registry:** Messages rely on standard JSON serialization without schema versioning.
* **No Production Deployment Architecture:** Designed strictly as a local learning lab.
* **Embedded Kafka Limits:** Integration tests run on Embedded Kafka, which does not simulate all real-world cluster network partitions or broker failure scenarios.
