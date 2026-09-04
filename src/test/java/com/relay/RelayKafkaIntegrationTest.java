package com.relay;

import com.relay.config.KafkaTopicConfig;
import com.relay.consumer.EventStore;
import com.relay.consumer.IdempotentEventProcessor;
import com.relay.consumer.ProcessingAttemptTracker;
import com.relay.event.RelayEvent;
import com.relay.producer.RelayEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = { KafkaTopicConfig.RELAY_EVENTS_TOPIC, KafkaTopicConfig.RELAY_EVENTS_DLT_TOPIC })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RelayKafkaIntegrationTest {

    @Autowired
    private RelayEventProducer producer;

    @Autowired
    private EventStore eventStore;

    @Autowired
    private IdempotentEventProcessor idempotentEventProcessor;

    @Autowired
    private ProcessingAttemptTracker attemptTracker;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @AfterEach
    void tearDown() {
        eventStore.clear();
        idempotentEventProcessor.clear();
        attemptTracker.clear();
    }

    @Test
    void publishAndConsumeHappyPath_verifiesEventConsumptionAndAggregateIdAsKafkaKey() {
        String testAggregateId = "agg-happy-path-" + UUID.randomUUID();
        RelayEvent event = new RelayEvent(
                "evt-integration-1",
                testAggregateId,
                "UserRegistered",
                "{\"email\":\"user99@example.com\"}"
        );

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-key-verifier-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.relay.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.relay.event.RelayEvent");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, RelayEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, RelayEvent> testConsumer = consumerFactory.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(testConsumer, KafkaTopicConfig.RELAY_EVENTS_TOPIC);

            producer.sendEvent(event).join();

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<RelayEvent> appEvents = eventStore.getReceivedEvents()
                        .stream()
                        .filter(e -> testAggregateId.equals(e.aggregateId()))
                        .toList();
                assertThat(appEvents).hasSize(1).contains(event);
            });

            ConsumerRecord<String, RelayEvent> targetRecord = null;
            long startTime = System.currentTimeMillis();
            while (targetRecord == null && System.currentTimeMillis() - startTime < 10000) {
                ConsumerRecords<String, RelayEvent> records = testConsumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, RelayEvent> rec : records) {
                    if (testAggregateId.equals(rec.key())) {
                        targetRecord = rec;
                        break;
                    }
                }
            }

            assertThat(targetRecord).isNotNull();
            assertThat(targetRecord.key())
                    .as("Kafka record key MUST be equal to aggregateId")
                    .isEqualTo(testAggregateId);
            assertThat(targetRecord.value()).isEqualTo(event);
        }
    }

    @Test
    void sameKeyEvents_routedToSamePartition_andPreserveProducerOrdering() {
        String sharedAggregateId = "order-agg-" + UUID.randomUUID();

        List<RelayEvent> sequence = List.of(
                new RelayEvent("evt-seq-1", sharedAggregateId, "OrderCreated", "{\"step\":1}"),
                new RelayEvent("evt-seq-2", sharedAggregateId, "PaymentRequested", "{\"step\":2}"),
                new RelayEvent("evt-seq-3", sharedAggregateId, "PaymentProcessed", "{\"step\":3}"),
                new RelayEvent("evt-seq-4", sharedAggregateId, "InventoryReserved", "{\"step\":4}"),
                new RelayEvent("evt-seq-5", sharedAggregateId, "OrderCompleted", "{\"step\":5}")
        );

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-ordering-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.relay.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.relay.event.RelayEvent");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, RelayEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, RelayEvent> testConsumer = consumerFactory.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(testConsumer, KafkaTopicConfig.RELAY_EVENTS_TOPIC);

            List<SendResult<String, RelayEvent>> sendResults = new ArrayList<>();
            for (RelayEvent evt : sequence) {
                CompletableFuture<SendResult<String, RelayEvent>> future = producer.sendEvent(evt);
                sendResults.add(future.join());
            }

            int expectedPartition = sendResults.get(0).getRecordMetadata().partition();
            for (int i = 0; i < sendResults.size(); i++) {
                assertThat(sendResults.get(i).getRecordMetadata().partition())
                        .as("Producer metadata for record %d must match partition %d", i, expectedPartition)
                        .isEqualTo(expectedPartition);
            }

            List<ConsumerRecord<String, RelayEvent>> polledRecords = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            while (polledRecords.size() < sequence.size() && System.currentTimeMillis() - startTime < 10000) {
                ConsumerRecords<String, RelayEvent> records = testConsumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, RelayEvent> rec : records) {
                    if (sharedAggregateId.equals(rec.key())) {
                        polledRecords.add(rec);
                    }
                }
            }

            assertThat(polledRecords)
                    .as("Must consume exactly %d records for the key %s", sequence.size(), sharedAggregateId)
                    .hasSize(sequence.size());

            for (int i = 0; i < polledRecords.size(); i++) {
                ConsumerRecord<String, RelayEvent> rec = polledRecords.get(i);
                assertThat(rec.key()).isEqualTo(sharedAggregateId);
                assertThat(rec.partition())
                        .as("Every consumed record with aggregateId %s must land on partition %d", sharedAggregateId, expectedPartition)
                        .isEqualTo(expectedPartition);
                assertThat(rec.value()).isEqualTo(sequence.get(i));

                if (i > 0) {
                    assertThat(rec.offset())
                            .as("Offset of record %d (%d) must be strictly greater than record %d (%d)",
                                    i, rec.offset(), i - 1, polledRecords.get(i - 1).offset())
                            .isGreaterThan(polledRecords.get(i - 1).offset());
                }
            }

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<RelayEvent> appEvents = eventStore.getReceivedEvents()
                        .stream()
                        .filter(e -> sharedAggregateId.equals(e.aggregateId()))
                        .toList();

                assertThat(appEvents)
                        .as("Relay EventStore must observe events in exact publication sequence order")
                        .isEqualTo(sequence);
            });
        }
    }

    @Test
    void duplicateKafkaEvent_recordedOnceInEventStore_whileKafkaTopicContainsBothRecords() {
        String testAggregateId = "agg-dup-kafka-" + UUID.randomUUID();
        RelayEvent duplicateEvent = new RelayEvent(
                "evt-dup-kafka-101",
                testAggregateId,
                "OrderPaid",
                "{\"amount\":199.99}"
        );

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-dup-verifier-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.relay.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.relay.event.RelayEvent");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, RelayEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, RelayEvent> testConsumer = consumerFactory.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(testConsumer, KafkaTopicConfig.RELAY_EVENTS_TOPIC);

            // Publish exact same logical event twice to Kafka
            producer.sendEvent(duplicateEvent).join();
            producer.sendEvent(duplicateEvent).join();

            // Verify EventStore records the logical event EXACTLY ONCE
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<RelayEvent> appEvents = eventStore.getReceivedEvents()
                        .stream()
                        .filter(e -> testAggregateId.equals(e.aggregateId()))
                        .toList();
                assertThat(appEvents).hasSize(1).contains(duplicateEvent);
            });

            // Verify Kafka topic physically contains BOTH records
            List<ConsumerRecord<String, RelayEvent>> polledRecords = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            while (polledRecords.size() < 2 && System.currentTimeMillis() - startTime < 10000) {
                ConsumerRecords<String, RelayEvent> records = testConsumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, RelayEvent> rec : records) {
                    if (testAggregateId.equals(rec.key())) {
                        polledRecords.add(rec);
                    }
                }
            }

            assertThat(polledRecords)
                    .as("Kafka topic MUST contain both published physical records")
                    .hasSize(2);
            assertThat(polledRecords.get(0).value()).isEqualTo(duplicateEvent);
            assertThat(polledRecords.get(1).value()).isEqualTo(duplicateEvent);
        }
    }

    @Test
    void sameAggregateId_differentEventIds_bothProcessedSuccessfully() {
        String sharedAggregateId = "agg-same-key-" + UUID.randomUUID();
        RelayEvent event1 = new RelayEvent("evt-same-key-1", sharedAggregateId, "StepOne", "{\"step\":1}");
        RelayEvent event2 = new RelayEvent("evt-same-key-2", sharedAggregateId, "StepTwo", "{\"step\":2}");

        producer.sendEvent(event1).join();
        producer.sendEvent(event2).join();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<RelayEvent> appEvents = eventStore.getReceivedEvents()
                    .stream()
                    .filter(e -> sharedAggregateId.equals(e.aggregateId()))
                    .toList();
            assertThat(appEvents)
                    .as("Both events sharing aggregateId but having distinct eventIds must be processed")
                    .hasSize(2)
                    .containsExactly(event1, event2);
        });
    }

    @Test
    void failingEvent_retriedBoundedNumberOfTimes_maximumThreeTotalAttempts() {
        String failingEventId = "evt-retry-fail-" + UUID.randomUUID();
        String failingAggregateId = "agg-retry-fail-" + UUID.randomUUID();
        RelayEvent failingEvent = new RelayEvent(failingEventId, failingAggregateId, "FAIL_ALWAYS", "{\"fail\":true}");

        producer.sendEvent(failingEvent).join();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(attemptTracker.getAttemptCount(failingEventId))
                    .as("Failing event MUST be attempted exactly 3 times total (1 initial delivery attempt + 2 retries)")
                    .isEqualTo(3);
        });

        List<RelayEvent> storeEvents = eventStore.getReceivedEvents()
                .stream()
                .filter(e -> failingEventId.equals(e.eventId()))
                .toList();
        assertThat(storeEvents).as("EventStore MUST NOT contain the exhausted failing event").isEmpty();
    }

    @Test
    void exhaustedFailingEvent_publishedToDeadLetterTopic_withMatchingKeyAndPartition() {
        String failingEventId = "evt-dlt-fail-" + UUID.randomUUID();
        String failingAggregateId = "agg-dlt-fail-" + UUID.randomUUID();
        RelayEvent failingEvent = new RelayEvent(failingEventId, failingAggregateId, "FAIL_ALWAYS", "{\"dlt\":true}");

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-dlt-verifier-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.relay.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.relay.event.RelayEvent");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, RelayEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, RelayEvent> dltConsumer = consumerFactory.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, KafkaTopicConfig.RELAY_EVENTS_DLT_TOPIC);

            SendResult<String, RelayEvent> sendResult = producer.sendEvent(failingEvent).join();
            int expectedPartition = sendResult.getRecordMetadata().partition();

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(attemptTracker.getAttemptCount(failingEventId))
                        .as("Failing event MUST be attempted exactly 3 times total before dead-lettering")
                        .isEqualTo(3);
            });

            List<ConsumerRecord<String, RelayEvent>> matchedRecords = new ArrayList<>();
            long pollStartTime = System.currentTimeMillis();
            while (matchedRecords.isEmpty() && System.currentTimeMillis() - pollStartTime < 10000) {
                ConsumerRecords<String, RelayEvent> records = dltConsumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, RelayEvent> rec : records) {
                    if (failingAggregateId.equals(rec.key()) && failingEvent.equals(rec.value())) {
                        matchedRecords.add(rec);
                    }
                }
            }

            assertThat(matchedRecords)
                    .as("Exhausted failing record MUST be published to relay.events.DLT topic")
                    .hasSize(1);

            ConsumerRecord<String, RelayEvent> matchedDltRecord = matchedRecords.get(0);
            assertThat(matchedDltRecord.key()).isEqualTo(failingAggregateId);
            assertThat(matchedDltRecord.value()).isEqualTo(failingEvent);
            assertThat(matchedDltRecord.partition())
                    .as("DLT record partition MUST match source record partition index")
                    .isEqualTo(expectedPartition);
        }
    }

    @Test
    void successfulEvent_processedNormally_andNotSentToDeadLetterTopic() {
        String validEventId = "evt-success-dlt-" + UUID.randomUUID();
        String validAggregateId = "agg-success-dlt-" + UUID.randomUUID();
        RelayEvent validEvent = new RelayEvent(validEventId, validAggregateId, "OrderProcessed", "{\"valid\":true}");

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-dlt-check-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.relay.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.relay.event.RelayEvent");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, RelayEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, RelayEvent> dltConsumer = consumerFactory.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, KafkaTopicConfig.RELAY_EVENTS_DLT_TOPIC);

            producer.sendEvent(validEvent).join();

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<RelayEvent> storeEvents = eventStore.getReceivedEvents()
                        .stream()
                        .filter(e -> validEventId.equals(e.eventId()))
                        .toList();
                assertThat(storeEvents).hasSize(1).contains(validEvent);
                assertThat(attemptTracker.getAttemptCount(validEventId)).isEqualTo(1);
            });

            List<ConsumerRecord<String, RelayEvent>> matchedRecords = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 2000) {
                ConsumerRecords<String, RelayEvent> records = dltConsumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, RelayEvent> rec : records) {
                    if (validAggregateId.equals(rec.key())) {
                        matchedRecords.add(rec);
                    }
                }
            }

            assertThat(matchedRecords)
                    .as("Successful event MUST NOT produce any DLT record for its unique key")
                    .isEmpty();
        }
    }

    @Test
    void failureDoesNotPoisonFutureValidEvents() {
        String sharedAggregateId = "agg-shared-partition-" + UUID.randomUUID();
        String failingEventId = "evt-poison-fail-" + UUID.randomUUID();
        RelayEvent failingEvent = new RelayEvent(failingEventId, sharedAggregateId, "FAIL_ALWAYS", "{\"fail\":true}");

        String validEventId = "evt-poison-valid-" + UUID.randomUUID();
        RelayEvent validEvent = new RelayEvent(validEventId, sharedAggregateId, "UserLogin", "{\"valid\":true}");

        SendResult<String, RelayEvent> failResult = producer.sendEvent(failingEvent).join();
        SendResult<String, RelayEvent> validResult = producer.sendEvent(validEvent).join();

        assertThat(failResult.getRecordMetadata().partition())
                .as("Both records MUST route to the exact same Kafka partition due to identical aggregateId key")
                .isEqualTo(validResult.getRecordMetadata().partition());

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(attemptTracker.getAttemptCount(failingEventId))
                    .as("Failing event MUST be attempted exactly 3 times total before dead-lettering")
                    .isEqualTo(3);

            List<RelayEvent> failingStoreEvents = eventStore.getReceivedEvents()
                    .stream()
                    .filter(e -> failingEventId.equals(e.eventId()))
                    .toList();
            assertThat(failingStoreEvents)
                    .as("Failing event MUST NOT be recorded in EventStore")
                    .isEmpty();

            List<RelayEvent> validStoreEvents = eventStore.getReceivedEvents()
                    .stream()
                    .filter(e -> validEventId.equals(e.eventId()))
                    .toList();
            assertThat(validStoreEvents)
                    .as("Subsequent valid event on the same partition MUST be processed successfully")
                    .hasSize(1)
                    .contains(validEvent);

            assertThat(attemptTracker.getAttemptCount(validEventId))
                    .as("Valid event MUST have attempt count 1")
                    .isEqualTo(1);
        });
    }

    private Consumer<String, RelayEvent> createRawConsumer(String groupId, String clientId) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(groupId, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.relay.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.relay.event.RelayEvent");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        DefaultKafkaConsumerFactory<String, RelayEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        return consumerFactory.createConsumer();
    }

    @Test
    void consumerGroup_dividesPartitionOwnership_withoutOverlap() {
        String sharedGroupId = "test-group-divides-" + UUID.randomUUID();

        try (Consumer<String, RelayEvent> consumerA = createRawConsumer(sharedGroupId, "consumer-a-" + UUID.randomUUID());
             Consumer<String, RelayEvent> consumerB = createRawConsumer(sharedGroupId, "consumer-b-" + UUID.randomUUID())) {

            consumerA.subscribe(Collections.singletonList(KafkaTopicConfig.RELAY_EVENTS_TOPIC));
            consumerB.subscribe(Collections.singletonList(KafkaTopicConfig.RELAY_EVENTS_TOPIC));

            await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                consumerA.poll(Duration.ofMillis(100));
                consumerB.poll(Duration.ofMillis(100));

                Set<Integer> partitionsA = consumerA.assignment().stream()
                        .map(TopicPartition::partition)
                        .collect(Collectors.toSet());
                Set<Integer> partitionsB = consumerB.assignment().stream()
                        .map(TopicPartition::partition)
                        .collect(Collectors.toSet());

                assertThat(partitionsA).as("Consumer A must own at least one partition").isNotEmpty();
                assertThat(partitionsB).as("Consumer B must own at least one partition").isNotEmpty();

                Set<Integer> union = new HashSet<>(partitionsA);
                union.addAll(partitionsB);
                assertThat(union).as("Union of consumer assignments must cover all 3 partitions").containsExactlyInAnyOrder(0, 1, 2);

                Set<Integer> intersection = new HashSet<>(partitionsA);
                intersection.retainAll(partitionsB);
                assertThat(intersection).as("Consumer assignments must be disjoint (no partition assigned to both)").isEmpty();
            });
        }
    }

    @Test
    void consumerMembershipChange_triggersRebalanceAndRedistributesPartitions() {
        String sharedGroupId = "test-group-rebalance-" + UUID.randomUUID();

        Consumer<String, RelayEvent> consumerA = createRawConsumer(sharedGroupId, "consumer-a-" + UUID.randomUUID());
        Consumer<String, RelayEvent> consumerB = null;

        try {
            consumerA.subscribe(Collections.singletonList(KafkaTopicConfig.RELAY_EVENTS_TOPIC));

            // Poll Consumer A until group assignment stabilizes and A owns all 3 partitions
            await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                consumerA.poll(Duration.ofMillis(100));
                Set<Integer> initialPartitionsA = consumerA.assignment().stream()
                        .map(TopicPartition::partition)
                        .collect(Collectors.toSet());
                assertThat(initialPartitionsA).as("Single active consumer initially owns all 3 partitions").containsExactlyInAnyOrder(0, 1, 2);
            });

            // Start Consumer B in the same consumer group
            consumerB = createRawConsumer(sharedGroupId, "consumer-b-" + UUID.randomUUID());
            consumerB.subscribe(Collections.singletonList(KafkaTopicConfig.RELAY_EVENTS_TOPIC));

            final Consumer<String, RelayEvent> finalB = consumerB;

            // Poll both consumers until rebalance completes and partition redistribution is reflected
            await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                consumerA.poll(Duration.ofMillis(100));
                finalB.poll(Duration.ofMillis(100));

                Set<Integer> partitionsA = consumerA.assignment().stream()
                        .map(TopicPartition::partition)
                        .collect(Collectors.toSet());
                Set<Integer> partitionsB = finalB.assignment().stream()
                        .map(TopicPartition::partition)
                        .collect(Collectors.toSet());

                assertThat(partitionsB).as("Consumer B must receive at least one partition after joining").isNotEmpty();
                assertThat(partitionsA).as("Consumer A's assignment must change after Consumer B joins").isNotEqualTo(Set.of(0, 1, 2));

                Set<Integer> union = new HashSet<>(partitionsA);
                union.addAll(partitionsB);
                assertThat(union).as("Union of assignments must still cover all 3 partitions").containsExactlyInAnyOrder(0, 1, 2);

                Set<Integer> intersection = new HashSet<>(partitionsA);
                intersection.retainAll(partitionsB);
                assertThat(intersection).as("Assignments after rebalance must remain non-overlapping").isEmpty();
            });

            // Optional leave/rejoin demonstration: Close Consumer B and verify Consumer A regains all partitions
            consumerB.close();
            consumerB = null;

            await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                consumerA.poll(Duration.ofMillis(100));
                Set<Integer> regainedPartitionsA = consumerA.assignment().stream()
                        .map(TopicPartition::partition)
                        .collect(Collectors.toSet());
                assertThat(regainedPartitionsA).as("After Consumer B leaves, Consumer A regains all 3 partitions").containsExactlyInAnyOrder(0, 1, 2);
            });

        } finally {
            if (consumerB != null) {
                consumerB.close();
            }
            consumerA.close();
        }
    }
}
