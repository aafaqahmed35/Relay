package com.relay;

import com.relay.config.KafkaTopicConfig;
import com.relay.consumer.EventStore;
import com.relay.event.RelayEvent;
import com.relay.producer.RelayEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = { KafkaTopicConfig.RELAY_EVENTS_TOPIC })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RelayKafkaIntegrationTest {

    @Autowired
    private RelayEventProducer producer;

    @Autowired
    private EventStore eventStore;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @AfterEach
    void tearDown() {
        eventStore.clear();
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
    void differentKeys_canRouteToDifferentPartitions_whenKeysHashDifferently() {
        RelayEvent eventKeyA = new RelayEvent("evt-keyA", "agg-alpha-" + UUID.randomUUID(), "TypeA", "payloadA");
        RelayEvent eventKeyB = new RelayEvent("evt-keyB", "agg-beta-" + UUID.randomUUID(), "TypeB", "payloadB");

        SendResult<String, RelayEvent> resultA = producer.sendEvent(eventKeyA).join();
        SendResult<String, RelayEvent> resultB = producer.sendEvent(eventKeyB).join();

        int partitionA = resultA.getRecordMetadata().partition();
        int partitionB = resultB.getRecordMetadata().partition();

        assertThat(resultA.getProducerRecord().key()).isEqualTo(eventKeyA.aggregateId());
        assertThat(resultB.getProducerRecord().key()).isEqualTo(eventKeyB.aggregateId());

        assertThat(partitionA).isBetween(0, 2);
        assertThat(partitionB).isBetween(0, 2);
    }
}
