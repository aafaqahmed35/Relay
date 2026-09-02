package com.relay;

import com.relay.config.KafkaTopicConfig;
import com.relay.consumer.EventStore;
import com.relay.event.RelayEvent;
import com.relay.producer.RelayEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = { KafkaTopicConfig.RELAY_EVENTS_TOPIC })
@DirtiesContext
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
        RelayEvent event = new RelayEvent(
                "evt-integration-1",
                "aggregate-user-99",
                "UserRegistered",
                "{\"email\":\"user99@example.com\"}"
        );

        // 1. Set up a raw Kafka consumer to directly observe and verify the published record key
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-key-verifier-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.relay.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.relay.event.RelayEvent");

        DefaultKafkaConsumerFactory<String, RelayEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, RelayEvent> testConsumer = consumerFactory.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(testConsumer, KafkaTopicConfig.RELAY_EVENTS_TOPIC);

            // 2. Publish event via RelayEventProducer
            producer.sendEvent(event);

            // 3. Verify Spring Kafka Listener consumes and records the event in EventStore using Awaitility
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(eventStore.getReceivedEvents())
                        .hasSize(1)
                        .contains(event);
            });

            // 4. Inspect the raw ConsumerRecord to obtain direct empirical evidence that the Kafka key == aggregateId
            ConsumerRecord<String, RelayEvent> record = KafkaTestUtils.getSingleRecord(testConsumer, KafkaTopicConfig.RELAY_EVENTS_TOPIC, Duration.ofSeconds(5));
            assertThat(record).isNotNull();
            assertThat(record.key())
                    .as("Kafka record key MUST be equal to aggregateId")
                    .isEqualTo(event.aggregateId());
            assertThat(record.value()).isEqualTo(event);
        }
    }
}
