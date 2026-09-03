package com.relay.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String RELAY_EVENTS_TOPIC = "relay.events";
    public static final String RELAY_EVENTS_DLT_TOPIC = "relay.events.DLT";

    @Bean
    public NewTopic relayEventsTopic() {
        return TopicBuilder.name(RELAY_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic relayEventsDltTopic() {
        // Match source topic partition count (3 partitions) so DeadLetterPublishingRecoverer can preserve partition routing
        return TopicBuilder.name(RELAY_EVENTS_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
