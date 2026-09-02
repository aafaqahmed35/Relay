package com.relay.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String RELAY_EVENTS_TOPIC = "relay.events";

    @Bean
    public NewTopic relayEventsTopic() {
        return TopicBuilder.name(RELAY_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
