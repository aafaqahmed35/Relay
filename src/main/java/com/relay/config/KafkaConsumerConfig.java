package com.relay.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(KafkaTopicConfig.RELAY_EVENTS_DLT_TOPIC, record.partition()));
        // FixedBackOff(100L, 2L): 1 initial delivery attempt + 2 retries = 3 maximum processing attempts total
        FixedBackOff backOff = new FixedBackOff(100L, 2L);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
