package com.relay.consumer;

import com.relay.config.KafkaTopicConfig;
import com.relay.event.RelayEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RelayEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RelayEventConsumer.class);

    private final IdempotentEventProcessor idempotentEventProcessor;

    public RelayEventConsumer(IdempotentEventProcessor idempotentEventProcessor) {
        this.idempotentEventProcessor = idempotentEventProcessor;
    }

    @KafkaListener(topics = KafkaTopicConfig.RELAY_EVENTS_TOPIC, groupId = "relay-consumers")
    public void consume(RelayEvent event) {
        log.info("Consumed eventId={} with aggregateId={}", event.eventId(), event.aggregateId());
        idempotentEventProcessor.process(event);
    }
}
