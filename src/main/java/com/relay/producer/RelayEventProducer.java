package com.relay.producer;

import com.relay.config.KafkaTopicConfig;
import com.relay.event.RelayEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class RelayEventProducer {

    private static final Logger log = LoggerFactory.getLogger(RelayEventProducer.class);

    private final KafkaTemplate<String, RelayEvent> kafkaTemplate;

    public RelayEventProducer(KafkaTemplate<String, RelayEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, RelayEvent>> sendEvent(RelayEvent event) {
        log.info("Publishing eventId={} with aggregateId key={} to topic={}",
                event.eventId(), event.aggregateId(), KafkaTopicConfig.RELAY_EVENTS_TOPIC);
        return kafkaTemplate.send(KafkaTopicConfig.RELAY_EVENTS_TOPIC, event.aggregateId(), event);
    }
}
