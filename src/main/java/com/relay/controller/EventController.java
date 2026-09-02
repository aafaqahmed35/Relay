package com.relay.controller;

import com.relay.event.RelayEvent;
import com.relay.producer.RelayEventProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
public class EventController {

    private final RelayEventProducer eventProducer;

    public EventController(RelayEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    @PostMapping
    public ResponseEntity<Void> publishEvent(@RequestBody RelayEvent event) {
        eventProducer.sendEvent(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
