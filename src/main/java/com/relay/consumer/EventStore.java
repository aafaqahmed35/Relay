package com.relay.consumer;

import com.relay.event.RelayEvent;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class EventStore {

    private final List<RelayEvent> receivedEvents = new CopyOnWriteArrayList<>();

    public void record(RelayEvent event) {
        receivedEvents.add(event);
    }

    public List<RelayEvent> getReceivedEvents() {
        return Collections.unmodifiableList(receivedEvents);
    }

    public void clear() {
        receivedEvents.clear();
    }
}
