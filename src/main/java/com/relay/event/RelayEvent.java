package com.relay.event;

public record RelayEvent(
        String eventId,
        String aggregateId,
        String type,
        String payload
) {}
