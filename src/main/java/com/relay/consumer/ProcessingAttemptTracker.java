package com.relay.consumer;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ProcessingAttemptTracker {

    private final ConcurrentHashMap<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

    public void recordAttempt(String eventId) {
        if (eventId != null && !eventId.isBlank()) {
            attemptCounts.computeIfAbsent(eventId, k -> new AtomicInteger(0)).incrementAndGet();
        }
    }

    public int getAttemptCount(String eventId) {
        AtomicInteger count = attemptCounts.get(eventId);
        return count != null ? count.get() : 0;
    }

    public void clear() {
        attemptCounts.clear();
    }
}
