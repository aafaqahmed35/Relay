package com.relay.consumer;

import com.relay.event.RelayEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IdempotentEventProcessor {

    private final EventStore eventStore;
    private final ConcurrentHashMap<String, CompletableFuture<Void>> processingFutures = new ConcurrentHashMap<>();

    public IdempotentEventProcessor(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    public boolean process(RelayEvent event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()) {
            throw new IllegalArgumentException("Event and eventId must not be null or blank");
        }

        String eventId = event.eventId();

        while (true) {
            CompletableFuture<Void> newFuture = new CompletableFuture<>();
            CompletableFuture<Void> existingFuture = processingFutures.putIfAbsent(eventId, newFuture);

            if (existingFuture == null) {
                // This thread atomically claimed ownership of processing for eventId
                try {
                    eventStore.record(event);
                    newFuture.complete(null);
                    return true;
                } catch (Throwable t) {
                    processingFutures.remove(eventId, newFuture);
                    newFuture.completeExceptionally(t);
                    throw t;
                }
            } else {
                // Concurrent or past attempt detected; wait for its outcome
                try {
                    existingFuture.join();
                    // In-flight attempt completed successfully
                    return false;
                } catch (CompletionException e) {
                    // The leading attempt failed and released the claim; retry loop to attempt processing
                }
            }
        }
    }

    public void clear() {
        processingFutures.clear();
    }
}
