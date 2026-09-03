package com.relay.consumer;

import com.relay.event.RelayEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotentEventProcessorTest {

    private EventStore eventStore;
    private IdempotentEventProcessor processor;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        processor = new IdempotentEventProcessor(eventStore);
    }

    @Test
    void process_throwsIllegalArgumentException_whenEventOrEventIdIsInvalid() {
        assertThatThrownBy(() -> processor.process(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Event and eventId must not be null or blank");

        RelayEvent nullEventId = new RelayEvent(null, "agg-1", "TypeA", "payload");
        assertThatThrownBy(() -> processor.process(nullEventId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Event and eventId must not be null or blank");

        RelayEvent blankEventId = new RelayEvent("   ", "agg-1", "TypeA", "payload");
        assertThatThrownBy(() -> processor.process(blankEventId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Event and eventId must not be null or blank");
    }

    @Test
    void process_allowsSameAggregateId_withDifferentEventIds() {
        String sharedAggregateId = "order-agg-" + UUID.randomUUID();
        RelayEvent event1 = new RelayEvent("evt-001", sharedAggregateId, "OrderCreated", "{\"step\":1}");
        RelayEvent event2 = new RelayEvent("evt-002", sharedAggregateId, "PaymentRequested", "{\"step\":2}");

        boolean result1 = processor.process(event1);
        boolean result2 = processor.process(event2);

        assertThat(result1).isTrue();
        assertThat(result2).isTrue();
        assertThat(eventStore.getReceivedEvents()).containsExactly(event1, event2);
    }

    @Test
    void process_suppressesDuplicateEventId() {
        RelayEvent event = new RelayEvent("evt-dup-1", "agg-dup", "UserRegistered", "{}");

        boolean firstAttempt = processor.process(event);
        boolean secondAttempt = processor.process(event);

        assertThat(firstAttempt).isTrue();
        assertThat(secondAttempt).isFalse();
        assertThat(eventStore.getReceivedEvents()).hasSize(1).contains(event);
    }

    @Test
    void process_handlesConcurrentDuplicateAttempts_usingBarrierContention() throws Exception {
        int threadCount = 20;
        String sharedEventId = "evt-concurrent-" + UUID.randomUUID();
        RelayEvent event = new RelayEvent(sharedEventId, "agg-concurrent", "PaymentProcessed", "{}");

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return processor.process(event);
            }));
        }

        int trueResultsCount = 0;
        int falseResultsCount = 0;

        for (Future<Boolean> future : futures) {
            if (future.get(5, TimeUnit.SECONDS)) {
                trueResultsCount++;
            } else {
                falseResultsCount++;
            }
        }

        executor.shutdown();

        assertThat(trueResultsCount).as("Exactly one thread must successfully process the event").isEqualTo(1);
        assertThat(falseResultsCount).as("All other 19 threads must receive false for duplicate suppression").isEqualTo(19);
        assertThat(eventStore.getReceivedEvents()).hasSize(1).contains(event);
    }

    @Test
    void process_releasesClaimOnFailure_allowingSubsequentAttemptToSucceed() {
        String eventId = "evt-fail-" + UUID.randomUUID();
        RelayEvent event = new RelayEvent(eventId, "agg-fail", "FailingEvent", "{}");

        AtomicInteger attemptCounter = new AtomicInteger(0);
        EventStore failingStore = new EventStore() {
            @Override
            public void record(RelayEvent eventToRecord) {
                if (attemptCounter.incrementAndGet() == 1) {
                    throw new RuntimeException("Simulated transient storage failure");
                }
                super.record(eventToRecord);
            }
        };

        IdempotentEventProcessor failingProcessor = new IdempotentEventProcessor(failingStore);

        // First attempt throws exception
        assertThatThrownBy(() -> failingProcessor.process(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Simulated transient storage failure");

        // Verify state was released and second attempt succeeds
        boolean secondAttemptResult = failingProcessor.process(event);

        assertThat(secondAttemptResult).isTrue();
        assertThat(failingStore.getReceivedEvents()).hasSize(1).contains(event);
        assertThat(attemptCounter.get()).isEqualTo(2);
    }

    @Test
    void process_handlesConcurrentFailureRecovery_whereInFlightWaiterClaimsReleasedEvent() throws Exception {
        String eventId = "evt-race-fail-" + UUID.randomUUID();
        RelayEvent event = new RelayEvent(eventId, "agg-race", "RaceFailureEvent", "{}");

        CountDownLatch firstAttemptStartedLatch = new CountDownLatch(1);
        CountDownLatch allowFirstAttemptToFailLatch = new CountDownLatch(1);
        AtomicInteger storeRecordCalls = new AtomicInteger(0);

        EventStore failingStore = new EventStore() {
            @Override
            public void record(RelayEvent eventToRecord) {
                int callCount = storeRecordCalls.incrementAndGet();
                if (callCount == 1) {
                    firstAttemptStartedLatch.countDown();
                    try {
                        allowFirstAttemptToFailLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException("Attempt 1 failed");
                }
                super.record(eventToRecord);
            }
        };

        IdempotentEventProcessor raceProcessor = new IdempotentEventProcessor(failingStore);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Thread 1: Claims claim first, then pauses before failing
        Future<?> thread1Future = executor.submit(() -> {
            raceProcessor.process(event);
        });

        // Wait until Thread 1 has claimed the eventId and entered record()
        assertThat(firstAttemptStartedLatch.await(5, TimeUnit.SECONDS)).isTrue();

        // Thread 2: Starts processing while Thread 1 is in-flight, so it waits on Thread 1's future
        Future<Boolean> thread2Future = executor.submit(() -> raceProcessor.process(event));

        // Unblock Thread 1 to fail
        allowFirstAttemptToFailLatch.countDown();

        // Thread 1 fails
        assertThatThrownBy(() -> {
            try {
                thread1Future.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }).isInstanceOf(RuntimeException.class).hasMessage("Attempt 1 failed");

        // Thread 2 catches the failure of Thread 1, retries, claims the released eventId, and succeeds!
        Boolean thread2Result = thread2Future.get(5, TimeUnit.SECONDS);

        executor.shutdown();

        assertThat(thread2Result).isTrue();
        assertThat(failingStore.getReceivedEvents()).hasSize(1).contains(event);
        assertThat(storeRecordCalls.get()).isEqualTo(2);
    }
}
