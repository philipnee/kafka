package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Fetch;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchEventExecutor<K, V> extends ServerEventExecutor {
    private final Time time;
    private final Timer timer;
    private final ConsumerCoordinator coordinator;
    private final Fetcher fetcher;
    private final Metadata metadata;
    private final ConsumerNetworkClient networkClient;
    private final SubscriptionState subscriptionState;
    private final Logger log;
    private final long retryBackoffMs;
    private FetchEventAbstract event;
    private boolean cachedSubscriptionHasAllFetchPositions;

    public FetchEventExecutor(Time time,
                              Timer timer,
                              ConsumerCoordinator coordinator,
                              Fetcher fetcher,
                              Metadata metadata,
                              ConsumerNetworkClient networkClient,
                              SubscriptionState subscriptionState,
                              boolean cachedSubscriptionHasAllFetchPositions,
                              Logger log,
                              long retryBackoffMs) {
        this.time = time;
        this.timer = timer;
        this.coordinator = coordinator;
        this.fetcher = fetcher;
        this.metadata = metadata;
        this.networkClient = networkClient;
        this.subscriptionState = subscriptionState;
        this.cachedSubscriptionHasAllFetchPositions = cachedSubscriptionHasAllFetchPositions;
        this.log = log;
        this.retryBackoffMs = retryBackoffMs;
    }

    // determine where interceptor is executed
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> doPoll() {
       do {
           networkClient.maybeTriggerWakeup();

           if (event.includeMetadataInTimeout) {
               updateAssignmentMetadataIfNeeded(timer, false);
           }
           while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
               log.warn("Still waiting for metadata");
           }

           final Fetch<K, V> fetch = pollForFetches(timer);
           if (!fetch.isEmpty()) {
               // before returning the fetched records, we can send off the next round of fetches
               // and avoid block waiting for their responses to enable pipelining while the user
               // is handling the fetched records.
               //
               // NOTE: since the consumed position has already been updated, we must not allow
               // wakeups or any other errors to be triggered prior to returning the fetched records.
               if (fetcher.sendFetches() > 0 || networkClient.hasPendingRequests()) {
                   networkClient.transmitSends();
               }
               if (fetch.records().isEmpty()) {
                   log.trace("Returning empty records from `poll()` "
                           + "since the consumer's position has advanced for at least one topic partition");
               }

               return fetch.records();
           }
       } while (this.timer.notExpired());
       return new HashMap<>();
    }

    private Fetch<K, V> pollForFetches(Timer timer) {
        long pollTimeout = coordinator == null ? timer.remainingMs() :
                Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = fetcher.collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // send any new fetches (won't resend pending fetches)
        fetcher.sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        networkClient.poll(pollTimer, () -> {
            // since a fetch might be completed by the background thread, we need this poll condition
            // to ensure that we do not block unnecessarily in poll()
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());

        return fetcher.collectFetch();
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }

        return updateFetchPositions(timer);
    }

    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        fetcher.validateOffsetsIfNeeded();

        cachedSubscriptionHasAllFetchPositions = subscriptionState.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        if (coordinator != null && !coordinator.refreshCommittedOffsetsIfNeeded(timer)) return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        subscriptionState.resetInitializingPositions();

        // Finally send an asynchronous request to lookup and update the positions of any
        // partitions which are awaiting reset.
        fetcher.resetOffsetsIfNeeded();

        return true;
    }

    @Override
    public Void call() throws Exception {
        this.event = (FetchEventAbstract)  this.serverEvent;
        if(this.subscriptionState.hasNoSubscriptionOrUserAssignment()) {
            throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
        }

        try {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = doPoll();
        } catch(IllegalStateException e) {
            //
        }

        return null;
    }
}
