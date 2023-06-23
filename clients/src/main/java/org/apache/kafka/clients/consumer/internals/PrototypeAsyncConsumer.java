/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.MetadataUpdateApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchEvent;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredIsolationLevel;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;
import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * This prototype consumer uses the EventHandler to process application
 * events so that the network IO can be processed in a background thread. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor" >this document</a>
 * for detail implementation.
 */
public class PrototypeAsyncConsumer<K, V> implements Consumer<K, V> {
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    private final Metrics metrics;
    private final LogContext logContext;
    private final EventHandler eventHandler;
    private final Time time;
    private final Optional<String> groupId;
    private final Logger log;
    private final SubscriptionState subscriptions;
    private final long defaultApiTimeoutMs;
    private final ConsumerMetadata metadata;
    private final long retryBackoffMs;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;
    private final FetchBuffer<K, V> fetchBuffer;
    private final FetchCollector<K, V> fetchCollector;
    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;

    public PrototypeAsyncConsumer(final Properties properties,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    public PrototypeAsyncConsumer(final Map<String, Object> configs,
                                  final Deserializer<K> keyDeser,
                                  final Deserializer<V> valDeser) {
        this(new ConsumerConfig(appendDeserializerToConfig(configs, keyDeser, valDeser)), keyDeser, valDeser);
    }

    public PrototypeAsyncConsumer(final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this(Time.SYSTEM, config, keyDeserializer, valueDeserializer);
    }


    public PrototypeAsyncConsumer(final Time time,
                                  final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this.time = time;
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.logContext = createLogContext(config, groupRebalanceConfig);
        this.log = logContext.logger(getClass());
        Deserializers<K, V> deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
        this.subscriptions = createSubscriptionState(config, logContext);
        this.metrics = createMetrics(config, time);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

        List<ConsumerInterceptor<K, V>> interceptorList = getConfiguredConsumerInterceptors(config);
        this.interceptors = new ConsumerInterceptors<>(interceptorList);
        ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                interceptorList,
                Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
        this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
        // Bootstrap the metadata with the bootstrap server IP address, which will be used once for the subsequent
        // metadata refresh once the background thread has started up.
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
        metadata.bootstrap(addresses);

        final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();

        FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
        this.isolationLevel = getConfiguredIsolationLevel(config);

        ApiVersions apiVersions = new ApiVersions();
        final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(time,
                logContext,
                metadata,
                config,
                apiVersions,
                metrics,
                fetchMetricsManager);
        final Supplier<RequestManagers<String, String>> requestManagersSupplier = RequestManagers.supplier(time,
                logContext,
                backgroundEventQueue,
                metadata,
                subscriptions,
                config,
                groupRebalanceConfig,
                apiVersions,
                fetchMetricsManager,
                networkClientDelegateSupplier);
        final Supplier<ApplicationEventProcessor<String, String>> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(logContext,
                metadata,
                backgroundEventQueue,
                requestManagersSupplier);
        this.eventHandler = new DefaultEventHandler<>(time,
                logContext,
                applicationEventQueue,
                backgroundEventQueue,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier);

        // These are specific to the foreground thread
        FetchConfig<K, V> fetchConfig = createFetchConfig(config, deserializers);
        this.fetchBuffer = new FetchBuffer<>(logContext);
        this.fetchCollector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                fetchMetricsManager,
                time);
    }

    public PrototypeAsyncConsumer(LogContext logContext,
                                  Time time,
                                  EventHandler eventHandler,
                                  Metrics metrics,
                                  Optional<String> groupId,
                                  SubscriptionState subscriptions,
                                  long defaultApiTimeoutMs,
                                  ConsumerMetadata metadata,
                                  long retryBackoffMs,
                                  ConsumerInterceptors<K, V> interceptors,
                                  FetchBuffer<K, V> fetchBuffer,
                                  FetchCollector<K, V> fetchCollector) {
        this.logContext = logContext;
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.time = time;
        this.metrics = metrics;
        this.groupId = groupId;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.eventHandler = eventHandler;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.interceptors = interceptors;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.fetchBuffer = fetchBuffer;
        this.fetchCollector = fetchCollector;
    }

    /**
     * poll implementation using {@link EventHandler}.
     *  1. Poll for background events. If there's a fetch response event, process the record and return it. If it is
     *  another type of event, process it.
     *  2. Send fetches if needed.
     *  If the timeout expires, return an empty ConsumerRecord.
     *
     * @param timeout timeout of the poll loop
     * @return ConsumerRecord.  It can be empty if time timeout expires.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
            throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
        }

        try {
            Timer timer = time.timer(timeout);

            do {
                final Fetch<K, V> fetch = pollForFetches(timer);

                if (!fetch.isEmpty()) {
                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }

                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
                }
                // We will wait for retryBackoffMs
            } while (timer.notExpired());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return ConsumerRecords.empty();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * This method sends a commit event to the EventHandler and return.
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        CompletableFuture<Void> future = commit(offsets);
        final OffsetCommitCallback commitCallback = callback == null ? new DefaultOffsetCommitCallback() : callback;
        future.whenComplete((r, t) -> {
            if (t != null) {
                commitCallback.onComplete(offsets, new KafkaException(t));
            } else {
                commitCallback.onComplete(offsets, null);
            }
        }).exceptionally(e -> {
            System.out.println(e);
            throw new KafkaException(e);
        });
    }

    // Visible for testing
    CompletableFuture<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        maybeThrowInvalidGroupIdException();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offsets);
        eventHandler.add(commitEvent);
        return commitEvent.future();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        log.info("Seeking to offset {} for partition {}", offset, partition);
        SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                offset,
                Optional.empty(), // This will ensure we skip validation
                this.metadata.currentLeader(partition));
        this.subscriptions.seekUnvalidated(partition, newPosition);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        if (!this.subscriptions.isAssigned(partition))
            throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

        Timer timer = time.timer(timeout);
        do {
            SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
            if (position != null)
                return position.offset;

            updateFetchPositions(timer);
        } while (timer.notExpired());

        throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                "for partition " + partition + " could be determined");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        maybeThrowInvalidGroupIdException();
        if (partitions.isEmpty()) {
            return new HashMap<>();
        }

        return eventHandler.addAndGet(new OffsetFetchApplicationEvent(partitions), time.timer(timeout));
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent() || groupId.get().isEmpty()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        Cluster cluster = this.metadata.fetch();
        List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
        if (!parts.isEmpty())
            return parts;

        Map<String, List<PartitionInfo>> partitionInfo = eventHandler.addAndGet(new TopicMetadataApplicationEvent(Optional.of(topic)), time.timer(timeout));
        return partitionInfo.getOrDefault(topic, Collections.emptyList());
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return eventHandler.addAndGet(new TopicMetadataApplicationEvent(Optional.empty()), time.timer(timeout));
    }

    @Override
    public Set<TopicPartition> paused() {
        return Collections.unmodifiableSet(subscriptions.pausedPartitions());
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        log.debug("Pausing partitions {}", partitions);
        for (TopicPartition partition: partitions) {
            subscriptions.pause(partition);
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        log.debug("Resuming partitions {}", partitions);
        for (TopicPartition partition: partitions) {
            subscriptions.resume(partition);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");

        if (timestampsToSearch.isEmpty()) {
            return Collections.emptyMap();
        }
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampsToSearch,
                true);

        // If timeout is set to zero return empty immediately; otherwise try to get the results
        // and throw timeout exception if it cannot complete in time.
        if (timeout.toMillis() == 0L)
            return listOffsetsEvent.emptyResult();

        return eventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(partitions, "Partitions cannot be null");

        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, Long> timestampToSearch =
                partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> timestamp));
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampToSearch,
                false);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                eventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
        return offsetAndTimestampMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
                e -> e.getValue().offset()));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

        // if the log end offset is not known and hence cannot return lag and there is
        // no in-flight list offset requested yet,
        // issue a list offset request for that partition so that next time
        // we may get the answer; we do not need to wait for the return value
        // since we would not try to poll the network client synchronously
        if (lag == null) {
            if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                    !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                subscriptions.requestPartitionEndOffset(topicPartition);
                beginningOrEndOffset(Collections.singleton(topicPartition), ListOffsetsRequest.LATEST_TIMESTAMP, Duration.ofMillis(0));
            }

            return OptionalLong.empty();
        }

        return OptionalLong.of(lag);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void enforceRebalance() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void enforceRebalance(String reason) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @Override
    public void close(Duration timeout) {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        closeQuietly(this.eventHandler, "event handler", firstException);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    @Override
    public void wakeup() {
    }

    /**
     * This method sends a commit event to the EventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    @Override
    public void commitSync(final Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        maybeThrowInvalidGroupIdException();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offsets);
        eventHandler.addAndGet(commitEvent, time.timer(timeout));
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
    }

    /**
     * Get the current subscription.  or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        return Collections.unmodifiableSet(this.subscriptions.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
        }

        if (partitions.isEmpty()) {
            this.unsubscribe();
            return;
        }

        for (TopicPartition tp : partitions) {
            String topic = (tp != null) ? tp.topic() : null;
            if (isBlank(topic))
                throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
        }

        // Clear the buffered data which are not a part of newly assigned topics
        final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (partitions.contains(tp))
                currentTopicPartitions.add(tp);
        }

        fetchBuffer.retainAll(currentTopicPartitions);

        // make sure the offsets of topic partitions the consumer is unsubscribing from
        // are committed since there will be no following rebalance
        commit(subscriptions.allConsumed());

        log.info("Assigned to partition(s): {}", join(partitions, ", "));
        if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
           updateMetadata(time.milliseconds());
    }

    private void updateMetadata(long milliseconds) {
        final MetadataUpdateApplicationEvent event = new MetadataUpdateApplicationEvent(milliseconds);
        eventHandler.add(event);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void subscribe(Pattern pattern) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void unsubscribe() {
        fetchBuffer.retainAll(Collections.emptySet());
        eventHandler.add(new UnsubscribeApplicationEvent());
        this.subscriptions.unsubscribe();
    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(Duration.ofMillis(timeoutMs));
    }

    private void sendFetches() {
        if (!eventHandler.isEmpty()) {
            final Optional<BackgroundEvent> backgroundEvent = eventHandler.poll();
            // processEvent() may process 3 types of event:
            // 1. Errors
            // 2. Callback Invocation
            // 3. Fetch responses
            // Errors will be handled or rethrown.
            // Callback invocation will trigger callback function execution, which is blocking until completion.
            // Successful fetch responses will be added to the completedFetches in the fetcher, which will then
            // be processed in the collectFetches().
            backgroundEvent.ifPresent(event -> log.warn("Do something with this background event: {}", event));
        }

        FetchEvent<K, V> event = new FetchEvent<>();
        eventHandler.add(event);

        event.future().whenComplete((completedFetches, error) -> {
            if (completedFetches != null && !completedFetches.isEmpty()) {
                fetchBuffer.addAll(completedFetches);
            }
        });
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private Fetch<K, V> pollForFetches(Timer timer) {
        long pollTimeout = timer.remainingMs();

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // send any new fetches (won't resend pending fetches)
        sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        Queue<CompletedFetch<K, V>> completedFetches = eventHandler.addAndGet(new FetchEvent<>(), pollTimer);
        if (completedFetches != null && !completedFetches.isEmpty()) {
            fetchBuffer.addAll(completedFetches);
        }
        timer.update(pollTimer.currentTimeMs());

        return fetchCollector.collectFetch(fetchBuffer);
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     * @return true iff the operation completed without timing out
     */
    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        ResetPositionsApplicationEvent event = new ResetPositionsApplicationEvent();
        eventHandler.add(event);
        event.get(timer);

        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to look up and update the positions of any
        // partitions which are awaiting reset.
        eventHandler.add(event);

        return true;
    }

    // This is here temporary as we don't have public access to the ConsumerConfig in this module.
    public static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                 Deserializer<?> keyDeserializer,
                                                                 Deserializer<?> valueDeserializer) {
        // validate deserializer configuration, if the passed deserializer instance is null, the user must explicitly set a valid deserializer configuration value
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null)
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(KEY_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        return newConfigs;
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }
}
