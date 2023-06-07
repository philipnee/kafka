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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This tests the {@link FetchCollector} functionality in addition to what {@link FetcherTest} tests during the course
 * of its tests.
 */
public class FetchCollectorTest {

    private final Time time = new MockTime(0, 0, 0);
    private final TopicPartition topicAPartition0 = new TopicPartition("topic-a", 0);
    private final TopicPartition topicAPartition1 = new TopicPartition("topic-a", 1);
    private final TopicPartition topicAPartition2 = new TopicPartition("topic-a", 2);
    private final Set<TopicPartition> allPartitions = partitions(topicAPartition0, topicAPartition1, topicAPartition2);
    private LogContext logContext;

    private SubscriptionState subscriptions;

    private FetchConfig<String, String> fetchConfig;

    private FetchMetricsManager metricsManager;

    private ConsumerMetadata metadata;

    private FetchBuffer<String, String> fetchBuffer;

    private FetchCollector<String, String> fetchCollector;

    @Test
    public void testFetchNormal() {
        int recordCount = 1000;
        buildDependencies(recordCount);
        assignAndSeek(topicAPartition0);

        CompletedFetch<String, String> completedFetch = completedFetch(recordCount);

        // Validate that the buffer is empty until after we add the fetch data.
        assertTrue(fetchBuffer.isEmpty());
        fetchBuffer.add(completedFetch);
        assertFalse(fetchBuffer.isEmpty());

        // Validate that the completed fetch isn't initialized just because we add it to the buffer.
        assertFalse(completedFetch.isInitialized());

        // Fetch the data and validate that we get all the records we want back.
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
        assertFalse(fetch.isEmpty());
        assertEquals(recordCount, fetch.numRecords());

        // When we collected the data from the buffer, this will cause the completed fetch to get initialized.
        assertTrue(completedFetch.isInitialized());

        // However, even though we've collected the data, it isn't (completely) consumed yet.
        assertFalse(completedFetch.isConsumed());

        // The buffer is now considered "empty" because our queue is empty.
        assertTrue(fetchBuffer.isEmpty());
        assertNull(fetchBuffer.peek());
        assertNull(fetchBuffer.poll());

        // However, while the queue is "empty", the next-in-line fetch is actually still in the buffer.
        assertNotNull(fetchBuffer.nextInLineFetch());

        // Validate that the next fetch position has been updated to point to the record after our last fetched
        // record.
        SubscriptionState.FetchPosition position = subscriptions.position(topicAPartition0);
        assertEquals(recordCount, position.offset);

        // Now attempt to collect more records from the fetch buffer.
        fetch = fetchCollector.collectFetch(fetchBuffer);

        // The Fetch object is non-null, but it's empty.
        assertEquals(0, fetch.numRecords());
        assertTrue(fetch.isEmpty());

        // However, once we read *past* the end of the records in the CompletedFetch, then we will call
        // drain on it, and it will be considered all consumed.
        assertTrue(completedFetch.isConsumed());
    }

    @Test
    public void testNoResultsIfInitializing() {
        buildDependencies();

        // Intentionally call assign (vs. assignAndSeek) so that we don't set the position. The SubscriptionState
        // will consider the partition as in the SubscriptionState.FetchStates.INITIALIZED state.
        assign(topicAPartition0);

        // The position should thus be null and considered un-fetchable and invalid.
        assertNull(subscriptions.position(topicAPartition0));
        assertFalse(subscriptions.isFetchable(topicAPartition0));
        assertFalse(subscriptions.hasValidPosition(topicAPartition0));

        // Add some valid CompletedFetch records to the FetchBuffer queue and collect them into the Fetch.
        fetchBuffer.add(completedFetch());
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);

        // Verify that no records are fetched for the partition as it did not have a valid position set.
        assertEquals(0, fetch.numRecords());
    }

    @ParameterizedTest
    @CsvSource(
            {
                    "10,runtime",
                    "0,runtime",
                    "10,kafka",
                    "0,kafka"
            }
    )
    public void testErrorInInitialize(int numRecords, String exceptionClassName) {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        final Class<? extends RuntimeException> exceptionClass = getInitializeExceptionClass(exceptionClassName);

        // Create a FetchCollector that fails on CompletedFetch initialization.
        fetchCollector = new FetchCollector<String, String>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                time) {

            @Override
            CompletedFetch<String, String> initialize(final CompletedFetch<String, String> completedFetch) {
                return throwInitializeException(exceptionClass);
            }
        };

        // Add the CompletedFetch to the FetchBuffer queue
        fetchBuffer.add(completedFetch(numRecords));

        // At first, the queue is populated
        assertFalse(fetchBuffer.isEmpty());

        // Now run our ill-fated collectFetch.
        assertThrows(exceptionClass, () -> fetchCollector.collectFetch(fetchBuffer));

        // If the number of records in the CompletedFetch was 0, the call to FetchCollector.collectFetch() will
        // remove it from the queue. If there are records in the CompletedFetch, FetchCollector.collectFetch will
        // leave it on the queue.
        assertEquals(numRecords == 0, fetchBuffer.isEmpty());
    }

    @Test
    public void testFetchingPausedPartitionsYieldsNoRecords() {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        // The partition should not be 'paused' in the SubscriptionState until we explicitly tell it to.
        assertFalse(subscriptions.isPaused(topicAPartition0));
        subscriptions.pause(topicAPartition0);
        assertTrue(subscriptions.isPaused(topicAPartition0));

        CompletedFetch<String, String> completedFetch = completedFetch();

        // Set the CompletedFetch to the next-in-line fetch, *not* the queue.
        fetchBuffer.setNextInLineFetch(completedFetch);

        // The next-in-line CompletedFetch should reference the same object that was just created
        assertSame(fetchBuffer.nextInLineFetch(), completedFetch);

        // The FetchBuffer queue should be empty as the CompletedFetch was added to the next-in-line.
        // CompletedFetch, not the queue.
        assertTrue(fetchBuffer.isEmpty());

        // Ensure that the partition for the next-in-line CompletedFetch is still 'paused'.
        assertTrue(subscriptions.isPaused(completedFetch.partition));

        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);

        // There should be no records in the Fetch as the partition being fetched is 'paused'.
        assertEquals(0, fetch.numRecords());

        // The FetchBuffer queue should not be empty; the CompletedFetch is added to the FetchBuffer queue by
        // the FetchCollector when it detects a 'paused' partition.
        assertFalse(fetchBuffer.isEmpty());

        // The next-in-line CompletedFetch should be null; the CompletedFetch is added to the FetchBuffer
        // queue by the FetchCollector when it detects a 'paused' partition.
        assertNull(fetchBuffer.nextInLineFetch());
    }

    @ParameterizedTest
    @MethodSource("handleInitializeErrorSource")
    public void testFetchWithMetadataRefreshErrors(final Errors error) {
        buildDependencies();
        assignAndSeek(topicAPartition0);

        CompletedFetch<String, String> completedFetch = completedFetch(10, error);
        fetchBuffer.add(completedFetch);
        subscriptions.preferredReadReplica(completedFetch.partition, time.milliseconds());
        assertNotNull(subscriptions.preferredReadReplica(completedFetch.partition, time.milliseconds()));
        // Fetch the data and validate that we get all the records we want back.
        Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
        assertTrue(fetch.isEmpty());
        assertTrue(metadata.updateRequested());
        assertEquals(Optional.empty(), subscriptions.preferredReadReplica(completedFetch.partition, time.milliseconds()));
    }

    private CompletedFetch<String, String> completedFetch() {
        return completedFetch(10);
    }

    private CompletedFetch<String, String> completedFetch(int recordCount) {
        return completedFetch(recordCount, null);
    }

    private CompletedFetch<String, String> completedFetch(int count, Errors error) {
        Records records;

        ByteBuffer allocate = ByteBuffer.allocate(1024);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(allocate,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0)) {
            for (int i = 0; i < count; i++)
                builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());

            records = builder.build();
        }

        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(topicAPartition0.partition())
                .setHighWatermark(1000)
                .setRecords(records);

        if (error != null)
            partitionData.setErrorCode(error.code());

        return completedFetch(topicAPartition0, partitionData);
    }

    private CompletedFetch<String, String> completedFetch(TopicPartition tp, FetchResponseData.PartitionData partitionData) {
        FetchMetricsAggregator metricsAggregator = new FetchMetricsAggregator(metricsManager, allPartitions);
        return new CompletedFetch<>(
                logContext,
                subscriptions,
                fetchConfig,
                BufferSupplier.create(),
                tp,
                partitionData,
                metricsAggregator,
                0L,
                ApiKeys.FETCH.latestVersion());
    }

    /**
     * This is a handy utility method for returning a set from a varargs array.
     */
    private static Set<TopicPartition> partitions(TopicPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }

    private void buildDependencies() {
        buildDependencies(ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);
    }

    private void buildDependencies(int maxPollRecords) {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));

        ConsumerConfig config = new ConsumerConfig(p);

        Deserializers<String, String> deserializers = new Deserializers<>(new StringDeserializer(), new StringDeserializer());

        subscriptions = createSubscriptionState(config, logContext);
        fetchConfig = createFetchConfig(config, deserializers);

        Metrics metrics = createMetrics(config, time);
        metricsManager = createFetchMetricsManager(metrics);
        metadata = new ConsumerMetadata(
                0,
                10000,
                false,
                false,
                subscriptions,
                logContext,
                new ClusterResourceListeners());
        fetchCollector = new FetchCollector<>(
                logContext,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                time);
        fetchBuffer = new FetchBuffer<>(logContext);
    }

    private void assign(TopicPartition... partitions) {
        subscriptions.assignFromUser(partitions(partitions));
    }

    private void assignAndSeek(TopicPartition tp) {
        assign(tp);
        subscriptions.seek(tp, 0);
    }

    /**
     * Supplies the {@link Arguments} to {@link #testFetchWithMetadataRefreshErrors(Errors)}.
     */
    private static Stream<Arguments> handleInitializeErrorSource() {
        List<Errors> errors = Arrays.asList(
                Errors.NOT_LEADER_OR_FOLLOWER,
                Errors.REPLICA_NOT_AVAILABLE,
                Errors.KAFKA_STORAGE_ERROR,
                Errors.FENCED_LEADER_EPOCH,
                Errors.OFFSET_NOT_AVAILABLE,
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                Errors.UNKNOWN_TOPIC_ID,
                Errors.INCONSISTENT_TOPIC_ID
        );

        return errors.stream().map(Arguments::of);
    }

    private Class<? extends RuntimeException> getInitializeExceptionClass(String exceptionClassName) {
        if (exceptionClassName.equalsIgnoreCase("runtime"))
            return RuntimeException.class;

        if (exceptionClassName.equalsIgnoreCase("kafka"))
            return KafkaException.class;

        fail("Please provide the correct error type");

        // We won't get to here. This is just to appease the compiler...
        return RuntimeException.class;
    }

    private CompletedFetch<String, String> throwInitializeException(Class<? extends RuntimeException> exceptionClass) {
        if (exceptionClass == RuntimeException.class)
            throw new RuntimeException("Runtime error");

        if (exceptionClass == KafkaException.class)
            throw new KafkaException("Kafka error");

        throw new IllegalArgumentException("Please provide the correct error type");
    }
}
