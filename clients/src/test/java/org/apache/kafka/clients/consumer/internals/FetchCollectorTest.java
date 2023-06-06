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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private FetchCollector<String, String> fetchCollector;

    @Test
    public void testFetchNormal() {
        int recordCount = 1000;
        buildDependencies(recordCount);

        subscriptions.assignFromUser(partitions(topicAPartition0));
        subscriptions.seek(topicAPartition0, 0);

        try (FetchBuffer<String, String> fetchBuffer = new FetchBuffer<>(logContext)) {
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
            // drain on it and it will be considered all consumed.
            assertTrue(completedFetch.isConsumed());
        }
    }

    @Test
    public void testNoResultsIfInitializing() {
        buildDependencies(ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);

        subscriptions.assignFromUser(partitions(topicAPartition0));

        try (FetchBuffer<String, String> fetchBuffer = new FetchBuffer<>(logContext)) {
            fetchBuffer.add(completedFetch(1000));
            Fetch<String, String> fetch = fetchCollector.collectFetch(fetchBuffer);
            assertEquals(0, fetch.numRecords());
        }
    }

    @ParameterizedTest
    @CsvSource(
            {
                    "10,RuntimeException,false",
                    "0,RuntimeException,true",
                    "10,KafkaException,false",
                    "0,KafkaException,true"
            }
    )
    public void testErrorInInitialize(int numRecords,
                                      String exceptionClassName,
                                      boolean shouldFetchQueueBeEmpty) {
        buildDependencies(ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);
        subscriptions.assignFromUser(partitions(topicAPartition0));

        // Create a FetchCollector that fails on CompletedFetch initialization.
        fetchCollector = new FetchCollector<String, String>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                time) {

            @Override
            CompletedFetch<String, String> initialize(final CompletedFetch<String, String> completedFetch) {
                if (exceptionClassName.equalsIgnoreCase("RuntimeException"))
                    throw new RuntimeException("Runtime error");
                else if (exceptionClassName.equalsIgnoreCase("KafkaException"))
                    throw new KafkaException("Kafka error");
                else
                    throw new IllegalArgumentException("Please provide the correct error type");
            }
        };

        try (FetchBuffer<String, String> fetchBuffer = new FetchBuffer<>(logContext)) {
            fetchBuffer.add(completedFetch(numRecords));
            assertFalse(fetchBuffer.isEmpty());

            if (exceptionClassName.equalsIgnoreCase("RuntimeException"))
                assertThrows(RuntimeException.class, () -> fetchCollector.collectFetch(fetchBuffer));
            else if (exceptionClassName.equalsIgnoreCase("KafkaException"))
                assertThrows(KafkaException.class, () -> fetchCollector.collectFetch(fetchBuffer));

            assertEquals(shouldFetchQueueBeEmpty, fetchBuffer.isEmpty());
        }
    }

    private CompletedFetch<String, String> completedFetch(int count) {
        MemoryRecords records = records(0, count, 0);
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(topicAPartition0.partition())
                .setHighWatermark(1000)
                .setRecords(records);
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

    private MemoryRecords records(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
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
    }
}
