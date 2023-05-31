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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

/**
 * {@code FetchBuffer} buffers up the results from the broker responses as they are received. It is essentially a
 * wrapper around a {@link java.util.Queue} of {@link CompletedFetch}.
 *
 * <p/>
 *
 * <em>Note</em>: this class is not thread-safe and is intended to only be used from a single thread.
 *
 * @param <K> Record key type
 * @param <V> Record value type
 */
public class FetchBuffer<K, V> implements Closeable {

    private final Logger log;
    private final ConcurrentLinkedQueue<CompletedFetch<K, V>> completedFetches;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();

    private CompletedFetch<K, V> nextInLineFetch;

    public FetchBuffer(final LogContext logContext) {
        this.log = logContext.logger(FetchBuffer.class);
        this.completedFetches = new ConcurrentLinkedQueue<>();
    }

    /**
     * Returns {@code true} if there are no completed fetches pending to return to the user.
     *
     * @return {@code true} if the buffer is empty, {@code false} otherwise
     */
    boolean isEmpty() {
        return completedFetches.isEmpty();
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return {@code true} if there are completed fetches that match the {@link Predicate}, {@code false} otherwise
     */
    boolean hasCompletedFetches(Predicate<CompletedFetch<K, V>> predicate) {
        return completedFetches.stream().anyMatch(predicate);
    }

    void add(CompletedFetch<K, V> completedFetch) {
        completedFetches.add(completedFetch);
    }

    void addAll(Collection<CompletedFetch<K, V>> completedFetches) {
        this.completedFetches.addAll(completedFetches);
    }

    CompletedFetch<K, V> nextInLineFetch() {
        return nextInLineFetch;
    }

    void setNextInLineFetch(CompletedFetch<K, V> completedFetch) {
        this.nextInLineFetch = completedFetch;
    }

    CompletedFetch<K, V> peek() {
        return completedFetches.peek();
    }

    CompletedFetch<K, V> poll() {
        return completedFetches.poll();
    }

    /**
     * Updates the buffer to retain only the fetch data that corresponds to the given partitions. Any previously
     * {@link CompletedFetch fetched data} is removed if its partition is not in the given set of partitions.
     *
     * @param partitions {@link Set} of {@link TopicPartition}s for which any buffered data should be kept
     */
    void retainAll(final Set<TopicPartition> partitions) {
        final Iterator<CompletedFetch<K, V>> completedFetchesItr = completedFetches.iterator();

        while (completedFetchesItr.hasNext()) {
            final CompletedFetch<K, V> completedFetch = completedFetchesItr.next();

            if (!partitions.contains(completedFetch.partition)) {
                log.debug("Removing {} from buffered fetch data as it is not in the set of partitions to retain", completedFetch.partition);
                completedFetch.drain();
                completedFetchesItr.remove();
            }
        }

        if (nextInLineFetch != null && !partitions.contains(nextInLineFetch.partition)) {
            log.debug("Removing {} from buffered fetch data as it is not in the set of partitions to retain", nextInLineFetch.partition);
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }
    }

    Set<TopicPartition> partitions() {
        Set<TopicPartition> partitions = new HashSet<>();

        if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
            partitions.add(nextInLineFetch.partition);
        }

        for (CompletedFetch<K, V> completedFetch : completedFetches) {
            partitions.add(completedFetch.partition);
        }

        return partitions;
    }

    @Override
    public void close() {
        idempotentCloser.close(() -> {
            log.debug("Closing the fetch buffer");

            if (nextInLineFetch != null) {
                nextInLineFetch.drain();
                nextInLineFetch = null;
            }
        }, () -> log.warn("The fetch buffer was previously closed"));
    }
}
