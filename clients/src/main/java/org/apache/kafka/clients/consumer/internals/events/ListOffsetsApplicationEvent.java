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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Event for retrieving partition offsets by performing a
 * {@link org.apache.kafka.common.requests.ListOffsetsRequest ListOffsetsRequest}.
 * This event is created with a map of {@link TopicPartition} and target timestamps to search
 * offsets for. It is completed with a map of {@link TopicPartition} and the
 * {@link OffsetAndTimestamp} found (offset of the first message whose timestamp is greater than
 * or equals to the target timestamp)
 */
public class ListOffsetsApplicationEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndTimestamp>> {
    private final CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future;

    final Map<TopicPartition, Long> timestampsToSearch;
    final boolean requireTimestamps;

    public ListOffsetsApplicationEvent(Map<TopicPartition, Long> timestampToSearch, boolean requireTimestamps) {
        super(Type.LIST_OFFSETS);
        this.timestampsToSearch = timestampToSearch;
        this.requireTimestamps = requireTimestamps;
        this.future = new CompletableFuture<>();
    }

    public CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future() {
        return future;
    }

    @Override
    public String toString() {
        return "ListOffsetsApplicationEvent {" +
                "timestampsToSearch=" + timestampsToSearch + ", " +
                "requireTimestamps=" + requireTimestamps + '}';
    }
}
