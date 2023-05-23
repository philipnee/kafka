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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;

public class CommitApplicationEvent extends CompletableApplicationEvent<Void> {

    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public CommitApplicationEvent(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        super(Type.COMMIT);
        this.offsets = Collections.unmodifiableMap(offsets);

        for (OffsetAndMetadata offsetAndMetadata : offsets.values()) {
            if (offsetAndMetadata.offset() < 0) {
                throw new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset());
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    @Override
    public String toString() {
        return "CommitApplicationEvent{" +
                "offsets=" + offsets +
                ", future=" + future +
                ", type=" + type +
                '}';
    }
}