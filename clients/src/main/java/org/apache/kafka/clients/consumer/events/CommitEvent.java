package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class CommitEvent extends KafkaServerEvent{
    private boolean isAsync;
    final Map<TopicPartition, OffsetAndMetadata> consumedOffsets;

    public CommitEvent(final Map<TopicPartition, OffsetAndMetadata> offsets, boolean isAsync) {
        super(KafkaServerEventType.COMMIT, false);
        this.isAsync = isAsync;
        this.consumedOffsets = offsets;
    }
}
