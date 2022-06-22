package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import java.util.Collection;

public class PartitionAssignmentServerEvent extends KafkaServerEvent{
    private final Collection<TopicPartition> partitions;
    private final boolean shouldUpdateMetadata;

    public PartitionAssignmentServerEvent(Collection<TopicPartition> partitions, boolean shouldUpdateMetadata) {
        super(KafkaServerEventType.ASSIGN, true);
        this.partitions = partitions;
        this.shouldUpdateMetadata = shouldUpdateMetadata;
    }

    public Collection<TopicPartition> getPartitions() {
        return partitions;
    }

    public boolean shouldUpdateMetadata() {
        return this.shouldUpdateMetadata;
    }
}

