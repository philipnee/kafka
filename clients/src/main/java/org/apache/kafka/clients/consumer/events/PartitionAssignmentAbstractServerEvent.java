package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class PartitionAssignmentAbstractServerEvent extends AbstractServerEvent {
    private final Collection<TopicPartition> partitions;
    private final boolean shouldUpdateMetadata;

    public PartitionAssignmentAbstractServerEvent(Collection<TopicPartition> partitions, boolean shouldUpdateMetadata) {
        super(ServerEventType.ASSIGN, true);
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

