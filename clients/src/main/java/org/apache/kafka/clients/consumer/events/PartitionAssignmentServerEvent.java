package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class PartitionAssignmentServerEvent extends ServerEvent {
    private final Collection<TopicPartition> partitions;
    private final boolean shouldUpdateMetadata;

    public PartitionAssignmentServerEvent(Collection<TopicPartition> partitions, boolean shouldUpdateMetadata) {
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

