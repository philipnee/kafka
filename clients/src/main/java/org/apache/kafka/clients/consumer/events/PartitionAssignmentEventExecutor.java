package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.AsyncConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.common.utils.Time;

import java.util.HashSet;
import java.util.Optional;

public class PartitionAssignmentEventExecutor<K, V> extends ServerEventExecutor {
    private final Metadata metadata;
    private final Fetcher<K, V> fetcher;
    private final AsyncConsumerCoordinator coordinator;
    private final Time time;

    private PartitionAssignmentServerEvent event;

    public PartitionAssignmentEventExecutor(Time time,
                                            Metadata metadata,
                                            Fetcher<K, V> fetcher,
                                            AsyncConsumerCoordinator coordinator) {
        this.metadata = metadata;
        this.fetcher = fetcher;
        this.coordinator = coordinator;
        this.time = time;
    }

    @Override
    public Void call() throws Exception {
        System.out.println("executing assign");
        this.event = (PartitionAssignmentServerEvent) this.serverEvent;
        fetcher.clearBufferedDataForUnassignedPartitions(event.getPartitions());
        if(coordinator != null)
            this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

        if(event.shouldUpdateMetadata())
            metadata.requestUpdateForNewTopics();
        System.out.println("done!!");
        return null;
    }
}
