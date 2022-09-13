package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.ConsumerAsyncCoordinator;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.common.utils.Time;

public class PartitionAssignmentEventExecutor<K, V> extends ServerEventExecutor {
    private final Metadata metadata;
    private final Fetcher<K, V> fetcher;
    private final ConsumerAsyncCoordinator coordinator;
    private final Time time;

    private PartitionAssignmentAbstractServerEvent event;

    public PartitionAssignmentEventExecutor(Time time,
                                            Metadata metadata,
                                            Fetcher<K, V> fetcher,
                                            ConsumerAsyncCoordinator coordinator) {
        this.metadata = metadata;
        this.fetcher = fetcher;
        this.coordinator = coordinator;
        this.time = time;
    }

    @Override
    public Void call() throws Exception {
        System.out.println("executing assign");
        this.event = (PartitionAssignmentAbstractServerEvent) this.serverEvent;
        fetcher.clearBufferedDataForUnassignedPartitions(event.getPartitions());
        if(coordinator != null)
            this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

        if(event.shouldUpdateMetadata())
            metadata.requestUpdateForNewTopics();
        System.out.println("done!!");
        return null;
    }
}
