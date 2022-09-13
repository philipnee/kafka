package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CommitEventAbstract extends AbstractServerEvent {
    private boolean isAsync;
    final Map<TopicPartition, OffsetAndMetadata> consumedOffsets;
    CompletableFuture<Void> asyncCommitFuture;

    public CommitEventAbstract(final Map<TopicPartition, OffsetAndMetadata> offsets, CompletableFuture<Void> future) {
        super(ServerEventType.COMMIT, false);
        this.consumedOffsets = offsets;
        this.asyncCommitFuture = future;
    }
}
