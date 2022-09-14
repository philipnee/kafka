package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CommitEvent extends AbstractServerEvent implements ServerEvent {
    private boolean isAsync;
    final Map<TopicPartition, OffsetAndMetadata> consumedOffsets;
    final
    public CompletableFuture<Void> asyncCommitFuture;

    public CommitEvent(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        super(ServerEventType.COMMIT, false);
        this.consumedOffsets = offsets;
        this.asyncCommitFuture = new CompletableFuture<>();
    }
}
