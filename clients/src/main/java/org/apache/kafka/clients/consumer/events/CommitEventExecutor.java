package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;

public class CommitEventExecutor extends ServerEventExecutor {

    private final Time time;
    private final ConsumerCoordinator coordinator;
    private CommitEvent event;

    public CommitEventExecutor(Time time,
                               ConsumerCoordinator coordinator) {
        this.time = time;
        this.coordinator = coordinator;
    }
    @Override
    public Void call() throws Exception {
        this.event = (CommitEvent)  this.serverEvent;
        // TODO: implement callback
        coordinator.commitOffsetsAsync(new HashMap<>(event.consumedOffsets), null);
        return null;
    }
}
