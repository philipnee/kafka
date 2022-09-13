package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.common.utils.Time;

public class FetchEvent extends ServerEvent {
    final Time time;
    final boolean includeMetadataInTimeout;

    public FetchEvent(Time time, boolean includeMetadataInTimeout) {
        super(ServerEventType.FETCH, true);
        this.time = time;
        this.includeMetadataInTimeout = includeMetadataInTimeout;
    }
}
