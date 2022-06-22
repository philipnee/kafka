package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.common.utils.Time;

public class FetchEvent extends KafkaServerEvent{
    final Time time;
    final boolean includeMetadataInTimeout;

    public FetchEvent(Time time, boolean includeMetadataInTimeout) {
        super(KafkaServerEventType.FETCH, true);
        this.time = time;
        this.includeMetadataInTimeout = includeMetadataInTimeout;
    }
}
