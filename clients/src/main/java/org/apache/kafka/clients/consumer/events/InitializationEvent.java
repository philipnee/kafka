package org.apache.kafka.clients.consumer.events;

public class InitializationEvent extends ServerEvent {

    public InitializationEvent() {
        super(ServerEventType.NOOP, false);
    }
}
