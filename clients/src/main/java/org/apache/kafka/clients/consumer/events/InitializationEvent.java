package org.apache.kafka.clients.consumer.events;

public class InitializationEvent extends AbstractServerEvent implements ServerEvent {

    public InitializationEvent() {
        super(ServerEventType.NOOP, false);
    }
}
