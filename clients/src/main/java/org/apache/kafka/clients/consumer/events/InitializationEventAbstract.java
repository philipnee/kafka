package org.apache.kafka.clients.consumer.events;

public class InitializationEventAbstract extends AbstractServerEvent {

    public InitializationEventAbstract() {
        super(ServerEventType.NOOP, false);
    }
}
