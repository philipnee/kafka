package org.apache.kafka.clients.consumer.events;

public class InitializationEvent extends KafkaServerEvent {

    public InitializationEvent() {
        super(KafkaServerEventType.NOOP, false);
    }
}
