package org.apache.kafka.clients.consumer.events;

abstract public class KafkaServerEvent {
    private final KafkaServerEventType eventType;
    private boolean requireCoordinator = false;

    public KafkaServerEvent(KafkaServerEventType eventType, boolean requireCoordinator) {
       this.eventType = eventType;
       this.requireCoordinator = requireCoordinator;
    }

    public KafkaServerEventType getEventType() { return eventType; }

    public boolean isRequireCoordinator() { return requireCoordinator; }

    @Override
    public String toString() {
        return eventType.toString(); // TODO: need a better toString method
    }
}
