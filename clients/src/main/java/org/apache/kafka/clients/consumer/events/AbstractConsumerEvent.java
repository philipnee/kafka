package org.apache.kafka.clients.consumer.events;

public class AbstractConsumerEvent {
    private final ConsumerEventType eventType;
    private boolean requireCoordinator = false;

    public AbstractConsumerEvent(ConsumerEventType eventType, boolean requireCoordinator) {
        this.eventType = eventType;
        this.requireCoordinator = requireCoordinator;
    }

    public ConsumerEventType getEventType() { return eventType; }

    public boolean isRequireCoordinator() { return requireCoordinator; }

    @Override

    public String toString() {
            return eventType.toString(); // TODO: need a better toString method
        }
}
