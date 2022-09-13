package org.apache.kafka.clients.consumer.events;

abstract public class AbstractServerEvent {
    private final ServerEventType eventType;
    private boolean requireCoordinator = false;

    public AbstractServerEvent(ServerEventType eventType, boolean requireCoordinator) {
       this.eventType = eventType;
       this.requireCoordinator = requireCoordinator;
    }

    public ServerEventType getEventType() { return eventType; }

    public boolean isRequireCoordinator() { return requireCoordinator; }

    @Override
    public String toString() {
        return eventType.toString(); // TODO: need a better toString method
    }
}
