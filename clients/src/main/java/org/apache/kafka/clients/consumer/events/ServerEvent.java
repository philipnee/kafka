package org.apache.kafka.clients.consumer.events;

public interface ServerEvent {
    public boolean isRequireCoordinator();

    ServerEventType getEventType();
}
