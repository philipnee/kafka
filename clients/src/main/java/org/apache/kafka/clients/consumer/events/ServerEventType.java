package org.apache.kafka.clients.consumer.events;

public enum ServerEventType {
    NOOP,
    FETCH,
    COMMIT,
    ASSIGN,
}
