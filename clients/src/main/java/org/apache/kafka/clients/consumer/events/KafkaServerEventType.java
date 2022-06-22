package org.apache.kafka.clients.consumer.events;

public enum KafkaServerEventType {
    NOOP,
    FETCH,
    COMMIT,
    ASSIGN,
}
