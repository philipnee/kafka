package org.apache.kafka.clients.consumer.events;

abstract public class AbstractKafkaServerEventData {
    private long timestampMs;
    private ServerEventType type;
    public AbstractKafkaServerEventData(long time, ServerEventType type) {
        this.timestampMs = time;
        this.type = type;
    }
}
