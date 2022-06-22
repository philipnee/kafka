package org.apache.kafka.clients.consumer.events;

abstract public class AbstractKafkaServerEventData {
    private long timestampMs;
    private KafkaServerEventType type;
    public AbstractKafkaServerEventData(long time, KafkaServerEventType type) {
        this.timestampMs = time;
        this.type = type;
    }
}
