package org.apache.kafka.clients.consumer.channel;

import java.util.concurrent.BlockingQueue;

public class KafkaConsumerEventQueue {
    private BlockingQueue<KafkaConsumerEvent> queue;

    public KafkaConsumerEvent poll() {
        return null;
    }

    public boolean enqueue(KafkaConsumerEvent event) {
        return true;
    }
}

abstract class KafkaConsumerEvent {
    private KafkaConsumerEventType eventType;
    private KafkaConsumerEventData data;
    /*
    For blocking event: timeout > 0
    For non-blocking event: timeout = 0
     */
    private long timeout;
}

abstract class KafkaConsumerEventData {

}

enum KafkaConsumerEventType{
    FETCHED_RECORD,
    REBALANCE,
}