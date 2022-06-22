package org.apache.kafka.clients.consumer.events;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public interface EventExecutor extends Callable<Void> {
    // TODO: Should be KafkaConsumerEvents
}
