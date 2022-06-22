package org.apache.kafka.clients.consumer.channel;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.events.KafkaServerEvent;

import java.util.Optional;
import java.util.concurrent.*;

public class KafkaServerEventQueue {
    private static final int timeoutMs = 500; // TODO: get this from the config
    private LinkedBlockingDeque<KafkaServerEvent> queue;
    private final ConsumerConfig config;

    public KafkaServerEventQueue(final ConsumerConfig config) {
        this.queue = new LinkedBlockingDeque<>();
        this.config = config;
    }

    public Optional<KafkaServerEvent> poll() throws InterruptedException {
        return Optional.ofNullable(queue.poll(timeoutMs, TimeUnit.MILLISECONDS));
    }
    public Optional<KafkaServerEvent> peek() throws InterruptedException {
        Optional<KafkaServerEvent> event = Optional.ofNullable(queue.poll(timeoutMs, TimeUnit.MILLISECONDS));
        if(event.isPresent())
            queue.addFirst(event.get());
        return event;
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public boolean enqueue(KafkaServerEvent event) {
        return queue.offer(event);
    }
}

