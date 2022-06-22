package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.clients.consumer.channel.KafkaConsumerEventQueue;
import org.apache.kafka.clients.consumer.channel.KafkaServerEventQueue;

import java.util.Optional;
import java.util.concurrent.Callable;

abstract public class ServerEventExecutor implements Callable<Void> {
    KafkaServerEvent serverEvent;

    public void run(KafkaServerEvent event) { //TODO: return type
        try {
            this.serverEvent = event;

            this.call();
        } catch (Exception e) {
            // TODO: 1. return an errorEvent, 2. queue it up to consumerChannel 3. log the error
        } finally {
            // clear the event when done
            this.serverEvent = null;
        }
    }
}