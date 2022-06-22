package org.apache.kafka.clients.consumer.events;

public class NoopEventExecutor extends ServerEventExecutor {
    @Override
    public Void call() throws Exception {
        System.out.println("do nothing");
        return null;
    }
}
