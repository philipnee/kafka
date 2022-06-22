package org.apache.kafka.clients.consumer.internals;

public enum BackgroundStates {
    DOWN,
    INITIALIZED,
    COORDINATOR_DISCOVERY,
    STABLE,
}
