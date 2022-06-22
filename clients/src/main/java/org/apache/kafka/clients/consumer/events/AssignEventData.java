package org.apache.kafka.clients.consumer.events;

import org.apache.kafka.common.utils.Time;

public class AssignEventData extends AbstractKafkaServerEventData {
    public AssignEventData(Time time) {
        super(time.milliseconds(), ServerEventType.ASSIGN);
    }
}
