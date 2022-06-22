/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples2;

import kafka.examples.KafkaProperties;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.NewKafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Consumer extends ShutdownableThread {
    private final NewKafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final String groupId;
    private final int numMessageToConsume;
    private int messageRemaining;
    private final CountDownLatch latch;

    public Consumer(final String topic,
                    final String groupId,
                    final Optional<String> instanceId,
                    final boolean readCommitted,
                    final int numMessageToConsume,
                    final CountDownLatch latch) throws InterruptedException {
        super("KafkaConsumerExample", false);
        this.groupId = groupId;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        if (readCommitted) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new NewKafkaConsumer<>(props);
        this.topic = topic;
        this.numMessageToConsume = numMessageToConsume;
        this.messageRemaining = numMessageToConsume;
        this.latch = latch;
        TopicPartition tp1 = new TopicPartition("topic-test", 0);
        consumer.assign(Collections.singletonList(tp1));
    }

    NewKafkaConsumer<Integer, String> get() {
        return consumer;
    }

    @Override
    public void doWork() {
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }

    public void close() {
        this.consumer.close();
    }
}
