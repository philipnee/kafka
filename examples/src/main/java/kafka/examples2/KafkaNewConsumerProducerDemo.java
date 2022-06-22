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

import kafka.examples2.Consumer;
import kafka.examples.KafkaProperties;
import kafka.examples.Producer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TimeoutException;

import javax.sound.midi.SysexMessage;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaNewConsumerProducerDemo {
    public static void main(String[] args) throws Exception {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        CountDownLatch latch = new CountDownLatch(2);
        //Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync, null, false, 10000, -1, latch);
        //producerThread.start();

        createTopic("topic-test", 6);

        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC, "DemoConsumer", Optional.empty(), false, 10000, latch);
        consumerThread.start();

        try {
            latch.await(20, TimeUnit.SECONDS);
            consumerThread.close();
            consumerThread.shutdown();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTopic(String topicName, int numPartitions) throws Exception {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        AdminClient admin = AdminClient.create(config);

        boolean alreadyExists = admin.listTopics().names().get().stream()
                .anyMatch(existingTopicName -> existingTopicName.equals(topicName));
        if (alreadyExists) {
            System.out.printf("topic already exits: %s%n", topicName);
        } else {
            //creating new topic
            System.out.printf("creating topic: %s%n", topicName);
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }

        //describing
        System.out.println("-- describing topic --");
        admin.describeTopics(Collections.singleton(topicName)).allTopicNames().get()
                .forEach((topic, desc) -> {
                    System.out.println("Topic: " + topic);
                    System.out.printf("Partitions: %s, partition ids: %s%n", desc.partitions().size(),
                            desc.partitions()
                                    .stream()
                                    .map(p -> Integer.toString(p.partition()))
                                    .collect(Collectors.joining(",")));
                });

        admin.close();
    }
}
