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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.events.ConsumerEvent;
import org.apache.kafka.clients.consumer.events.ServerEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerResponseEvent;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultEventHandler implements EventHandler<ConsumerRequestEvent, ConsumerResponseEvent> {
    BlockingQueue<ConsumerRequestEvent> consumerRequestEventQueue;
    BlockingQueue<ConsumerResponseEvent> consumerResponseEventQueue;
    private final ConsumerBackgroundThread backgroundThread;

    public DefaultEventHandler(ConsumerConfig config,
                               SubscriptionState subscriptions, // TODO: it is currently a shared state between polling and background thread
                               ClusterResourceListeners clusterResourceListeners,
                               Metrics metrics) {
        this.consumerRequestEventQueue = new LinkedBlockingQueue<>();
        this.consumerResponseEventQueue = new LinkedBlockingQueue<>();
        this.backgroundThread = new ConsumerBackgroundThread(config, subscriptions, clusterResourceListeners, metrics, consumerRequestEventQueue, consumerResponseEventQueue);

        this.backgroundThread.start();
    }

    @Override
    public ConsumerResponseEvent poll() {
        return consumerResponseEventQueue.poll();
    }

    public boolean hasResponseEvent() {
        return !consumerResponseEventQueue.isEmpty();
    }

    @Override
    public boolean add(ConsumerRequestEvent event) {
        return consumerRequestEventQueue.add(event);
    }

    @Override
    public void close() {
        this.backgroundThread.close();
    }
}
