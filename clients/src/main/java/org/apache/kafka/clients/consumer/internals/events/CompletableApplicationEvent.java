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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CompletableApplicationEvent<T> extends ApplicationEvent {

    public final CompletableFuture<T> future;

    protected CompletableApplicationEvent(Type type) {
        super(type);
        this.future = new CompletableFuture<>();
    }

    public T get(Timer timer) {
        try {
            return future.get(timer.remainingMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();

            if (t instanceof KafkaException)
                throw (KafkaException) t;
            else
                throw new KafkaException(t);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new TimeoutException(e);
        }
    }

}
