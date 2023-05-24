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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code IdempotentCloser} encapsulates some basic logic to ensure that a given resource is only closed once.
 */
public class IdempotentCloser implements AutoCloseable {

    private final AtomicBoolean flag = new AtomicBoolean(false);

    public void maybeThrowIllegalStateException(String message) {
        if (isClosed())
            throw new IllegalStateException(message);
    }

    public boolean isClosed() {
        return flag.get();
    }

    @Override
    public void close() {
        close(null, null);
    }

    public void close(final Runnable onClose) {
        close(onClose, null);
    }

    public void close(final Runnable onClose, final Runnable onPreviousClose) {
        if (flag.compareAndSet(false, true)) {
            if (onClose != null)
                onClose.run();
        } else {
            if (onPreviousClose != null)
                onPreviousClose.run();
        }
    }
}