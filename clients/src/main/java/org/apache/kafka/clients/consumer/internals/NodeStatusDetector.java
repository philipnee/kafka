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

import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.Time;

public interface NodeStatusDetector {

    boolean isUnavailable(Node node);

    void maybeThrowAuthFailure(Node node);

    class NetworkClientNodeStatusDetector implements NodeStatusDetector {

        private final NetworkClient networkClient;
        private final Time time;

        public NetworkClientNodeStatusDetector(NetworkClient networkClient, Time time) {
            this.networkClient = networkClient;
            this.time = time;
        }

        @Override
        public boolean isUnavailable(Node node) {
            return networkClient.connectionFailed(node) && networkClient.connectionDelay(node, time.milliseconds()) > 0;
        }

        @Override
        public void maybeThrowAuthFailure(Node node) {
            AuthenticationException exception = networkClient.authenticationException(node);
            if (exception != null)
                throw exception;
        }
    }
}
