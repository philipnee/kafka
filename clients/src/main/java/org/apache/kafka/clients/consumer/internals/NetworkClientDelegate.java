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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_METRIC_GROUP_PREFIX;

/**
 * A wrapper around the {@link org.apache.kafka.clients.NetworkClient} to handle network poll and send operations.
 */
public class NetworkClientDelegate implements AutoCloseable {

    private final Logger log;
    private final Time time;
    private final KafkaClient client;
    private final int requestTimeoutMs;
    private final Queue<UnsentRequest> unsentRequests;
    private final long retryBackoffMs;

    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    public NetworkClientDelegate(final LogContext logContext,
                                 final Time time,
                                 final ConsumerConfig config,
                                 final KafkaClient client) {
        this(logContext,
                time,
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
                client);
    }

    public NetworkClientDelegate(final LogContext logContext,
                                 final Time time,
                                 final int requestTimeoutMs,
                                 final long retryBackoffMs,
                                 final KafkaClient client) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.client = client;
        this.unsentRequests = new ArrayDeque<>();
    }

    public static NetworkClientDelegate create(final LogContext logContext,
                                               final Time time,
                                               final ConsumerConfig config,
                                               final Metrics metrics,
                                               final ConsumerMetadata metadata,
                                               final ApiVersions apiVersions,
                                               final Sensor throttleTimeSensor) {
        NetworkClient networkClient = ClientUtils.createNetworkClient(config,
                metrics,
                CONSUMER_METRIC_GROUP_PREFIX,
                logContext,
                apiVersions,
                time,
                CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                metadata,
                throttleTimeSensor);
        return new NetworkClientDelegate(logContext, time, config, networkClient);
    }

    public void pollNoWakeup() {
        poll(time.timer(0));
    }

    public int pendingRequestCount() {
        return unsentRequests.size() + client.inFlightRequestCount();
    }

    public void poll(final Timer timer) {
        long pollTimeout = Math.min(timer.remainingMs(), requestTimeoutMs);
        if (client.inFlightRequestCount() == 0)
            pollTimeout = Math.min(pollTimeout, retryBackoffMs);
        poll(pollTimeout, timer.currentTimeMs());
    }

    /**
     * Returns the responses of the sent requests. This method will try to send the unsent requests, poll for responses,
     * and check the disconnected nodes.
     *
     * @param timeoutMs     timeout time
     * @param currentTimeMs current time
     * @return a list of client response
     */
    public void poll(final long timeoutMs, final long currentTimeMs) {
        handlePendingDisconnects();
        trySend(currentTimeMs);

        long pollTimeoutMs = timeoutMs;
        if (!unsentRequests.isEmpty()) {
            pollTimeoutMs = Math.min(retryBackoffMs, pollTimeoutMs);
        }
        this.client.poll(pollTimeoutMs, currentTimeMs);
        checkDisconnects();
    }

    /**
     * Tries to send the requests in the unsentRequest queue. If the request doesn't have an assigned node, it will
     * find the leastLoadedOne, and will be retried in the next {@code poll()}. If the request is expired, a
     * {@link TimeoutException} will be thrown.
     */
    private void trySend(final long currentTimeMs) {
        Iterator<UnsentRequest> iterator = unsentRequests.iterator();
        while (iterator.hasNext()) {
            UnsentRequest unsent = iterator.next();
            unsent.timer.update(currentTimeMs);
            if (unsent.timer.isExpired()) {
                iterator.remove();
                unsent.handler.onFailure(new TimeoutException(
                    "Failed to send request after " + unsent.timer.timeoutMs() + " ms."));
                continue;
            }

            if (!doSend(unsent, currentTimeMs)) {
                // continue to retry until timeout.
                continue;
            }
            iterator.remove();
        }
    }

    boolean doSend(final UnsentRequest r, final long currentTimeMs) {
        Node node = r.node.orElse(client.leastLoadedNode(currentTimeMs));
        if (node == null || nodeUnavailable(node)) {
            log.debug("No broker available to send the request: {}. Retrying.", r);
            return false;
        }
        ClientRequest request = makeClientRequest(r, node, currentTimeMs);
        if (!client.ready(node, currentTimeMs)) {
            // enqueue the request again if the node isn't ready yet. The request will be handled in the next iteration
            // of the event loop
            log.debug("Node is not ready, handle the request in the next event loop: node={}, request={}", node, r);
            return false;
        }
        client.send(request, currentTimeMs);
        return true;
    }

    private Set<Node> unsentRequestNodes() {
        Set<Node> set = new HashSet<>();

        for (UnsentRequest u : unsentRequests)
            u.node.ifPresent(set::add);

        return set;
    }

    private List<UnsentRequest> removeUnsentRequestByNode(Node node) {
        List<UnsentRequest> list = new ArrayList<>();

        Iterator<UnsentRequest> iter = unsentRequests.iterator();

        while (iter.hasNext()) {
            UnsentRequest u = iter.next();

            if (node.equals(u.node.orElse(null))) {
                iter.remove();
                list.add(u);
            }
        }

        return list;
    }

    private void checkDisconnects() {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        for (Node node : unsentRequestNodes()) {
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                for (UnsentRequest unsentRequest : removeUnsentRequestByNode(node)) {
                    AuthenticationException authenticationException = client.authenticationException(node);
                    FutureCompletionHandler handler = unsentRequest.handler;
                    AbstractRequest.Builder<?> request = unsentRequest.requestBuilder();

                    if (true)
                        throw new RuntimeException("Fix me!");
                }
            }
        }
    }

    private void handlePendingDisconnects() {
        while (true) {
            Node node = pendingDisconnects.poll();
            if (node == null)
                break;

            failUnsentRequests(node, DisconnectException.INSTANCE);
            client.disconnect(node.idString());
        }
    }

    public void disconnectAsync(Node node) {
        pendingDisconnects.offer(node);
        client.wakeup();
    }

    private void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        for (UnsentRequest unsentRequest : removeUnsentRequestByNode(node)) {
            FutureCompletionHandler handler = unsentRequest.handler;
            handler.onFailure(e);
        }
    }

    private ClientRequest makeClientRequest(
        final UnsentRequest unsent,
        final Node node,
        final long currentTimeMs
    ) {
        return client.newClientRequest(
            node.idString(),
            unsent.requestBuilder,
            currentTimeMs,
            true,
            (int) unsent.timer.remainingMs(),
            unsent.handler
        );
    }

    public Node leastLoadedNode() {
        return this.client.leastLoadedNode(time.milliseconds());
    }

    public void send(final UnsentRequest r) {
        r.setTimer(time, this.requestTimeoutMs);
        unsentRequests.add(r);
    }

    public void wakeup() {
        client.wakeup();
    }

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in reconnect
     * backoff window following the disconnect).
     */
    public boolean nodeUnavailable(final Node node) {
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    public void close() throws IOException {
        this.client.close();
    }

    public void addAll(final List<UnsentRequest> requests) {
        requests.forEach(u -> {
            u.setTimer(time, this.requestTimeoutMs);
        });
        this.unsentRequests.addAll(requests);
    }

    public static class PollResult {
        public final long timeUntilNextPollMs;
        public final List<UnsentRequest> unsentRequests;

        public PollResult(final long timeMsTillNextPoll) {
            this(timeMsTillNextPoll, Collections.emptyList());
        }

        public PollResult(final long timeMsTillNextPoll, final UnsentRequest unsentRequest) {
            this(timeMsTillNextPoll, Collections.singletonList(unsentRequest));
        }

        public PollResult(final long timeMsTillNextPoll, final List<UnsentRequest> unsentRequests) {
            this.timeUntilNextPollMs = timeMsTillNextPoll;
            this.unsentRequests = Collections.unmodifiableList(unsentRequests);
        }

        @Override
        public String toString() {
            return "PollResult{" +
                    "timeUntilNextPollMs=" + timeUntilNextPollMs +
                    ", unsentRequests=" + unsentRequests +
                    '}';
        }
    }

    public static class UnsentRequest {

        private final AbstractRequest.Builder<?> requestBuilder;
        private final FutureCompletionHandler handler;
        private final Optional<Node> node; // empty if random node can be chosen
        private Timer timer;

        public UnsentRequest(final AbstractRequest.Builder<?> requestBuilder, final Optional<Node> node) {
            this(requestBuilder, node, new FutureCompletionHandler());
        }

        public UnsentRequest(final AbstractRequest.Builder<?> requestBuilder,
                             final Optional<Node> node,
                             final FutureCompletionHandler handler) {
            Objects.requireNonNull(requestBuilder);
            this.requestBuilder = requestBuilder;
            this.node = node;
            this.handler = handler;
        }

        public void setTimer(final Time time, final long requestTimeoutMs) {
            this.timer = time.timer(requestTimeoutMs);
        }

        CompletableFuture<ClientResponse> future() {
            return handler.future;
        }

        RequestCompletionHandler callback() {
            return handler;
        }

        AbstractRequest.Builder<?> requestBuilder() {
            return requestBuilder;
        }

        @Override
        public String toString() {
            return "UnsentRequest(builder=" + requestBuilder + ")";
        }
    }

    public static class FutureCompletionHandler implements RequestCompletionHandler {

        private final CompletableFuture<ClientResponse> future;

        FutureCompletionHandler() {
            this.future = new CompletableFuture<>();
        }

        public void onFailure(final RuntimeException e) {
            future.completeExceptionally(e);
        }

        public CompletableFuture<ClientResponse> future() {
            return future;
        }

        @Override
        public void onComplete(final ClientResponse response) {
            if (response.authenticationException() != null) {
                onFailure(response.authenticationException());
            } else if (response.wasDisconnected()) {
                onFailure(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                onFailure(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }
    }

}
