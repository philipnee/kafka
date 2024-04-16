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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.metrics.NetworkThreadMetrics;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * Background thread runnable that consumes {@link ApplicationEvent} and produces {@link BackgroundEvent}. It
 * uses an event loop to consume and produce events, and poll the network client to handle network IO.
 */
public class ConsumerNetworkThread extends KafkaThread implements Closeable {

    // visible for testing
    static final long MAX_POLL_TIMEOUT_MS = 5000;
    private static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";
    private final Time time;
    private final Logger log;
    private final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier;
    private final Supplier<NetworkClientDelegate> networkClientDelegateSupplier;
    private final Supplier<RequestManagers> requestManagersSupplier;
    private NetworkThreadMetrics networkThreadMetrics;
    private ApplicationEventProcessor applicationEventProcessor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private volatile boolean running;
    private final IdempotentCloser closer = new IdempotentCloser();
    private volatile Duration closeTimeout = Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS);
    private volatile long cachedMaximumTimeToWait = MAX_POLL_TIMEOUT_MS;

    public ConsumerNetworkThread(LogContext logContext,
                                 Time time,
                                 Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                                 Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                 Supplier<RequestManagers> requestManagersSupplier,
                                 Metrics metrics) {
        super(BACKGROUND_THREAD_NAME, true);
        this.time = time;
        this.log = logContext.logger(getClass());
        this.applicationEventProcessorSupplier = applicationEventProcessorSupplier;
        this.networkClientDelegateSupplier = networkClientDelegateSupplier;
        this.requestManagersSupplier = requestManagersSupplier;
        this.networkThreadMetrics = new NetworkThreadMetrics(metrics);
        this.running = true;
    }

    @Override
    public void run() {
        try {
            log.debug("Consumer network thread started");

            // Wait until we're securely in the background network thread to initialize these objects...
            initializeResources();

            while (running) {
                try {
                    runOnce();
                } catch (final Throwable e) {
                    // Swallow the exception and continue
                    log.error("Unexpected error caught in consumer network thread", e);
                }
            }
        } finally {
            cleanup();
        }
    }

    void initializeResources() {
        applicationEventProcessor = applicationEventProcessorSupplier.get();
        networkClientDelegate = networkClientDelegateSupplier.get();
        requestManagers = requestManagersSupplier.get();
    }

    /**
     * Poll and process the {@link ApplicationEvent application events}. It performs the following tasks:
     *
     * <ol>
     *     <li>
     *         Drains and processes all the events from the application thread's application event queue via
     *         {@link ApplicationEventProcessor}
     *     </li>
     *     <li>
     *         Iterate through the {@link RequestManager} list and invoke {@link RequestManager#poll(long)} to get
     *         the {@link NetworkClientDelegate.UnsentRequest} list and the poll time for the network poll
     *     </li>
     *     <li>
     *         Stage each {@link AbstractRequest.Builder request} to be sent via
     *         {@link NetworkClientDelegate#addAll(List)}
     *     </li>
     *     <li>
     *         Poll the client via {@link KafkaClient#poll(long, long)} to send the requests, as well as
     *         retrieve any available responses
     *     </li>
     * </ol>
     */
    void runOnce() {
        // Process the events—if any—that were produced by the application thread. It is possible that when processing
        // an event generates an error. In such cases, the processor will log an exception, but we do not want those
        // errors to be propagated to the caller.
        applicationEventProcessor.process();

        final long currentTimeMs = time.milliseconds();
        final long pollWaitTimeMs = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(rm -> pollAndRecordMetric.apply(rm, currentTimeMs))
                .map(networkClientDelegate::addAll)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);

        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);

        cachedMaximumTimeToWait = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(rm -> rm.maximumTimeToWait(currentTimeMs))
                .reduce(Long.MAX_VALUE, Math::min);

        networkThreadMetrics.recordBackoffTime(System.currentTimeMillis() - currentTimeMs);
    }

    private final BiFunction<RequestManager, Long, NetworkClientDelegate.PollResult> pollAndRecordMetric = (rm, currentTimeMs) -> {
        NetworkClientDelegate.PollResult pollTime = rm.poll(currentTimeMs);
        networkThreadMetrics.recordPollTime(pollTime.timeUntilNextPollMs);
        return pollTime;
    };

    /**
     * Performs any network I/O that is needed at the time of close for the consumer:
     *
     * <ol>
     *     <li>
     *         Iterate through the {@link RequestManager} list and invoke {@link RequestManager#pollOnClose()}
     *         to get the {@link NetworkClientDelegate.UnsentRequest} list and the poll time for the network poll
     *     </li>
     *     <li>
     *         Stage each {@link AbstractRequest.Builder request} to be sent via
     *         {@link NetworkClientDelegate#addAll(List)}
     *     </li>
     *     <li>
     *         {@link KafkaClient#poll(long, long) Poll the client} to send the requests, as well as
     *         retrieve any available responses
     *     </li>
     *     <li>
     *         Continuously {@link KafkaClient#poll(long, long) poll the client} as long as the
     *         {@link Timer#notExpired() timer hasn't expired} to retrieve the responses
     *     </li>
     * </ol>
     */
    // Visible for testing
    static void runAtClose(final Collection<Optional<? extends RequestManager>> requestManagers,
                           final NetworkClientDelegate networkClientDelegate,
                           final Timer timer) {
        // These are the optional outgoing requests at the
        requestManagers.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(RequestManager::pollOnClose)
                .forEach(networkClientDelegate::addAll);
    }

    public boolean isRunning() {
        return running;
    }

    public void wakeup() {
        // The network client can be null if the initializeResources method has not yet been called.
        if (networkClientDelegate != null)
            networkClientDelegate.wakeup();
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * Because this method is called by the application thread, it's not allowed to access the request managers
     * that actually provide the information. As a result, the consumer network thread periodically caches the
     * information from the request managers and this can then be read safely using this method.
     *
     * @return The maximum delay in milliseconds
     */
    public long maximumTimeToWait() {
        return cachedMaximumTimeToWait;
    }

    @Override
    public void close() {
        close(closeTimeout);
    }

    public void close(final Duration timeout) {
        Objects.requireNonNull(timeout, "Close timeout for consumer network thread must be non-null");

        closer.close(
                () -> closeInternal(timeout),
                () -> log.warn("The consumer network thread was already closed")
        );
    }

    /**
     * Starts the closing process.
     *
     * <p/>
     *
     * This method is called from the application thread, but our resources are owned by the network thread. As such,
     * we don't actually close any of those resources here, immediately, on the application thread. Instead, we just
     * update our internal state on the application thread. When the network thread next
     * {@link #run() executes its loop}, it will notice that state, cease processing any further events, and begin
     * {@link #cleanup() closing its resources}.
     *
     * <p/>
     *
     * This method will wait (i.e. block the application thread) for up to the duration of the given timeout to give
     * the network thread the time to close down cleanly.
     *
     * @param timeout Upper bound of time to wait for the network thread to close its resources
     */
    private void closeInternal(final Duration timeout) {
        long timeoutMs = timeout.toMillis();
        log.trace("Signaling the consumer network thread to close in {}ms", timeoutMs);
        running = false;
        closeTimeout = timeout;
        wakeup();

        try {
            join();
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for consumer network thread to complete", e);
        }
    }

    /**
     * Check the unsent queue one last time and poll until all requests are sent or the timer runs out.
     */
    private void sendUnsentRequests(final Timer timer) {
        if (networkClientDelegate.unsentRequests().isEmpty())
            return;
        do {
            networkClientDelegate.poll(timer.remainingMs(), timer.currentTimeMs());
            timer.update();
        } while (timer.notExpired() && !networkClientDelegate.unsentRequests().isEmpty());
    }

    void cleanup() {
        log.trace("Closing the consumer network thread");
        Timer timer = time.timer(closeTimeout);
        try {
            runAtClose(requestManagers.entries(), networkClientDelegate, timer);
        } catch (Exception e) {
            log.error("Unexpected error during shutdown.  Proceed with closing.", e);
        } finally {
            sendUnsentRequests(timer);
            closeQuietly(requestManagers, "request managers");
            closeQuietly(networkClientDelegate, "network client delegate");
            closeQuietly(applicationEventProcessor, "application event processor");
            log.debug("Closed the consumer network thread");
        }
    }
}
