package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.events.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.*;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ConsumerBackgroundThread<K,V> extends KafkaThread implements AutoCloseable {
    private static final String CLIENT_ID_METRIC_TAG = "client-id";
    private static final String JMX_PREFIX = "kafka.consumer";
    private static final String CONSUMER_BACKGROUND_THREAD_PREFIX = "consumer_background_thread";
    private final Time time;
    private final LogContext logContext;
    private final ConsumerAsyncCoordinator coordinator;
    private final List<ConsumerPartitionAssignor> assignors;
    private final SubscriptionState subscription;
    private final Fetcher<K, V> fetcher;

    private BackgroundStateMachine stateMachine;
    private boolean closed = false;
    private boolean needCoordinator = false;
    private long retryBackoffMs;
    private String clientId;

    private final ConsumerNetworkClient networkClient;
    private final ConsumerMetadata metadata;
    private final static String metricGrpPrefix = "consumer";

    final Metrics metrics;

    private int heartbeatIntervalMs;
    private Logger log;
    private Optional<String> groupId;
    private GroupRebalanceConfig groupRebalanceConfig;
    private int requestTimeoutMs;
    private IsolationLevel isolationLevel;
    private final Heartbeat heartbeat;

    private BlockingQueue<AbstractServerEvent> serverEventQueue;
    private BlockingQueue<AbstractConsumerEvent> consumerEventQueue;

    private Map<ServerEventType, ServerEventExecutor> eventExecutorRegistry;

    private AtomicBoolean shouldWakeup = new AtomicBoolean(false);

    private boolean shouldHeartBeat = false;
    private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

    public ConsumerBackgroundThread(ConsumerConfig config,
                                    SubscriptionState subscriptions, // TODO: it is currently a shared state between polling and background thread
                                    ClusterResourceListeners clusterResourceListeners,
                                    Metrics metrics,
                                    BlockingQueue<AbstractServerEvent> serverEventQueue,
                                    BlockingQueue<AbstractConsumerEvent> consumerEventQueue) {
        super(CONSUMER_BACKGROUND_THREAD_PREFIX, true);
        configuration(config);
        this.time = Time.SYSTEM;
        this.metrics = metrics;
        this.stateMachine = new BackgroundStateMachine(BackgroundStates.DOWN);
        this.subscription = subscriptions;

        this.logContext = initializeLogContext(config);
        this.log = logContext.logger(getClass());

        this.metadata = new ConsumerMetadata(retryBackoffMs,
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                subscriptions, logContext, clusterResourceListeners);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
        this.metadata.bootstrap(addresses);

        FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry(Collections.singleton(CLIENT_ID_METRIC_TAG), metricGrpPrefix);
        ApiVersions apiVersions = new ApiVersions();
        Sensor throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry);

        this.networkClient = initializeNetworkClient(config, logContext, apiVersions, throttleTimeSensor);
        this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
        );

        this.coordinator = maybeInitiateCoordinator(groupId, config);
        this.fetcher = new Fetcher<>(
                logContext,
                this.networkClient,
                config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
                config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
                null,
                null,
                this.metadata,
                this.subscription,
                metrics,
                metricsRegistry,
                this.time,
                this.retryBackoffMs,
                this.requestTimeoutMs,
                isolationLevel,
                apiVersions);
        this.heartbeat = new Heartbeat(groupRebalanceConfig, time);

        this.serverEventQueue = serverEventQueue;
        this.consumerEventQueue = consumerEventQueue;

        // contains a bunch of event executors, it is unmodifiable after initialized
        this.eventExecutorRegistry = initializeEventExecutorRegistry();
    }

    private Map<ServerEventType, ServerEventExecutor> initializeEventExecutorRegistry() {
        Map<ServerEventType, ServerEventExecutor> registry = new ConcurrentHashMap<>();
        registry.put(ServerEventType.NOOP, new ServerEventExecutor() {
            @Override
            public Void call() throws Exception {
                return null;
            }
        });

        registry.put(ServerEventType.ASSIGN, new PartitionAssignmentEventExecutor<>(
                time,
                this.metadata,
                this.fetcher,
                this.coordinator));

        return Collections.unmodifiableMap(registry);
    }

    private ConsumerAsyncCoordinator maybeInitiateCoordinator(Optional<String> groupId, ConsumerConfig config) {
        boolean enableAutoCommit = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (!groupId.isPresent()) {
            config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
            //config.ignore(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
            return null;
        }

        return new ConsumerAsyncCoordinator(groupRebalanceConfig,
                logContext,
                this.networkClient,
                assignors,
                this.metadata,
                this.subscription,
                metrics,
                metricGrpPrefix,
                this.time,
                enableAutoCommit,
                config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                null,
                false); // TODO: revisit the config
    }

    private LogContext initializeLogContext(ConsumerConfig config) {
        return groupRebalanceConfig.groupInstanceId.map(
                s -> new LogContext("[Consumer instanceId=" + s + ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] "))
                .orElseGet(
                        () -> new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] "));

    }

    @Override
    public void run() {
        try {
            while (!closed) {
                if (shouldWakeup.get() || !serverEventQueue.isEmpty()) {
                    Optional<AbstractServerEvent> event = Optional.ofNullable(serverEventQueue.poll());
                    runStateMachine(event);
                    if (event.isPresent() && eventExecutorRegistry.containsKey(event.get().getEventType())) {
                        ServerEventExecutor executor = eventExecutorRegistry.getOrDefault(event.get().getEventType(), new NoopEventExecutor());
                        executor.run(event.get());
                    }
                }
                this.networkClient.poll(time.timer(Long.MAX_VALUE));
            }

        } catch (AuthenticationException e) {
            log.error("An authentication error occurred in the background thread", e);
            this.failed.set(e);
        } catch (GroupAuthorizationException e) {
            log.error("A group authorization error occurred in the background thread", e);
            this.failed.set(e);
        } catch (InterruptedException | InterruptException e) {
            Thread.interrupted();
            log.error("Unexpected interrupt received in background thread", e);
            this.failed.set(new RuntimeException(e));
        } catch (Throwable e) {
            log.error("Background thread failed due to unexpected error", e);
            if (e instanceof RuntimeException)
                this.failed.set((RuntimeException) e);
            else
                this.failed.set(new RuntimeException(e));
        } finally {
            log.debug("Heartbeat thread has closed");
        }
    }

    private void runStateMachine(Optional<AbstractServerEvent> optionalEvent) throws InterruptedException {
        if(optionalEvent.isPresent()) {
            AbstractServerEvent event = optionalEvent.get();
            if (event.isRequireCoordinator()) {
                this.needCoordinator = true;
            }
        }
        while(true) {
            switch (stateMachine.getCurrentState()) {
                case INITIALIZED:
                    if (!needCoordinator) {
                        // no coordinator is required
                       return;
                    }
                    maybeTransitionToCoordinatorDiscovery();
                    break;
                case COORDINATOR_DISCOVERY:
                    // in progress of finding the coordinator
                    maybeTransitionToStable();
                    break;
                case STABLE:
                    if (coordinator.coordinatorUnknown()) {
                        maybeTransitionToInitialized();
                        log.warn("lost coordinator");
                        break;
                    }
                    coordinator.poll();
                    return;
                case DOWN:
                    maybeTransitionToInitialized();
                    log.info("closed");
                    break;
            }
        }
    }

    private void maybeTransitionToInitialized() {
        stateMachine.transitionTo(BackgroundStates.INITIALIZED);
    }

    /*
    private void maybeHeartbeat(long heartbeatTimeoutMs) {
        networkClient.pollNoWakeup();
        if(coordinator.coordinatorUnknown()) {
            log.error("no coordinator, rediscover");
            maybeTransitionToInitialized();
            return;
        }

        if(heartbeat.shouldHeartbeat(time.milliseconds())) {

        }
    }

    private RequestFuture<Void> sendHeartbeat() {
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        .setGroupId(groupRebalanceConfig.groupId)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.groupRebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
    }

     */

    // TODO: ask Jason about the blocking behavior of coordinator discovery
    private void maybeTransitionToCoordinatorDiscovery() {
        if(coordinator.coordinatorUnknown() && !coordinator.ensureCoordinatorReady()) {
            stateMachine.transitionTo(BackgroundStates.INITIALIZED);
            return;
        }
        stateMachine.transitionTo(BackgroundStates.COORDINATOR_DISCOVERY);
    }

    public void wakeup() {
        this.networkClient.wakeup();
        this.shouldWakeup.set(true);
    }

    private void maybeTransitionToStable() {
        if(coordinator.coordinatorUnknown()) {
            // TODO: make it retriable
            // do something
            log.error("Unable to find coordinator");
            stateMachine.transitionTo(BackgroundStates.INITIALIZED);
            return;
        }
        this.needCoordinator = false;
        stateMachine.transitionTo(BackgroundStates.STABLE);
        return;
    }

    @Override
    public void close() {
        this.closed = true;
        this.wakeup();
        
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        try {
            if (coordinator != null)
                coordinator.close(time.timer(Math.min(500, requestTimeoutMs))); // TODO: timeoutMs needs to be impl here
        } catch (Throwable t) {
            firstException.compareAndSet(null, t);
            log.error("Failed to close coordinator", t);
            t.printStackTrace();
        }
        this.shouldWakeup.set(false);
        this.stateMachine.transitionTo(BackgroundStates.DOWN);
        org.apache.kafka.common.utils.Utils.closeQuietly(fetcher, "fetcher", firstException);
        org.apache.kafka.common.utils.Utils.closeQuietly(metrics, "consumer metrics", firstException);
        org.apache.kafka.common.utils.Utils.closeQuietly(networkClient, "consumer network client", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null) { // TODO: swallo exception
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    private void configuration(ConsumerConfig config) {
        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.isolationLevel = IsolationLevel.valueOf(
                config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));

        this.groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
    }

    private ConsumerNetworkClient initializeNetworkClient(ConsumerConfig config, LogContext logContext, ApiVersions apiVersions, Sensor throttleTimeSensor) {
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
        NetworkClient networkClient = new NetworkClient(
                new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder, logContext),
                this.metadata,
                clientId,
                100, // a fixed large enough value will suffice for max in-flight requests
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time,
                true,
                apiVersions,
                logContext);
        return new ConsumerNetworkClient(
                logContext,
                networkClient,
                metadata,
                time,
                retryBackoffMs,
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                heartbeatIntervalMs);
    }

}
