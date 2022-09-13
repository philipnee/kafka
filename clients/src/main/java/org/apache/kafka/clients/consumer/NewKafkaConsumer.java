package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.events.InitializationEvent;
import org.apache.kafka.clients.consumer.events.ServerEvent;
import org.apache.kafka.clients.consumer.events.PartitionAssignmentServerEvent;
import org.apache.kafka.clients.consumer.internals.ConsumerBackgroundThread;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.internals.ClusterResourceListeners;

import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.KafkaConsumer.DEFAULT_CLOSE_TIMEOUT_MS;

public class NewKafkaConsumer<K, V> implements Consumer<K, V> {
    private static final String CLIENT_ID_METRIC_TAG = "client-id";
    private static final String JMX_PREFIX = "kafka.consumer";
    private final Optional<String> groupId;
    private final ConsumerInterceptors<K,V> interceptors;
    private Logger log;

    private final Time time;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final String clientId;

    private SubscriptionState subscriptionState;
    final Metrics metrics;
    private volatile boolean closed = false;
    private AtomicBoolean shouldUpdateState = new AtomicBoolean(false);
    private static final long NO_CURRENT_THREAD = -1L;

    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);
    private List<ConsumerPartitionAssignor> assignors;

    private ConsumerBackgroundThread<K, V> backgroundThread;

    private BlockingQueue<ServerEvent> serverEventQueue;
    private BlockingQueue<KafkaConsumerEvent> consumerEventQueue;

    public NewKafkaConsumer(Properties properties) {
        this(properties, null, null);
    }
    public NewKafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public NewKafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(Utils.propsToMap(properties), keyDeserializer, valueDeserializer);
    }
    public NewKafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    public NewKafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeser, Deserializer<V> valDeser) {
        LogContext logContext;
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);

        this.time = Time.SYSTEM;
        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);

        // If group.instance.id is set, we will append it to the log context.
        logContext = groupRebalanceConfig.groupInstanceId.map(s -> new LogContext("[Consumer instanceId=" + s +
                ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ")).orElseGet(() -> new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] "));

        this.log = logContext.logger(getClass());
        this.keyDeserializer = initializeKeyDeserializer(config, keyDeser);
        this.valueDeserializer = initializeValueDeserializer(config, valDeser);

        OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
        this.subscriptionState = new SubscriptionState(logContext, offsetResetStrategy);

        /*List<ConsumerInterceptor<K, V>> interceptorList = (List) config.getConfiguredInstances(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ConsumerInterceptor.class,
                Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
*/
        this.metrics = buildMetrics(config, time, clientId);
        /*
        this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
        );
         */
        List<ConsumerInterceptor<K, V>> interceptorList = (List) config.getConfiguredInstances(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ConsumerInterceptor.class,
                Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
        this.interceptors = new ConsumerInterceptors<>(interceptorList);

        this.serverEventQueue = new ArrayBlockingQueue<>(100);
        this.consumerEventQueue = new ArrayBlockingQueue<>(100);

        this.backgroundThread = new ConsumerBackgroundThread<>(
                config,
                subscriptionState,
                configureClusterResourceListeners(keyDeserializer, valueDeserializer, metrics.reporters(), interceptorList),
                metrics,
                this.serverEventQueue,
                this.consumerEventQueue);
        System.out.println("pour the wine");
        startBackgroundThread();
        this.serverEventQueue.add(new InitializationEvent()); // to kick off the background thread
        this.backgroundThread.wakeup();
    }


    public void startBackgroundThread() {
        try {
            this.backgroundThread.start();
        }catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
            close();
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        return null;
    }

    @Override
    public Set<String> subscription() {
        return null;
    }
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    private void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        refcount.incrementAndGet();
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }
    @Override
    public void subscribe(Collection<String> topics) {

    }
    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            if (topics == null)
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    if (Utils.isBlank(topic))
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }

                throwIfNoAssignorsConfigured();
                //fetcher.clearBufferedDataForUnassignedTopics(topics);
                log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));
                if (this.subscriptionState.subscribe(new HashSet<>(topics), callback)) {
                    this.shouldUpdateState.set(true);  // TODO: Do we actually need to use the atomic variable
                    log.info("do something");
                    //metadata.requestUpdateForNewTopics();
                }
            }
        } finally {
            release();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            }
            if (partitions.isEmpty()) {
                this.unsubscribe();
                return;
            }

            for (TopicPartition tp : partitions) {
                String topic = (tp != null) ? tp.topic() : null;
                if (Utils.isBlank(topic))
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
            }

            boolean updateMetadata = false;
            if (this.subscriptionState.assignFromUser(new HashSet<>(partitions)))
                updateMetadata = true;

            ServerEvent serverEvent = new PartitionAssignmentServerEvent(partitions, updateMetadata);
            this.serverEventQueue.add(serverEvent);
            this.backgroundThread.wakeup();
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {

    }

    @Override
    public void subscribe(Pattern pattern) {

    }

    @Override
    public void unsubscribe() {

    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(long timeout) {
        return null;
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return null;
    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitSync(Duration timeout) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {

    }

    @Override
    public void commitAsync() {

    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

    }

    @Override
    public void seek(TopicPartition partition, long offset) {

    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {

    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {

    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {

    }

    @Override
    public long position(TopicPartition partition) {
        return 0;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return 0;
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(partition, Duration.ofMillis(100));
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
        return committed(Collections.singleton(partition), timeout).get(partition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(100));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            return new HashMap<>();
        } finally {
            release();
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {

    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {

    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return null;
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return null;
    }

    @Override
    public void enforceRebalance() {

    }

    @Override
    public void enforceRebalance(String reason) {

    }

    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout.toMillis(), false);
            }
        } catch(Exception e ) {
            e.printStackTrace();
        } finally {
            closed = true;
            release();
        }
    }

    private void close(long time, boolean swallowException) {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        try {
            this.backgroundThread.close();
        } catch (Throwable t) {
            firstException.compareAndSet(null, t);
            log.error("Failed to close coordinator", t);
            t.printStackTrace();
        }
        Utils.closeQuietly(metrics, "consumer metrics", firstException);
        Utils.closeQuietly(keyDeserializer, "consumer key deserializer", firstException);
        Utils.closeQuietly(valueDeserializer, "consumer value deserializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    @Override
    public void wakeup() {

    }
    @SuppressWarnings("unchecked")
    private Deserializer<K> initializeKeyDeserializer(ConsumerConfig config, Deserializer<K> keyDeserializer) {
        Deserializer<K> deserializer;
        if (keyDeserializer == null) {
            deserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            deserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
        } else {
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            deserializer = keyDeserializer;
        }
        return deserializer;
    }

    @SuppressWarnings("unchecked")
    private Deserializer<V> initializeValueDeserializer(ConsumerConfig config, Deserializer<V> valueDeserializer) {
        Deserializer<V> deserializer;
        if (valueDeserializer == null) {
            deserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            deserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
        } else {
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            deserializer = valueDeserializer;
        }
        return deserializer;
    }

    private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    private static Metrics buildMetrics(ConsumerConfig config, Time time, String clientId) {
        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricsTags);
        List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class, Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
        JmxReporter jmxReporter = new JmxReporter();
        jmxReporter.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)));
        reporters.add(jmxReporter);
        MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
        return new Metrics(metricConfig, reporters, time, metricsContext);
    }
}
