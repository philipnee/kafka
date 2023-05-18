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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Build requests for retrieving partition offsets from partition leaders (see
 * {@link #fetchOffsets(Set, long, boolean)}). Requests are kept in-memory
 * ready to be sent on the next call to {@link #poll(long)}.
 * <p>
 * Partition leadership information required to build the requests is retrieved from the
 * {@link ConsumerMetadata}, so this implements {@link ClusterResourceListener} to get notified
 * when the cluster metadata is updated.
 */
public class ListOffsetsRequestManager implements RequestManager, ClusterResourceListener {

    private final ConsumerMetadata metadata;
    private final IsolationLevel isolationLevel;
    private final Logger log;
    private final OffsetFetcherUtils offsetFetcherUtils;

    private final List<ListOffsetsRequestState> requestsToRetry = Collections.synchronizedList(new ArrayList<>());
    private final List<NetworkClientDelegate.UnsentRequest> pendingRequests =
            Collections.synchronizedList(new ArrayList<>());

    public ListOffsetsRequestManager(final SubscriptionState subscriptionState,
                                     final ConsumerMetadata metadata,
                                     final ConsumerConfig config,
                                     final Time time,
                                     final ApiVersions apiVersions,
                                     final LogContext logContext) {
        requireNonNull(subscriptionState);
        requireNonNull(metadata);
        requireNonNull(config);
        requireNonNull(time);
        requireNonNull(apiVersions);
        requireNonNull(logContext);

        this.metadata = metadata;
        this.metadata.addClusterUpdateListener(this);
        String isolationLevelStr = config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        this.isolationLevel = IsolationLevel.valueOf(((isolationLevelStr == null) ?
                ConsumerConfig.DEFAULT_ISOLATION_LEVEL : isolationLevelStr).toUpperCase(Locale.ROOT));
        this.log = logContext.logger(getClass());
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptionState,
                time, apiVersions);

    }

    /**
     * Determine if a there are pending fetch offsets requests to be sent and build a
     * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult}
     * containing it.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        List<NetworkClientDelegate.UnsentRequest> requestsToSend;
        synchronized (pendingRequests) {
            requestsToSend = new ArrayList<>(pendingRequests);
            pendingRequests.clear();
        }
        if (requestsToSend.isEmpty()) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList());
        }

        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE,
                Collections.unmodifiableList(requestsToSend));
    }

    /**
     * Retrieve offsets for the given partitions and timestamp. This will build multiple requests (one for each
     * node that is leader of some of the partitions involved), add them to the list of
     * `pendingRequests` ready to be sent on the next call to {@link #poll(long)} and compute an
     * aggregated result.
     *
     * @param partitions        Partitions to get offsets for
     * @param timestamp         Target time
     * @param requireTimestamps True if this should fail with an UnsupportedVersionException if
     *                          the broker does not support fetching precise timestamps for offsets
     * @return Future containing the map of offsets retrieved for each partition. The future will
     * complete when the responses for the requests are received and processed following a call
     * to {@link #poll(long)}
     */
    public CompletableFuture<Map<TopicPartition, Long>> fetchOffsets(final Set<TopicPartition> partitions,
                                                                     long timestamp,
                                                                     boolean requireTimestamps) {
        if (partitions.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        metadata.addTransientTopics(offsetFetcherUtils.topicsForPartitions(partitions));

        Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), tp -> timestamp));
        ListOffsetsRequestState requestState = new ListOffsetsRequestState(timestampsToSearch,
                requireTimestamps, offsetFetcherUtils, isolationLevel);
        requestState.globalResult.whenComplete((result, error) -> metadata.clearTransientTopics());

        fetchOffsetsByTimes(timestampsToSearch, requireTimestamps, requestState);

        return requestState.globalResult;
    }

    /**
     * Generate requests for partitions with known leaders. Update the requestState by adding
     * partitions with unknown leader to the requestState.remainingToSearch
     */
    private void fetchOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                     boolean requireTimestamps,
                                     ListOffsetsRequestState requestState) {

        Map<TopicPartition, Long> remainingToSearch = new HashMap<>(timestampsToSearch);

        List<NetworkClientDelegate.UnsentRequest> unsentRequests =
                sendListOffsetsRequests(remainingToSearch, requireTimestamps, requestState);

        if (!requestState.remainingToSearch.isEmpty()) {
            requestsToRetry.add(requestState);
        }
        pendingRequests.addAll(unsentRequests);
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        // Cluster metadata has been updated. Retry any request that is waiting to be built or retried.
        List<ListOffsetsRequestState> requestsToProcess;
        synchronized (requestsToRetry) {
            requestsToProcess = new ArrayList<>(requestsToRetry);
            requestsToRetry.clear();
        }
        requestsToProcess.forEach(requestState -> fetchOffsetsByTimes(requestState.remainingToSearch, requestState.requireTimestamps,
                requestState));
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps  true if we should fail with an UnsupportedVersionException if the broker does
     *                           not support fetching precise timestamps for offsets
     * @return A list of
     * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest}
     * that can be polled to obtain the corresponding timestamps and offsets.
     */
    private List<NetworkClientDelegate.UnsentRequest> sendListOffsetsRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final boolean requireTimestamps,
            final ListOffsetsRequestState requestState) {
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, requestState);
        if (timestampsToSearchByNode.isEmpty()) {
            throw new StaleMetadataException();
        }
        MultiNodeRequest multiNodeRequest = new MultiNodeRequest(timestampsToSearchByNode.size());
        requestState.addMultiNodeRequest(multiNodeRequest);

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                    .forConsumer(requireTimestamps, isolationLevel, false)
                    .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(entry.getValue()));

            log.debug("Sending ListOffsetRequest {} to broker {}", builder, node);

            NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                    builder,
                    Optional.ofNullable(node));
            unsentRequests.add(unsentRequest);
            unsentRequest.future().whenComplete((response, error) -> {
                if (error != null) {
                    log.error("Sending ListOffsetRequest {} to broker {} failed",
                            builder,
                            node,
                            error);
                    multiNodeRequest.resultFuture.completeExceptionally(error);
                } else {
                    ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                    log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                    onSendToNodeSuccess(lor, multiNodeRequest);
                }
            });
        }
        return unsentRequests;
    }

    private void onSendToNodeSuccess(final ListOffsetsResponse response,
                                     final MultiNodeRequest multiNodeRequest) {
        try {
            ListOffsetResult partialResult = offsetFetcherUtils.handleListOffsetResponse(response);
            multiNodeRequest.fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
            multiNodeRequest.partitionsToRetry.addAll(partialResult.partitionsToRetry);

            if (multiNodeRequest.expectedResponses.decrementAndGet() == 0) {
                ListOffsetResult result =
                        new ListOffsetResult(multiNodeRequest.fetchedTimestampOffsets,
                                multiNodeRequest.partitionsToRetry);
                multiNodeRequest.resultFuture.complete(result);
            }
        } catch (RuntimeException e) {
            multiNodeRequest.resultFuture.completeExceptionally(e);
        }
    }

    private static class ListOffsetsRequestState {

        private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        private final Map<TopicPartition, Long> remainingToSearch;
        private final List<MultiNodeRequest> multiNodeRequests;
        private final CompletableFuture<Map<TopicPartition, Long>> globalResult;
        final boolean requireTimestamps;
        final OffsetFetcherUtils offsetFetcherUtils;
        final IsolationLevel isolationLevel;

        private ListOffsetsRequestState(Map<TopicPartition, Long> timestampsToSearch,
                                        boolean requireTimestamps,
                                        OffsetFetcherUtils offsetFetcherUtils,
                                        IsolationLevel isolationLevel) {
            remainingToSearch = Collections.synchronizedMap(timestampsToSearch);
            fetchedOffsets = Collections.synchronizedMap(new HashMap<>());
            multiNodeRequests = Collections.synchronizedList(new ArrayList<>());
            globalResult = new CompletableFuture<>();
            this.requireTimestamps = requireTimestamps;
            this.offsetFetcherUtils = offsetFetcherUtils;
            this.isolationLevel = isolationLevel;
        }

        void addMultiNodeRequest(MultiNodeRequest multiNodeRequest) {
            multiNodeRequest.resultFuture.whenComplete((multiNodeResult, error) -> {
                // Done sending request to a set of known leaders
                multiNodeRequests.remove(multiNodeRequest);
                if (error == null) {
                    fetchedOffsets.putAll(multiNodeResult.fetchedOffsets);
                    remainingToSearch.keySet().retainAll(multiNodeResult.partitionsToRetry);
                    offsetFetcherUtils.updateSubscriptionState(
                            multiNodeResult.fetchedOffsets,
                            isolationLevel);

                    if (remainingToSearch.size() == 0) {
                        ListOffsetResult listOffsetResult = new ListOffsetResult(fetchedOffsets,
                                remainingToSearch.keySet());
                        globalResult.complete(listOffsetResult.offsetAndMetadataMap());
                    }
                } else {
                    // TODO: check if retriable errors should be considered/retried here
                    globalResult.completeExceptionally(error);
                }
            });

            multiNodeRequests.add(multiNodeRequest);
        }
    }

    private static class MultiNodeRequest {
        private final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets;
        private final Set<TopicPartition> partitionsToRetry;
        private final AtomicInteger expectedResponses;
        private final CompletableFuture<ListOffsetResult> resultFuture;

        private MultiNodeRequest(int nodeCount) {
            fetchedTimestampOffsets = Collections.synchronizedMap(new HashMap<>());
            partitionsToRetry = Collections.synchronizedSet(new HashSet<>());
            expectedResponses = new AtomicInteger(nodeCount);
            resultFuture = new CompletableFuture<>();
        }
    }

    /**
     * Group partitions by leader. Topic partitions from `timestampsToSearch` for which
     * the leader is not known are kept as `remainingToSearch` in the `requestState`
     *
     * @param timestampsToSearch The mapping from partitions to the target timestamps
     * @param requestState       Request state that will be extended by adding to its
     *                           `remainingToSearch` map all partitions for which the
     *                           request cannot be performed due to unknown leader (need
     *                           metadata update)
     */
    private Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> groupListOffsetRequests(
            Map<TopicPartition, Long> timestampsToSearch,
            final ListOffsetsRequestState requestState) {
        final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (!leaderAndEpoch.leader.isPresent()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate();
                requestState.remainingToSearch.put(tp, offset);
            } else {
                int currentLeaderEpoch = leaderAndEpoch.epoch.orElse(ListOffsetsResponse.UNKNOWN_EPOCH);
                partitionDataMap.put(tp, new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(tp.partition())
                        .setTimestamp(offset)
                        .setCurrentLeaderEpoch(currentLeaderEpoch));
            }
        }
        return offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap);
    }
}
