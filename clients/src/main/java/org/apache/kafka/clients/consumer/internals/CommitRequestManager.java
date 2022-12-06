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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

public class CommitRequestManager implements RequestManager {
    private final Queue<StagedCommit> stagedCommits;
    private final SubscriptionState subscriptionState;
    private final Logger log;
    private final Optional<AutoCommitState> autoCommitState;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final GroupStateManager groupState;

    public CommitRequestManager(
            final Time time,
            final LogContext logContext,
            final SubscriptionState subscriptionState,
            final boolean autoCommitEnabled,
            final long autoCommitInterval,
            final CoordinatorRequestManager coordinatorRequestManager,
            final GroupStateManager groupState) {
        this.log = logContext.logger(getClass());
        this.stagedCommits = new LinkedList<>();
        if (autoCommitEnabled) {
            this.autoCommitState = Optional.of(new AutoCommitState(time, autoCommitInterval));
        } else {
            this.autoCommitState = Optional.empty();
        }
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.groupState = groupState;
        this.subscriptionState = subscriptionState;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        maybeAutoCommit(currentTimeMs);

        if (stagedCommits.isEmpty()) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, new ArrayList<>());
        }

        Queue<NetworkClientDelegate.UnsentRequest> unsentCommitRequests = new LinkedList<>();
        while (!stagedCommits.isEmpty()) {
            unsentCommitRequests.add(toUnsentRequest(stagedCommits.poll()));
        }
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, new ArrayList<>(unsentCommitRequests));
    }

    public void maybeAutoCommit(final long currentTimeMs) {
        if (!autoCommitState.isPresent()) {
            return;
        }

        AutoCommitState autocommit = autoCommitState.get();
        if (autocommit.canSendAutocommit(currentTimeMs)) {
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptionState.allConsumed();
            log.debug("Auto-committing offsets {}", allConsumedOffsets);
            stagedCommits.add(
                    new StagedCommit(
                            allConsumedOffsets,
                            new AutoCommitRequestHandler(allConsumedOffsets),
                            groupState.groupId,
                            groupState.groupInstanceId.orElse(null),
                            groupState.generation));
            autocommit.reset();
        }
    }

    private NetworkClientDelegate.UnsentRequest toUnsentRequest(final StagedCommit poll) {
        return poll.toUnsentRequest();
    }

    public void add(final Map<TopicPartition, OffsetAndMetadata> offsets,
                    final OffsetCommitCallback callback) {
        this.stagedCommits.add(new StagedCommit(offsets,
                callback,
                groupState.groupId,
                groupState.groupInstanceId.orElse(null),
                groupState.generation));
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    private class AutoCommitRequestHandler extends NetworkClientDelegate.AbstractRequestFutureCompletionHandler {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public AutoCommitRequestHandler(final Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void handleResponse(ClientResponse r, Throwable t) {
            if (t != null) {
                // TODO: rebase
                Exception e = new Exception(t);
                //if (e instanceof RetriableCommitFailedException) {
                if (r == null) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", offsets,
                            e);
                    autoCommitState.get().reset();
                } else {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, e.getMessage());
                }
            } else {
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        }
    }

    private static class OffsetCommitRequestHandler extends NetworkClientDelegate.AbstractRequestFutureCompletionHandler {
        private final Optional<OffsetCommitCallback> callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public OffsetCommitRequestHandler(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                          final OffsetCommitCallback callback) {
            this.callback = Optional.ofNullable(callback);
            this.offsets = offsets;
        }

        @Override
        public void handleResponse(ClientResponse r, Throwable t) {
            if (this.callback.isPresent()) {
                callback.get().onComplete(offsets, new RuntimeException(t));
            }
        }
    }

    private class StagedCommit {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Optional<NetworkClientDelegate.AbstractRequestFutureCompletionHandler> callback;
        private final String groupId;
        private final GroupStateManager.Generation generation;
        private final String groupInstanceId;

        public StagedCommit(final Map<TopicPartition, OffsetAndMetadata> offsets,
                            final NetworkClientDelegate.AbstractRequestFutureCompletionHandler callback,
                            final String groupId,
                            final String groupInstanceId,
                            final GroupStateManager.Generation generation) {
            this.offsets = offsets;
            this.callback = Optional.ofNullable(callback);
            this.groupId = groupId;
            this.generation = generation;
            this.groupInstanceId = groupInstanceId;
        }

        public StagedCommit(final Map<TopicPartition, OffsetAndMetadata> offsets,
                            final OffsetCommitCallback callback,
                            final String groupId,
                            final String groupInstanceId,
                            final GroupStateManager.Generation generation) {
            this(offsets, new OffsetCommitRequestHandler(offsets, callback), groupId, groupInstanceId, generation);
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {
            Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = entry.getValue();
                /* TODO: handled it else where in the callback
                if (offsetAndMetadata.offset() < 0) {
                    return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
                }
                 */

                OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                        .getOrDefault(topicPartition.topic(),
                                new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                        .setName(topicPartition.topic())
                        );

                topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                        .setPartitionIndex(topicPartition.partition())
                        .setCommittedOffset(offsetAndMetadata.offset())
                        .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                        .setCommittedMetadata(offsetAndMetadata.metadata())
                );
                requestTopicDataMap.put(topicPartition.topic(), topic);
            }

            OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                    new OffsetCommitRequestData()
                            .setGroupId(this.groupId)
                            .setGenerationId(generation.generationId)
                            .setMemberId(generation.memberId)
                            .setGroupInstanceId(groupInstanceId)
                            .setTopics(new ArrayList<>(requestTopicDataMap.values())));
            return NetworkClientDelegate.UnsentRequest.makeUnsentRequest(
                    null,
                    builder,
                    callback.orElse(new OffsetCommitRequestHandler(offsets, new DefaultOffsetCommitCallback())),
                    coordinatorRequestManager.coordinator());
        }
    }

    static class AutoCommitState {
        private final Timer timer;
        private final long autoCommitInterval;

        public AutoCommitState(
                final Time time,
                final long autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            this.timer = time.timer(autoCommitInterval);
        }

        public boolean canSendAutocommit(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
            return this.timer.isExpired();
        }

        public void reset() {
            this.timer.reset(autoCommitInterval);
        }
    }
}
