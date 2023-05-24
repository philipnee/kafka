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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ListOffsetsRequestManagerTest {

    private ListOffsetsRequestManager requestManager;
    private ConsumerMetadata metadata;
    private SubscriptionState subscriptionState;
    private MockTime time;
    private static final String TEST_TOPIC = "t1";
    private static final int TEST_PARTITION = 1;
    private static final Node LEADER_1 = new Node(0, "localhost", 9092);
    private static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_COMMITTED;

    @BeforeEach
    public void setup() {
        metadata = mock(ConsumerMetadata.class);
        subscriptionState = mock(SubscriptionState.class);
        this.time = new MockTime(0);
        requestManager = new ListOffsetsRequestManager(subscriptionState,
                metadata, DEFAULT_ISOLATION_LEVEL, mock(Time.class),
                mock(ApiVersions.class), new LogContext());
    }

    @Test
    public void testListOffsetsRequest_Success() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> partitionsOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
        long offset = ListOffsetsRequest.EARLIEST_TIMESTAMP;
        partitionsOffsets.put(tp, offset);

        expectSuccessfulRequest();
        CompletableFuture<Map<TopicPartition, Long>> result = requestManager.fetchOffsets(
                partitionsOffsets.keySet(),
                ListOffsetsRequest.EARLIEST_TIMESTAMP,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPoll(res);

        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        unsentRequest.future().complete(buildListOffsetsClientResponse(unsentRequest,
                partitionsOffsets));

        verifyRequestSuccessfullyCompleted(tp, offset, result, partitionsOffsets);
    }

    @Test
    public void testListOffsetsRequest_FailAndRetrySuccess() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> partitionsOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
        long offset = 1L;
        partitionsOffsets.put(tp, offset);

        // List offsets request that fails with unknown leader
        expectFailedRequest_MissingLeader();
        CompletableFuture<Map<TopicPartition, Long>> fetchOffsetsFuture =
                requestManager.fetchOffsets(partitionsOffsets.keySet(),
                        ListOffsetsRequest.EARLIEST_TIMESTAMP,
                        false);
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(1, requestManager.requestsToRetry());
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertFalse(fetchOffsetsFuture.isDone());

        // Mock metadata update. Previously failed request should be retried and succeed
        expectSuccessfulRequest();
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        // Request manager poll
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPoll(retriedPoll);

        // Complete poll result
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        unsentRequest.future().complete(buildListOffsetsClientResponse(unsentRequest, partitionsOffsets));

        verifyRequestSuccessfullyCompleted(tp, offset, fetchOffsetsFuture, partitionsOffsets);

    }

    private void expectSuccessfulRequest() {
        when(metadata.currentLeader(any(TopicPartition.class))).thenReturn(testLeaderEpoch());
        when(metadata.fetch()).thenReturn(testClusterMetadata());
        when(subscriptionState.isAssigned(any(TopicPartition.class))).thenReturn(true);
    }

    private void expectFailedRequest_MissingLeader() {
        when(metadata.currentLeader(any(TopicPartition.class))).thenReturn(
                new Metadata.LeaderAndEpoch(Optional.empty(), Optional.of(1)));
        when(metadata.fetch()).thenReturn(testClusterMetadata());
        when(subscriptionState.isAssigned(any(TopicPartition.class))).thenReturn(true);
    }

    private void verifySuccessfulPoll(NetworkClientDelegate.PollResult pollResult) {
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(1, pollResult.unsentRequests.size());
    }

    private void verifyRequestSuccessfullyCompleted(TopicPartition tp,
                                                    long offset,
                                                    CompletableFuture<Map<TopicPartition, Long>> actualResult,
                                                    Map<TopicPartition, Long> expectedResult) throws ExecutionException, InterruptedException {
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        assertTrue(actualResult.isDone());
        assertFalse(actualResult.isCompletedExceptionally());
        assertEquals(expectedResult, actualResult.get());

        verify(subscriptionState).updateLastStableOffset(tp, offset);
    }

    private Metadata.LeaderAndEpoch testLeaderEpoch() {
        return new Metadata.LeaderAndEpoch(Optional.of(LEADER_1),
                Optional.of(1));
    }

    private Cluster testClusterMetadata() {
        List<PartitionInfo> partitions = Collections.singletonList(
                new PartitionInfo(TEST_TOPIC, TEST_PARTITION, LEADER_1, null, null));
        return new Cluster("clusterId", Collections.singletonList(LEADER_1), partitions, Collections.emptySet(),
                Collections.emptySet());
    }

    private ClientResponse buildListOffsetsClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, Long> partitionsOffsets) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof ListOffsetsRequest);
        ListOffsetsRequest offsetFetchRequest = (ListOffsetsRequest) abstractRequest;
        ListOffsetsResponse response = buildListOffsetsResponse(partitionsOffsets);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FETCH, offsetFetchRequest.version(), "", 1),
                request.callback(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                false,
                null,
                null,
                response
        );
    }

    private ListOffsetsResponse buildListOffsetsResponse(Map<TopicPartition, Long> offsets) {
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> offsetsTopicResponses = new ArrayList<>();
        offsets.forEach((tp, offset) -> offsetsTopicResponses.add(
                ListOffsetsResponse.singletonListOffsetsTopicResponse(tp, Errors.NONE, -1L,
                        offset, 123)));

        ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(offsetsTopicResponses);

        return new ListOffsetsResponse(responseData);
    }
}
