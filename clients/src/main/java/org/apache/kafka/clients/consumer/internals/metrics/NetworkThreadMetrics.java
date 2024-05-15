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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.apache.kafka.common.metrics.stats.Frequency;
import org.apache.kafka.common.metrics.stats.Max;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.NETWORK_METRICS_SUFFIX;

public class NetworkThreadMetrics {
    private Sensor pollTimeSensor;

    private Sensor backoffTimeSensor;

    private Sensor requestManagerPollSensor;
    private final Metrics metrics;

    public NetworkThreadMetrics(Metrics metrics) {
        this.metrics = metrics;
        final String metricGroupName = CONSUMER_METRIC_GROUP_PREFIX + NETWORK_METRICS_SUFFIX;

        pollTimeSensor = metrics.sensor("poll-time");

        MetricName pollTimeAvg = metrics.metricName("poll-time-avg",
            metricGroupName,
            "The average time taken for a poll request");
        MetricName pollTimeMax = metrics.metricName("poll-time-max",
            metricGroupName,
            "The max time taken for a poll request");

        pollTimeSensor.add(pollTimeAvg, new Avg());
        pollTimeSensor.add(pollTimeMax, new Max());

        backoffTimeSensor = metrics.sensor("backoff-time");
        MetricName backoffTimeAvg = metrics.metricName("backoff-time-avg",
            metricGroupName,
            "The average backoff time");
        MetricName backoffTimeMax = metrics.metricName("backoff-time-max",
            metricGroupName,
            "The max backoff time");

        MetricName noBackOff = metrics.metricName("no-backoff",
            metricGroupName,
            "The number of times no backoff was needed");

        // fuck this
        backoffTimeSensor.add(new Frequencies(2, 0, Long.MAX_VALUE,
            new Frequency(noBackOff, 0),
            new Frequency(noBackOff, Long.MAX_VALUE)));

        backoffTimeSensor.add(backoffTimeAvg, new Avg());
        backoffTimeSensor.add(backoffTimeMax, new Max());

        MetricName coordinatorPollTimeAvg = metrics.metricName("coordinator-poll-time-avg",
            metricGroupName,
            "average coordinator poll time");
        MetricName commitPollTimeAvg = metrics.metricName("commit-poll-time-avg",
            metricGroupName,
            "average commit poll time");
        MetricName hbPollTimeAvg = metrics.metricName("hb-poll-time-avg",
            metricGroupName,
            "average hb poll time");

        requestManagerPollSensor = metrics.sensor("poll-time-sensor");
        MetricName pta = metrics.metricName("poll-time-avg",
            metricGroupName,
            "The average time taken for a poll request");

        MetricName ptm = metrics.metricName("poll-time-max",
            metricGroupName,
            "The max time taken for a poll request");

        MetricName ztm = metrics.metricName("zero-poll-time",
            metricGroupName,
            "The number of time poll time returns zero");

        MetricName mtm = metrics.metricName("max-poll-time",
            metricGroupName,
            "The number of time poll time returns max value");

        requestManagerPollSensor.add(pta, new Avg());
        requestManagerPollSensor.add(ptm, new Max());
        requestManagerPollSensor.add(new Frequencies(2, 0, Long.MAX_VALUE,
            new Frequency(ztm, 0),
            new Frequency(mtm, Long.MAX_VALUE)));

    }

    public void recordPollTime(long pollTimeMs) {
        pollTimeSensor.record(pollTimeMs);
    }

    public void recordBackoffTime(long waitTime) {
        backoffTimeSensor.record(waitTime);
    }

    public void recordCoordinatorPollTime(long pollTimeMs) {
        requestManagerPollSensor.record(pollTimeMs);
    }

    public void recordCommitPollTime(long pollTimeMs) {
    }

    public void recordHeartbeatPollTime(long pollTimeMs) {
    }
}
