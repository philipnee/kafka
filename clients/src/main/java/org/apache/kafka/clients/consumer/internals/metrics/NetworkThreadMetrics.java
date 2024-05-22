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
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.apache.kafka.common.metrics.stats.Frequency;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.NETWORK_METRICS_SUFFIX;

public class NetworkThreadMetrics {
    private Sensor pollTimeSensor;

    private Sensor backoffTimeSensor;

    private Sensor coordinatorPollSensor;
    private Sensor commitRequestManagerPollSensor;
    private Sensor heartbeatRequestManagerPollSensor;
    private Sensor fetchRequestManagerPollSensor;
    private Sensor offsetsRequestManagerPollSensor;
    private Sensor runSensor;
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

        MetricName pollRate =  metrics.metricName("poll-rate", metricGroupName,
                String.format("The number of %s per second", "network poll"));

        pollTimeSensor.add(pollTimeAvg, new Avg());
        pollTimeSensor.add(pollTimeMax, new Max());
        pollTimeSensor.add(pollRate,  new Rate(TimeUnit.SECONDS, new WindowedCount()));


        runSensor = metrics.sensor("poll-sum");
        MetricName pollRun = metrics.metricName("poll-run", metricGroupName,
            "The number of times the network thread loops");
        runSensor.add(pollRun, new CumulativeCount());

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
        backoffTimeSensor.add(noBackOff, new Frequencies(2, 0, Long.MAX_VALUE,
            freq("0", 0),
            freq("max", Long.MAX_VALUE)));

        backoffTimeSensor.add(backoffTimeAvg, new Avg());
        backoffTimeSensor.add(backoffTimeMax, new Max());

        // coordinator poll time sensor
        coordinatorPollSensor = metrics.sensor("coordinator-poll-time-sensor");
        MetricName coordinatorPollTimeAvg = metrics.metricName("coordinator-poll-time-avg",
            metricGroupName,
            "The average time taken for a coordinator poll request");
        MetricName coordinatorPollTimeMin = metrics.metricName("coordinator-poll-time-hist",
            metricGroupName,
            "The number of times coordinator return 0 poll time");
        coordinatorPollSensor.add(coordinatorPollTimeAvg, new Avg());
        coordinatorPollSensor.add(coordinatorPollTimeMin, new Frequencies(2, 0, Long.MAX_VALUE,
            freq("0", 0),
            freq("max", Long.MAX_VALUE)));

        // commit request manager poll time sensor
        commitRequestManagerPollSensor = metrics.sensor("commit-poll-time-sensor");
        MetricName commitRequestManagerPollTimeAvg = metrics.metricName("commit-poll-time-avg",
            metricGroupName,
            "The average time taken for a commit request manager poll request");
        MetricName commitRequestManagerPollTimeHist = metrics.metricName("commit-poll-time-hist",
            metricGroupName,
            "The number of times commit request manager return 0 poll time");
        commitRequestManagerPollSensor.add(commitRequestManagerPollTimeAvg, new Avg());
        commitRequestManagerPollSensor.add(commitRequestManagerPollTimeHist, new Frequencies(2, 0, Long.MAX_VALUE,
            freq("0", 0),
            freq("max", Long.MAX_VALUE)));

        // heartbeat request manager poll time sensor
        heartbeatRequestManagerPollSensor = metrics.sensor("heartbeat-poll-time-sensor");
        MetricName heartbeatRequestManagerPollTimeAvg = metrics.metricName("heartbeat-poll-time-avg",
            metricGroupName,
            "The average time taken for a heartbeat request manager poll request");
        heartbeatRequestManagerPollSensor.add(heartbeatRequestManagerPollTimeAvg, new Avg());
        heartbeatRequestManagerPollSensor.add(new Frequencies(5, 0, Long.MAX_VALUE,
            freq("0", 0),
            freq("max", Long.MAX_VALUE)));

        // fetch request manager poll time sensor
        fetchRequestManagerPollSensor = metrics.sensor("fetch-poll-time-sensor");
        MetricName fetchRequestManagerPollTimeAvg = metrics.metricName("fetch-poll-time-avg",
            metricGroupName,
            "The average time taken for a fetch request manager poll request");
        MetricName fetchRequestManagerPollTimeHist = metrics.metricName("fetch-poll-time-hist",
            metricGroupName,
            "The number of times fetch request manager return 0 poll time");
        fetchRequestManagerPollSensor.add(fetchRequestManagerPollTimeAvg, new Avg());
        fetchRequestManagerPollSensor.add(fetchRequestManagerPollTimeHist, new Frequencies(2, 0, Long.MAX_VALUE,
            freq("0", 0),
            freq("max", Long.MAX_VALUE)));

        // offsets request manager poll time sensor
        offsetsRequestManagerPollSensor = metrics.sensor("offsets-poll-time-sensor");
        MetricName offsetsRequestManagerPollTimeAvg = metrics.metricName("offsets-poll-time-avg",
            metricGroupName,
            "The average time taken for a offsets request manager poll request");
        MetricName offsetsRequestManagerPollTimeHist = metrics.metricName("offsets-poll-time-hist",
            metricGroupName,
            "The number of times offsets request manager return 0 poll time");
        offsetsRequestManagerPollSensor.add(offsetsRequestManagerPollTimeAvg, new Avg());
        offsetsRequestManagerPollSensor.add(offsetsRequestManagerPollTimeHist, new Frequencies(2, 0, Long.MAX_VALUE,
            freq("0", 0),
            freq("max", Long.MAX_VALUE)));

        commitRequestManagerPollSensor = metrics.sensor("commit-poll-time-sensor");
    }

    protected MetricName name(String metricName) {
        return new MetricName(metricName, "group-id", "desc", Collections.emptyMap());
    }

    protected Frequency freq(String name, double value) {
        return new Frequency(name(name), value);
    }

    public void recordPollTime(String rmClass, long pollTimeMs) {
        switch (rmClass) {
            case "org.apache.kafka.clients.consumer.internals.CommitRequestManager":
                commitRequestManagerPollSensor.record(pollTimeMs);
                break;
            case "org.apache.kafka.clients.consumer.internals.HeartbeatRequestManager":
                heartbeatRequestManagerPollSensor.record(pollTimeMs);
                break;
            case "org.apache.kafka.clients.consumer.internals.FetchRequestManager":
                fetchRequestManagerPollSensor.record(pollTimeMs);
                break;
            case "org.apache.kafka.clients.consumer.internals.OffsetsRequestManager":
                offsetsRequestManagerPollSensor.record(pollTimeMs);
                break;
            case "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager":
                coordinatorPollSensor.record(pollTimeMs);
                break;
        }
        pollTimeSensor.record(pollTimeMs);
    }

    public void recordBackoffTime(long waitTime) {
        backoffTimeSensor.record(waitTime);
    }
    public void recordRun() {
        runSensor.record();
    }
}
