/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.KAFKA_CONNECT_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static java.util.Objects.requireNonNull;

public class ReadKafkaConnectP<T> extends AbstractProcessor implements DynamicMetricsProvider {

    private final ConnectorWrapper connectorWrapper;
    private final EventTimeMapper<T> eventTimeMapper;
    private final FunctionEx<SourceRecord, T> projectionFn;
    private TaskRunner taskRunner;
    private boolean snapshotInProgress;
    private Traverser<Entry<BroadcastKey<String>, State>> snapshotTraverser;
    private boolean snapshotsEnabled;
    private int processorIndex;
    private Traverser<?> traverser = Traversers.empty();
    private final LocalKafkaConnectStatsImpl localKafkaConnectStats = new LocalKafkaConnectStatsImpl();

    public ReadKafkaConnectP(@Nonnull ConnectorWrapper connectorWrapper,
                             @Nonnull EventTimePolicy<? super T> eventTimePolicy,
                             @Nonnull FunctionEx<SourceRecord, T> projectionFn) {
        requireNonNull(connectorWrapper, "connectorWrapper is required");
        requireNonNull(eventTimePolicy, "eventTimePolicy is required");
        requireNonNull(projectionFn, "projectionFn is required");
        this.connectorWrapper = connectorWrapper;
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.projectionFn = projectionFn;
        eventTimeMapper.addPartitions(1);
    }

    @Override
    protected void init(@Nonnull Context context) {
        processorIndex = context.globalProcessorIndex();
        taskRunner = connectorWrapper.createTaskRunner(processorIndex);
        snapshotsEnabled = context.snapshottingEnabled();

        NodeEngineImpl nodeEngine = getNodeEngine(context.hazelcastInstance());
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (snapshotInProgress) {
            return false;
        }
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        long start = Timer.nanos();
        List<SourceRecord> sourceRecords = taskRunner.poll();
        long durationInNanos = Timer.nanosElapsed(start);
        localKafkaConnectStats.addSourceRecordPollDuration(Duration.ofNanos(durationInNanos));
        localKafkaConnectStats.incrementSourceRecordPoll(sourceRecords.size());
        this.traverser = sourceRecords.isEmpty() ? eventTimeMapper.flatMapIdle() :
                traverseIterable(sourceRecords)
                        .flatMap(rec -> {
                            long eventTime = rec.timestamp() == null ?
                                    EventTimeMapper.NO_NATIVE_TIME
                                    : rec.timestamp();
                            T projectedRecord = projectionFn.apply(rec);
                            taskRunner.commitRecord(rec);
                            return eventTimeMapper.flatMapEvent(projectedRecord, 0, eventTime);
                        });
        emitFromTraverser(traverser);
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!snapshotsEnabled) {
            return true;
        }
        snapshotInProgress = true;
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.singleton(entry(snapshotKey(), taskRunner.createSnapshot()))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        getLogger().finest("Finished saving snapshot");
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    private BroadcastKey<String> snapshotKey() {
        return broadcastKey("snapshot-" + processorIndex);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        boolean forThisProcessor = snapshotKey().equals(key);
        if (forThisProcessor) {
            taskRunner.restoreSnapshot((State) value);
        }
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        try {
            if (success) {
                taskRunner.commit();
            }
        } finally {
            snapshotInProgress = false;
        }
        return true;
    }


    @Override
    public void close() {
        if (taskRunner != null) {
            taskRunner.stop();
        }
    }

    public Map<String, LocalKafkaConnectStats> getStats() {
        Map<String, LocalKafkaConnectStats> connectStats = new HashMap<>();
        if (taskRunner != null) {
            connectStats.put(taskRunner.name(), localKafkaConnectStats);
        }
        return connectStats;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        if (taskRunner != null) {
            descriptor.copy().withTag("task.runner", taskRunner.name());
        }
        provide(descriptor, context, KAFKA_CONNECT_PREFIX, getStats());
    }
}
