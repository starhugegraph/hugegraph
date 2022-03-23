/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.kafka.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.baidu.hugegraph.kafka.topic.SyncConfTopic;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Kafka consumer encapsulation & proxy
 * @author Scorpiour
 * @since 2022-01-18
 */
public abstract class ConsumerClient<K, V> {

    protected static final HugeGraphLogger LOGGER = 
        Log.getLogger(ConsumerClient.class);

    public final String topic;

    protected final KafkaConsumer<K, V> consumer;

    private volatile boolean closing = false;
    private final ExecutorService asyncExecutor;

    protected ConsumerClient(Properties props) {
        String topic = props.getProperty("topic");
        if (Strings.isNullOrEmpty(topic)) {
            throw new InstantiationError("Topic may not be null");
        }

        this.topic = topic;
        asyncExecutor = Executors.newSingleThreadExecutor();
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(ImmutableList.of(topic));
    }

    public final Map<TopicPartition, Long> getConsumerStackInfo() {
        Map<String, List<PartitionInfo>> topicMap =  consumer.listTopics();
        List<TopicPartition> tpList = new ArrayList<>();
        List<PartitionInfo> partitions = topicMap.get(this.topic);
        partitions.forEach((partition) -> {
            tpList.add(new TopicPartition(partition.topic(), partition.partition()));
        });

        Map<TopicPartition, Long> endMap = consumer.endOffsets(tpList);

        Map<TopicPartition, OffsetAndMetadata> committedMap = consumer.committed(new HashSet<>(tpList));

        Map<TopicPartition, Long> stackedCount = new HashMap<>();

        endMap.entrySet().forEach((entry) -> {
            OffsetAndMetadata meta = committedMap.get(entry.getKey());
            long end = entry.getValue();
            long committed = meta.offset();
            long diff = committed - end;

            stackedCount.put(entry.getKey(), diff);

            LOGGER.logCustomDebug("stacked {}", "Scorpiour", diff);
        });

        return stackedCount;
    }

    public final void consume() {
        asyncExecutor.submit(() -> {
            while(!closing) {
                boolean commit = false;
                try {
                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1000));

                    if (records.count() > 0) {
                        for(ConsumerRecord<K, V> record : records.records(topic)) {
                            try {
                                commit = handleRecord(record);
                            } catch (Exception e) {
                                LOGGER.logCustomDebug("Consume topic failed", this.getClass().getName(), record);
                            }
                        }
                    }
                } catch (Throwable t) {
                    
                } finally {
                    if (commit) {
                        consumer.commitAsync();
                    }
                }
            }
        });
    }

    protected abstract boolean handleRecord(ConsumerRecord<K, V> record);

    public void close() {
        this.closing = true;
        asyncExecutor.shutdownNow();
    }
}
