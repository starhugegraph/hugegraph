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

package com.baidu.hugegraph.kafka;

import java.nio.ByteBuffer;
import java.util.Properties;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.kafka.consumer.StandardConsumer;
import com.baidu.hugegraph.kafka.topic.HugeGraphMutateTopicBuilder;
import com.baidu.hugegraph.util.Log;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

/**
 * Used to consume HugeGraphMutateTopic, that is used to apply mutate to storage
 * This consumer is used in Slave cluster only
 * @author Scorpiour
 * @since 2022-01-22
 */
public class SyncMutateConsumer extends StandardConsumer {

    private static final Logger LOG = Log.logger(SyncMutateConsumer.class);

    private GraphManager manager;
    private HugeGraph graph;

    protected SyncMutateConsumer(Properties props) {
        super(props);
    }

    protected void setGraphManager(GraphManager manager) {
        this.manager = manager;
    }

    public void consume(HugeGraph graph) {
        this.graph = graph;
        this.consume();
    }

    @Override
    protected boolean handleRecord(ConsumerRecord<String, ByteBuffer> record) {

        LOG.info("====> Scorpiour: handle record of apply mutation, key {} , size {}", record.key(), record.value().array().length);

        if (BrokerConfig.getInstance().needKafkaSyncStorage()) {

            LOG.info("====> Scorpiour: going to pre-check");
            if (null == this.manager && null == this.graph) {
                LOG.info("====> Scorpiour: pre-check failed! {}, {}", this.manager, this.graph);
                return true;
            }
            LOG.info("====> Scorpiour: going to extract graph info");
            String[] graphInfo = HugeGraphMutateTopicBuilder.extractGraphs(record);
            String graphSpace = graphInfo[0];
            String graphName = graphInfo[1];

            LOG.info("====> Scorpiour: extract graphSpace {}, graph {}", graphSpace, graphName);

            HugeGraph graph = manager.graph(graphSpace, graphName);
            if (null == graph) {
                LOG.info("====> Scorpiour: graph is {}, consume failed!");
                return false;
            }
            BackendMutation mutation = HugeGraphMutateTopicBuilder.buildMutation(record.value());
            LOG.info("====> Scorpiour: going to apply mutation");
            graph.applyMutation(mutation);
            LOG.info("====> Scorpiour: going to commit");
            graph.tx().commit();
            LOG.info("====> Scorpiour: commit done");
            return true;
        }
        return false;

    }

    @Override
    public void close() {
        super.close();
    }
}
