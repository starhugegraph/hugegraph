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

 /**
  * Splitted from StandardTaskScheduler on 2022-1-4 by Scorpiour
  * Provide general transaction for queries;
  * @since 2022-01-04
  */
package com.baidu.hugegraph.task;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask.P;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.DataType;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.List;

public class TaskAndResultTransaction extends TaskTransaction {

    public static final String TASKRESULT = HugeTaskResult.P.TASKRESULT;

    /**
     * Task transactions, for persistence
     */
    protected volatile TaskAndResultTransaction taskTx = null;

    public TaskAndResultTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);
    }

    public HugeVertex constructTaskVertex(HugeTask<?> task) {
        if (!this.graph().existsVertexLabel(TASK)) {
            throw new HugeException("Schema is missing for task(%s) '%s'",
                                    task.id(), task.name());
        }

        return this.constructVertex(false, task.asArrayWithoutResult());
    }

    public HugeVertex constructTaskResultVertex(HugeTaskResult taskResult) {

        if (!this.graph().existsVertexLabel(TASKRESULT)) {
            throw new HugeException("Schema is missing for task result");
        }

        return this.constructVertex(false, taskResult.asArray());
    }

    public void initSchema() {

        super.initSchema();

        HugeGraph graph = this.graph();
        if (!this.existVertexLabel(TASKRESULT)) {
            String[] properties = this.initTaskResultProperties();

            // Create vertex label '~taskresult'
            VertexLabel label = graph.schema().vertexLabel(TASKRESULT)
                                     .properties(properties)
                                     .nullableKeys(HugeTaskResult.P.RESULT)
                                     .useAutomaticId()
                                     .enableLabelIndex(true)
                                     .build();
            this.params().schemaTransaction().addVertexLabel(label);

            this.createIndexLabel(label, HugeTaskResult.P.TASKID);

        }
    }

    private IndexLabel createIndexLabel(VertexLabel label, String field) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        String name = Graph.Hidden.hide("taskresult-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name)
                                      .on(HugeType.VERTEX_LABEL, TASKRESULT)
                                      .by(field)
                                      .build();
        this.params().schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }


    private String[] initTaskResultProperties() {
        List<String> props = new ArrayList<>();
        props.add(createPropertyKey(HugeTaskResult.P.TASKID, DataType.INT));
        props.add(createPropertyKey(HugeTaskResult.P.RESULT, DataType.BLOB));

        return props.toArray(new String[0]);
    }
}
