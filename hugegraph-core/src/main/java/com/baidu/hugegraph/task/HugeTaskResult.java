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

package com.baidu.hugegraph.task;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.util.Blob;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HugeTaskResult {

    private final Id taskId;
    private volatile String result;

    private static final float DECOMPRESS_RATIO = 10.0F;

    public HugeTaskResult(Id taskId) {
        this.taskId = taskId;
        this.result = null;
    }

    public Id taskId() {
       return this.taskId;
    }

    public void result(String result) {
        this.result = result;
    }

    public String result() {
        return this.result;
    }

    protected synchronized Object[] asArray() {

        List<Object> list = new ArrayList<>(4);

        list.add(T.label);
        list.add(HugeTaskResult.P.TASKRESULT);

        list.add(P.TASKID);
        list.add(this.taskId);

        if (this.result != null) {
            byte[] bytes = StringEncoding.compress(this.result);
            list.add(HugeTaskResult.P.RESULT);
            list.add(bytes);
        }

        return list.toArray();
    }

    public static HugeTaskResult fromVertex(Vertex vertex) {
        HugeTaskResult taskResult = new HugeTaskResult((Id) vertex.id());
        for (Iterator<VertexProperty<Object>> iter = vertex.properties();
             iter.hasNext();) {
            VertexProperty<Object> prop = iter.next();
            taskResult.property(prop.key(), prop.value());
        }
        return taskResult;
    }

    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case HugeTaskResult.P.RESULT:
                this.result = StringEncoding.decompress(((Blob) value).bytes(),
                                                        DECOMPRESS_RATIO);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    public static final class P {

        public static final String TASKRESULT = Graph.Hidden.hide("taskresult");

        public static final String TASKID = "~result_taskid";
        public static final String RESULT = "~result_result";

        public static String unhide(String key) {
            final String prefix = Graph.Hidden.hide("result_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }
}
