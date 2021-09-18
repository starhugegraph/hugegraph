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

package com.baidu.hugegraph.api.traversers;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.steps.Steps;
import com.baidu.hugegraph.type.define.Directions;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TraverserAPI extends API {

    protected static EdgeStep step(HugeGraph graph, Step step) {
        return new EdgeStep(graph, step.direction, step.labels, step.properties,
                            step.maxDegree, step.skipDegree);
    }

    protected static Steps steps(HugeGraph graph, Directions directions,
                                 String label, long maxDegree) {
        Map<String, Map<String, Object>> eSteps = new HashMap<>();
        eSteps.put(label, null);
        return new Steps(graph, directions, eSteps, null,
                         maxDegree, 0);
    }

    protected static Steps steps(HugeGraph graph, EVSteps steps) {
        Map<String, Map<String, Object>> eSteps = new HashMap<>();
        Map<String, Map<String, Object>> vSteps = new HashMap<>();

        if (steps.eSteps != null) {
            Iterator<EVStepEntity> itr = steps.eSteps.iterator();
            while (itr.hasNext()) {
                EVStepEntity item = itr.next();
                eSteps.put(item.label, item.properties);
            }
        }

        if (steps.vSteps != null) {
            Iterator<EVStepEntity> itr = steps.vSteps.iterator();
            while (itr.hasNext()) {
                EVStepEntity item = itr.next();
                vSteps.put(item.label, item.properties);
            }
        }

        return new Steps(graph, steps.direction, eSteps, vSteps,
                steps.maxDegree, steps.skipDegree);
    }

    protected static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "maxDegree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree);
        }
    }

    protected static class EVStepEntity {
        @JsonProperty("label")
        public String label;

        @JsonProperty("properties")
        public Map<String, Object> properties;

        @Override
        public String toString() {
            return String.format("EVStepEntity{label=%s,properties=%s}",
                                 this.label, this.properties);
        }
    }

    protected static class EVSteps {
        @JsonProperty("direction")
        public Directions direction;

        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);

        @JsonProperty("skip_degree")
        public long skipDegree = 0L;

        @JsonProperty("edge_steps")
        public List<EVStepEntity> eSteps;

        @JsonProperty("vertex_steps")
        public List<EVStepEntity> vSteps;

        @Override
        public String toString() {
            return String.format("Steps{direction=%s,maxDegree=%s," +
                                 "skipDegree=%s,eSteps=%s,vSteps=%s}",
                                 this.direction, this.maxDegree,
                                 this.skipDegree, this.eSteps, this.vSteps);
        }
    }
}
