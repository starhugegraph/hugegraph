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

package com.baidu.hugegraph.traversal.algorithm;

import java.util.Iterator;
import java.util.Objects;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.records.PathsRecords;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

public class PathsTraverser extends HugeTraverser {

    public PathsTraverser(HugeGraph graph) {
        super(graph);
    }

    @Watched
    public PathSet paths(Id sourceV, Directions sourceDir,
                         Id targetV, Directions targetDir, String label,
                         int depth, long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(sourceDir, "source direction");
        E.checkNotNull(targetDir, "target direction");
        E.checkArgument(sourceDir == targetDir ||
                        sourceDir == targetDir.opposite(),
                        "Source direction must equal to target direction" +
                        " or opposite to target direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        PathSet paths = new PathSet();
        if (sourceV.equals(targetV)) {
            return paths;
        }

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, targetV, labelId,
                                            degree, capacity, limit);
        // We should stop early if find cycle or reach limit
        while (true) {
            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.forward(targetV, sourceDir);

            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }

            traverser.backward(sourceV, targetDir);
        }
        paths.addAll(traverser.paths());
        return paths;
    }

    private class Traverser {

        private final PathsRecords record;

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;

        private PathSet paths;

        public Traverser(Id sourceV, Id targetV, Id label,
                         long degree, long capacity, long limit) {
            this.record = new PathsRecords(false, sourceV, targetV);
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;

            this.paths = new PathSet();
        }

        /**
         * Search forward from source
         */
        @Watched
        public void forward(Id targetV, Directions direction) {
            Iterator<Edge> edges;

            this.record.startOneLayer(true);
            while (this.record.hasNextKey()) {
                Id vid = this.record.nextKey();
                if (vid.equals(targetV)) {
                    LOG.debug("out of index, cur {}. targetV {}", vid, targetV);
                    break;
                }

                edges = edgesOfVertex(vid, direction, this.label, this.degree, false);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    LOG.trace("Go forward, vid {}, edge {}, targetId {}",
                              vid, edge, target);
                    PathSet results = this.record.findPath(target, null,
                                                           true, false);
                    for (Path path : results) {
                        this.paths.add(path);
                        if (this.reachLimit() ||
                            Objects.equals(target, targetV)) {
                            LOG.debug("Find cycle, cur vid is {}, target is {}",
                                      target, targetV);
                            return;
                        }
                    }
                }
            }
            this.record.finishOneLayer();
        }

        /**
         * Search backward from target
         */
        @Watched
        public void backward(Id sourceV, Directions direction) {
            Iterator<Edge> edges;

            this.record.startOneLayer(false);
            while (this.record.hasNextKey()) {
                Id vid = this.record.nextKey();
                if (vid.equals(sourceV)) {
                    LOG.debug("out of index, cur {}. source {}", vid, sourceV);
                    break;
                }

                edges = edgesOfVertex(vid, direction, this.label, this.degree, false);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    LOG.trace("Go back, vid {}, edge {}, targetId {}",
                              vid, edge, target);
                    PathSet results = this.record.findPath(target, null,
                                                           true, false);
                    for (Path path : results) {
                        this.paths.add(path);
                        if (this.reachLimit() ||
                            Objects.equals(target, sourceV)) {
                            LOG.debug("Find cycle, cur vid is {}, source is {}",
                                      target, sourceV);
                            return;
                        }
                    }
                }
            }

            this.record.finishOneLayer();
        }

        public PathSet paths() {
            return this.paths;
        }

        private boolean reachLimit() {
            checkCapacity(this.capacity, this.record.accessed(), "paths");
            return this.limit != NO_LIMIT && this.paths.size() >= this.limit;
        }
    }
}
