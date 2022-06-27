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
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.records.KneighborRecords;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.baidu.hugegraph.traversal.algorithm.steps.Steps;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

public class KneighborTraverser extends OltpTraverser {

    public KneighborTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth,
                             long degree, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-neighbor max_depth");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        Set<Id> all = newSet();

        latest.add(sourceV);

        while (depth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            latest = this.adjacentVerticesBatch(sourceV, latest, dir, labelId,
                                           all, degree, remaining);
            all.addAll(latest);
            if (reachLimit(limit, all.size())) {
                break;
            }
        }

        return all;
    }

    public KneighborRecords customizedKneighbor(Id source, Steps steps,
                                                int maxDepth, long limit,
                                                boolean withEdge) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-neighbor max_depth");
        checkLimit(limit);

        boolean concurrent = maxDepth >= this.concurrentDepth();

        KneighborRecords records = new KneighborRecords(RecordType.INT,
                                                        concurrent,
                                                        source, true);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertexAF(v, steps, false);

            while (!this.reachLimit(limit, records.size()) && edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                this.edgeIterCounter++;
                Id target = edge.id().otherVertexId();
                records.addPath(v, target);
                if(withEdge) {
                    // for breadth, we have to collect all edge during traversal,
                    // to avoid over occupy for memory, we collect edgeId only.
                    records.addEdgeId(edge.id());
                }
            }
        };

        while (maxDepth-- > 0) {
            records.startOneLayer(true);
            traverseIdsKneighbor(records.keys(), steps, consumer, concurrent,
                                 records, limit, withEdge);
            records.finishOneLayer();
            if (this.reachLimit(limit, records.size())) {
                break;
            }
        }

        if (withEdge) {
            // we should filter out unused-edges for breadth first algorithm.
            records.filterUnusedEdges(limit);
        }

        return records;
    }

    public KneighborRecords multiKneighbors(Set<Id> sources, Steps steps,
                                            int maxDepth, long limit,
                                            boolean withEdge) {
        E.checkNotNull(sources, "source vertices");
        E.checkArgument(sources.size() > 0, "source vertices can't be empty");
        for (Id source : sources) {
            E.checkNotNull(source, "source vertex id");
            this.checkVertexExist(source, "source vertex");
        }
        checkPositive(maxDepth, "egonet max_depth");
        checkLimit(limit);

        boolean concurrent = maxDepth >= this.concurrentDepth();

        // Construct a multi sources start layer
        KneighborRecords records = new KneighborRecords(RecordType.INT,
                                                        concurrent,
                                                        sources,
                                                        true);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertexAF(v, steps, false);

            while (!this.reachLimit(limit, records.size()) && edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                this.edgeIterCounter++;
                Id target = edge.id().otherVertexId();
                records.addPath(v, target);
                if (withEdge) {
                    // for breadth, we have to collect all edge during traversal
                    // to avoid over occupy for memory, we collect edgeId only.
                    records.addEdgeId(edge.id());
                }
            }
        };

        while (maxDepth-- > 0) {
            records.startOneLayer(true);
            traverseIdsKneighbor(records.keys(), steps, consumer, concurrent,
                                 records, limit, withEdge);
            records.finishOneLayer();
            if (this.reachLimit(limit, records.size())) {
                break;
            }
        }

        if (withEdge) {
            // we should filter out unused-edges for breadth first algorithm.
            records.filterUnusedEdges(limit);
        }

        return records;
    }

    private boolean reachLimit(long limit, int size) {
        return limit != NO_LIMIT && size >= limit;
    }

    private long traverseIdsKneighbor(Iterator<Id> ids, Steps steps,
                                        Consumer<Id> consumer,
                                        boolean concurrent,
                                        KneighborRecords records,
                                        long limit,
                                        boolean withEdge) {
        long count = 0L;
        if (steps.isEdgeStepPropertiesEmpty() && steps.isVertexEmpty()) {
            Id labelId = null;
            if (!steps.edgeSteps().isEmpty()) {
                Steps.StepEntity step =
                        steps.edgeSteps().values().iterator().next();
                String label = step.getLabel();
                labelId = this.getEdgeLabelId(label);
            }

            List<Id> vids = newList();
            while (ids.hasNext()) {
                Id vid = ids.next();
                vids.add(vid);
            }

            EdgesOfVerticesIterator edgeIts = edgesOfVertices(vids.iterator(),
                    steps.direction(),
                    labelId,
                    steps.degree(),
                    false);
            AdjacentVerticesBatchConsumer consumer1 =
                    new AdjacentVerticesBatchConsumerKneighbor(records, limit,
                            withEdge);
            edgeIts.setAvgDegreeSupplier(consumer1::getAvgDegree);
            BufferGroupEdgesOfVerticesIterator bufferEdgeIts = new BufferGroupEdgesOfVerticesIterator(edgeIts, vids,
                    steps.degree());
            this.traverseBatchCurrentThread(bufferEdgeIts, consumer1, "traverse-ite-edge", 1);
        } else {
            count = traverseIds(ids, consumer, concurrent);
        }
        return count;
    }

    class AdjacentVerticesBatchConsumerKneighbor extends AdjacentVerticesBatchConsumer {

        private KneighborRecords records;
        private boolean withEdge;

        public AdjacentVerticesBatchConsumerKneighbor(KneighborRecords records,
                                                      long limit,
                                                      boolean withEdge) {
            this(null, null, limit, null);
            this.records = records;
            this.withEdge = withEdge;
        }

        public AdjacentVerticesBatchConsumerKneighbor(Id sourceV,
                                                      Set<Id> excluded,
                                                      long limit,
                                                      Set<Id> neighbors) {
            super(sourceV, excluded, limit, neighbors);
        }

        @Override
        public void accept(CIter<Edge> edges) {
            if (this.reachLimit(limit, records.size())) {
                return;
            }

            long degree = 0;
            Id ownerId = null;

            while (!this.reachLimit(limit, records.size()) && edges.hasNext()) {
                edgeIterCounter++;
                degree++;
                HugeEdge e = (HugeEdge) edges.next();

                Id source = e.id().ownerVertexId();
                Id target = e.id().otherVertexId();
                records.addPath(source, target);
                if(withEdge) {
                    // for breadth, we have to collect all edge during traversal,
                    // to avoid over occupy for memory, we collect edgeId only.
                    records.addEdgeId(e.id());
                }

                Id owner = e.id().ownerVertexId();
                if (ownerId == null || ownerId.compareTo(owner) != 0) {
                    vertexIterCounter++;
                    this.avgDegree = this.avgDegreeRatio * this.avgDegree + (1 - this.avgDegreeRatio) * degree;
                    degree = 0;
                    ownerId = owner;
                }
            }
        }

        private boolean reachLimit(long limit, int size) {
            return limit != NO_LIMIT && size >= limit;
        }
    }
}
