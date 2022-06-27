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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.ws.rs.core.MultivaluedMap;

import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.steps.WeightedEdgeStep;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.util.Consumers;

import jersey.repackaged.com.google.common.base.Objects;

public abstract class OltpTraverser extends HugeTraverser
                                    implements AutoCloseable {

    private static final String EXECUTOR_NAME = "oltp";
    private static Consumers.ExecutorPool executors;

    protected OltpTraverser(HugeGraph graph) {
        super(graph);
        if (executors != null) {
            return;
        }
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                return;
            }
            int workers = this.graph()
                              .option(CoreOptions.OLTP_CONCURRENT_THREADS);
            if (workers > 0) {
                executors = new Consumers.ExecutorPool(EXECUTOR_NAME, workers);
            }
        }
    }

    @Override
    public void close() {
        // pass
    }

    public static void destroy() {
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                executors.destroy();
                executors = null;
            }
        }
    }

    protected long traversePairs(Iterator<Pair<Id, Id>> pairs,
                                 Consumer<Pair<Id, Id>> consumer) {
        return this.traverse(pairs, consumer, "traverse-pairs");
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer,
                               boolean concurrent) {
        if (concurrent) {
            return this.traverseIds(ids, consumer);
        } else {
            long count = 0L;
            while (ids.hasNext()) {
                this.edgeIterCounter++;
                count++;
                consumer.accept(ids.next());
            }
            return count;
        }
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer) {
        return this.traverse(ids, consumer, "traverse-ids");
    }

    protected <K> long traverse(Iterator<K> iterator, Consumer<K> consumer,
                                String name) {
        if (!iterator.hasNext()) {
            return 0L;
        }

        Consumers<K> consumers = new Consumers<>(executors.getExecutor(),
                                                 consumer, null);
        consumers.start(name);
        long total = 0L;
        try {
            while (iterator.hasNext()) {
                this.edgeIterCounter++;
                total++;
                K v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    protected <K> long traverseBatchCurrentThread(Iterator<CIter<K>> iterator,
                                                  Consumer<CIter<K>> consumer,
                                                  String name,
                                                  int queueWorkerSize) {
        if (!iterator.hasNext()) {
            return 0L;
        }

        Consumers<CIter<K>> consumers = new Consumers<>(null,
                                                        consumer, null, queueWorkerSize);
        consumers.start(name);
        long total = 0L;
        try {
            while (iterator.hasNext()) {
//                this.edgeIterCounter++;
                total++;
                CIter<K> v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
                iterator.forEachRemaining(it -> {
                    try {
                        it.close();
                    } catch (Exception ex) {
                        LOG.warn("Exception when closing CIter", ex);
                    }
                });
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    protected <K> long traverseBatch(Iterator<CIter<K>> iterator, Consumer<CIter<K>> consumer,
                                String name, int queueWorkerSize) {
        if (!iterator.hasNext()) {
            return 0L;
        }

        Consumers<CIter<K>> consumers = new Consumers<>(executors.getExecutor(),
                consumer, null, queueWorkerSize);
        consumers.start(name);
        long total = 0L;
        try {
            while (iterator.hasNext()) {
//                this.edgeIterCounter++;
                total++;
                CIter<K> v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
                iterator.forEachRemaining(it -> {
                    try {
                        it.close();
                    } catch (Exception ex) {
                        LOG.warn("Exception when closing CIter", ex);
                    }
                });
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    protected MultivaluedMap<Id, Node> traverseNodesBatch(
            MultivaluedMap<Id, Node> newVertices,
            MultivaluedMap<Id, Node> sources, WeightedEdgeStep step,
            int stepNum, boolean sorted, int pathCount, long access,
            long capacity, long limit) {
        if (limit != NO_LIMIT && !sorted && pathCount >= limit) {
            return sources;
        }

        boolean withProperties = sorted && step.weightBy() != null;
        AdjacentVerticesBatchConsumerCustomizedPaths consumer =
                new AdjacentVerticesBatchConsumerCustomizedPaths(
                        newVertices, step, stepNum, sorted, pathCount, access,
                        capacity, limit);

        for (Map.Entry<Id, List<Node>> entry : sources.entrySet()) {

            consumer.setAdjacency(newList());
            consumer.setEntryValue(entry.getValue());

            EdgeStep edgeStep = step.step();
            Map<Id, String> labels = edgeStep.labels();
            Directions directions = edgeStep.direction();
            Set<Id> idSet = newIdSet();
            idSet.add(entry.getKey());

            if (labels == null || labels.isEmpty()) {
                EdgesOfVerticesIterator edgeIts =
                        edgesOfVertices(idSet, directions,
                                        (Id) null, edgeStep.limit(),
                                        withProperties);
                edgeIts.setAvgDegreeSupplier(consumer::getAvgDegree);
                this.traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
            }

            for (Id label : labels.keySet()) {
                E.checkNotNull(label, "edge label");
                EdgesOfVerticesIterator edgeIts =
                        edgesOfVertices(idSet, directions, label,
                                        edgeStep.limit(), withProperties);
                edgeIts.setAvgDegreeSupplier(consumer::getAvgDegree);
                this.traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
            }
        }
        return consumer.getNewVertices();
    }

    protected Iterator<Vertex> filter(Iterator<Vertex> vertices,
                                      String key, Object value) {
        return new FilterIterator<>(vertices, vertex -> {
            return match(vertex, key, value);
        });
    }

    protected boolean match(Element elem, String key, Object value) {
        // check property key exists
        this.graph().propertyKey(key);
        // return true if property value exists & equals to specified value
        Property<Object> p = elem.property(key);
        return p.isPresent() && Objects.equal(p.value(), value);
    }

    protected Set<Id> adjacentVerticesBatch(Id sourceV, Set<Id> vertices,
                                       Directions dir, Id label,
                                       Set<Id> excluded, long degree,
                                       long limit) {
        if (limit == 0) {
            return ImmutableSet.of();
        }
        List<Id> vids = newList();
        for (Id vid: vertices) {
            vids.add(vid);
        }

        Set<Id> neighbors = newSet(true);

        EdgesOfVerticesIterator edgeIts = edgesOfVertices(vertices, dir, label, degree, false);
        AdjacentVerticesBatchConsumer consumer = new AdjacentVerticesBatchConsumer(
                sourceV, excluded, limit, neighbors);
        edgeIts.setAvgDegreeSupplier(consumer::getAvgDegree);
        BufferGroupEdgesOfVerticesIterator bufferEdgeIts = new BufferGroupEdgesOfVerticesIterator(edgeIts,
                                                                                                  vids, degree);
        this.traverseBatch(bufferEdgeIts, consumer, "traverse-ite-edge", 1);

        if (limit != NO_LIMIT && neighbors.size() > limit) {
            int redundantNeighborsCount = (int) (neighbors.size() - limit);
            List<Id> redundantNeighbors = new ArrayList<>(redundantNeighborsCount);
            for (Id vId: neighbors) {
                redundantNeighbors.add(vId);
                if (redundantNeighbors.size() >= redundantNeighborsCount) {
                    break;
                }
            }
            redundantNeighbors.forEach(neighbors::remove);
        }

        return neighbors;
    }

    class AdjacentVerticesBatchConsumerCustomizedPaths
            extends AdjacentVerticesBatchConsumer {
        protected MultivaluedMap<Id, Node> newVertices;
        protected WeightedEdgeStep step;
        protected int stepNum;
        protected boolean sorted;
        protected long capacity;
        protected int pathCount;
        protected long access;
        protected long limit;

        protected List<Node> adjacency;
        protected List<Node> entryValue;

        public AdjacentVerticesBatchConsumerCustomizedPaths(
                MultivaluedMap<Id, Node> newVertices, WeightedEdgeStep step,
                int stepNum, boolean sorted, int pathCount, long access,
                long capacity, long limit) {
            this(null, null, limit, null);
            this.newVertices = newVertices;
            this.step = step;
            this.stepNum = stepNum;
            this.sorted = sorted;
            this.pathCount = pathCount;
            this.access = access;
            this.capacity = capacity;
            this.limit = limit;
        }

        public AdjacentVerticesBatchConsumerCustomizedPaths(Id sourceV,
                                                            Set<Id> excluded,
                                                            long limit,
                                                            Set<Id> neighbors) {
            super(sourceV, excluded, limit, neighbors);
        }

        @Override
        public void accept(CIter<Edge> edges) {
            if (reachLimit()) {
                return ;
            }

            while (edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                Id target = edge.id().otherVertexId();
                for (Node n : entryValue) {
                    // If have loop, skip target
                    if (n.contains(target)) {
                        continue;
                    }
                    Node newNode;
                    if (sorted) {
                        double w = step.weightBy() != null ?
                                edge.value(step.weightBy().name()) :
                                step.defaultWeight();
                        newNode = new CustomizePathsTraverser.WeightNode(target,
                                                                         n, w);
                    } else {
                        newNode = new Node(target, n);
                    }
                    adjacency.add(newNode);

                    checkCapacity(capacity, ++access, "customized paths");
                }
            }

            if (step.sample() > 0) {
                // Sample current node's adjacent nodes
                adjacency = sample(adjacency, step.sample());
            }

            // Add current node's adjacent nodes
            for (Node node : adjacency) {
                newVertices.add(node.id(), node);
                // Avoid exceeding limit
                if (stepNum == 0) {
                    if (limit != NO_LIMIT && !sorted &&
                        ++pathCount >= limit) {
                        return;
                    }
                }
            }
        }

        public MultivaluedMap<Id, Node> getNewVertices() {
            return newVertices;
        }

        public void setAdjacency(
                List<Node> adjacency) {
            this.adjacency = adjacency;
        }

        public void setEntryValue(
                List<Node> entryValue) {
            this.entryValue = entryValue;
        }

        private List<Node> sample(List<Node> nodes, long sample) {
            if (nodes.size() <= sample) {
                return nodes;
            }
            List<Node> result = newList((int) sample);
            int size = nodes.size();
            for (int random : CollectionUtil.randomSet(0, size, (int) sample)) {
                result.add(nodes.get(random));
            }
            return result;
        }

        private boolean reachLimit() {
            return limit != NO_LIMIT && !sorted && pathCount >= limit;
        }
    }

    class AdjacentVerticesBatchConsumer implements Consumer<CIter<Edge>> {

        protected Id sourceV;
        protected Set<Id> excluded;
        protected long limit;
        protected Set<Id> neighbors;
        protected double avgDegreeRatio;
        protected double avgDegree;

        public AdjacentVerticesBatchConsumer(Id sourceV, Set<Id> excluded, long limit,
                                             Set<Id> neighbors) {
            this.sourceV = sourceV;
            this.excluded = excluded;
            this.limit = limit;
            this.neighbors = neighbors;
            this.avgDegreeRatio = graph().option(CoreOptions.OLTP_QUERY_BATCH_AVG_DEGREE_RATIO);
            this.avgDegree = 0;
        }

        public double getAvgDegree() {
            return this.avgDegree;
        }

        @Override
        public void accept(CIter<Edge> edges) {
            long degree = 0;
            Id ownerId = null;
            while (edges.hasNext()) {
                edgeIterCounter++;
                degree++;
                HugeEdge e = (HugeEdge) edges.next();

                Id owner = e.id().ownerVertexId();
                if (ownerId == null || ownerId.compareTo(owner) != 0) {
                    vertexIterCounter++;
                    this.avgDegree = this.avgDegreeRatio * this.avgDegree + (1 - this.avgDegreeRatio) * degree;
                    degree = 0;
                    ownerId = owner;
                }

                Id target = e.id().otherVertexId();
                boolean matchExcluded = (excluded != null &&
                        excluded.contains(target));
                if (matchExcluded || neighbors.contains(target) ||
                        sourceV.equals(target)) {
                    continue;
                }
                neighbors.add(target);
                if (limit != NO_LIMIT && neighbors.size() >= limit) {
                    if (edges.hasNext()) {
                        try {
                            edges.close();
                        } catch (Exception ex) {
                            LOG.warn("Exception when closing CIter", ex);
                        }
                    }
                    return;
                }
            }
        }

        @Override
        public Consumer<CIter<Edge>> andThen(Consumer<? super CIter<Edge>> after) {
            java.util.Objects.requireNonNull(after);
            return (CIter<Edge> t) -> {
                accept(t);
                if (limit != NO_LIMIT && neighbors.size() >= limit) {
                    try {
                        t.close();
                    } catch (Exception ex) {
                        LOG.warn("Exception when closing CIter", ex);
                    }
                    return;
                } else {
                    after.accept(t);
                }
            };
        }
    }
}
