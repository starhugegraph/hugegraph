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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
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

    public class ConcurrentMultiValuedMap<K, V>
           extends ConcurrentHashMap<K, List<V>> {

        private static final long serialVersionUID = -7249946839643493614L;

        public ConcurrentMultiValuedMap() {
            super();
        }

        public void add(K key, V value) {
            List<V> values = this.getValues(key);
            values.add(value);
        }

        public void addAll(K key, List<V> value) {
            List<V> values = this.getValues(key);
            values.addAll(value);
        }

        public List<V> getValues(K key) {
            List<V> values = this.get(key);
            if (values == null) {
                values = new CopyOnWriteArrayList<>();
                List<V> old = this.putIfAbsent(key, values);
                if (old != null) {
                    values = old;
                }
            }
            return values;
        }
    }

    protected Set<Id> adjacentVertices(Id sourceV, Set<Id> vertices,
                                       Directions dir, Id label,
                                       Set<Id> excluded, long degree,
                                       long limit, boolean concurrent) {
        if (limit == 0) {
            return ImmutableSet.of();
        }

        Set<Id> neighbors = newSet(concurrent);

        this.traverseIds(vertices.iterator(), new AdjacentVerticesConsumer(
                sourceV, dir, label, excluded, degree, limit, neighbors
        ), concurrent);

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

    protected Set<Id> adjacentVerticesBatch(Id sourceV, Set<Id> vertices,
                                       Directions dir, Id label,
                                       Set<Id> excluded, long degree,
                                       long limit) {
        if (limit == 0) {
            return ImmutableSet.of();
        }

        Set<Id> neighbors = newSet(true);

        EdgesOfVerticesIterator edgeIts = edgesOfVertices(vertices, dir, label, degree, false);
        AdjacentVerticesBatchConsumer consumer = new AdjacentVerticesBatchConsumer(
                sourceV, excluded, limit, neighbors);
        edgeIts.setAvgDegreeSupplier(consumer::getAvgDegree);
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);

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


    class AdjacentVerticesConsumer implements Consumer<Id> {

        private Id sourceV;
        private Directions dir;
        private Id label;
        private Set<Id> excluded;
        private long degree;
        private long limit;
        private Set<Id> neighbors;

        public AdjacentVerticesConsumer(Id sourceV, Directions dir, Id label,
                                        Set<Id> excluded, long degree,
                                        long limit, Set<Id> neighbors) {
            this.sourceV = sourceV;
            this.dir = dir;
            this.label = label;
            this.excluded = excluded;
            this.degree = degree;
            this.limit = limit;
            this.neighbors = neighbors;
        }

        @Override
        public void accept(Id id) {
            Iterator<Edge> edges = edgesOfVertex(id, dir,
                    label, degree, false);
            while (edges.hasNext()) {
                edgeIterCounter++;
                HugeEdge e = (HugeEdge) edges.next();
                Id target = e.id().otherVertexId();
                boolean matchExcluded = (excluded != null &&
                        excluded.contains(target));
                if (matchExcluded || neighbors.contains(target) ||
                        sourceV.equals(target)) {
                    continue;
                }
                neighbors.add(target);
                if (limit != NO_LIMIT && neighbors.size() >= limit) {
                    return;
                }
            }
        }

        @Override
        public Consumer<Id> andThen(Consumer<? super Id> after) {
            java.util.Objects.requireNonNull(after);
            return (Id t) -> {
                accept(t);
                if (limit != NO_LIMIT && neighbors.size() >= limit) {
                    return;
                } else {
                    after.accept(t);
                }
            };
        }
    }

    class AdjacentVerticesBatchConsumer implements Consumer<CIter<Edge>> {

        private Id sourceV;
        private Set<Id> excluded;
        private long limit;
        private Set<Id> neighbors;
        private double avgDegreeRatio;
        private double avgDegree;

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
