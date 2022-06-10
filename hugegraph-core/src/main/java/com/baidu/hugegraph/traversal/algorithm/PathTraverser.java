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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.util.Consumers;
import com.baidu.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.strategy.TraverseStrategy;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

public abstract class PathTraverser {

    public static final Logger LOG = Log.logger(PathTraverser.class);

    protected final HugeTraverser traverser;

    private static final String EXECUTOR_NAME = "path";
    private static Consumers.ExecutorPool executors;

    protected int stepCount;
    protected final long capacity;
    protected final long limit;
    protected int totalSteps; // TODO: delete or implement abstract method

    protected Map<Id, List<HugeTraverser.Node>> sources;
    protected Map<Id, List<HugeTraverser.Node>> sourcesAll;
    protected Map<Id, List<HugeTraverser.Node>> targets;
    protected Map<Id, List<HugeTraverser.Node>> targetsAll;

    protected Map<Id, List<HugeTraverser.Node>> newVertices;

    protected Set<HugeTraverser.Path> paths;

    protected TraverseStrategy traverseStrategy;

    public PathTraverser(HugeTraverser traverser, TraverseStrategy strategy,
                         Collection<Id> sources, Collection<Id> targets,
                         long capacity, long limit) {
        this.traverser = traverser;
        this.traverseStrategy = strategy;

        this.capacity = capacity;
        this.limit = limit;

        this.stepCount = 0;

        this.sources = this.newMultiValueMap();
        this.sourcesAll = this.newMultiValueMap();
        this.targets = this.newMultiValueMap();
        this.targetsAll = this.newMultiValueMap();

        for (Id id : sources) {
            this.addNode(this.sources, id, new HugeTraverser.Node(id));
        }
        for (Id id : targets) {
            this.addNode(this.targets, id, new HugeTraverser.Node(id));
        }
        this.sourcesAll.putAll(this.sources);
        this.targetsAll.putAll(this.targets);

        this.paths = this.newPathSet();

        this.initExecutor();
    }

    protected void initExecutor() {
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

    protected HugeGraph graph() {
        return this.traverser.graph();
    }

    public void forward() {
        EdgeStep currentStep = this.nextStep(true);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(true);

        // Traversal vertices of previous level
        // this.traverseOneLayer(this.sources, currentStep, this::forward);
        // 批量计算
        this.traverseOneLayerBatch(this.sources, currentStep, this::forwardBatch);

        this.afterTraverse(currentStep, true);
    }

    public void backward() {
        EdgeStep currentStep = this.nextStep(false);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(false);

        currentStep.swithDirection();
        // Traversal vertices of previous level
        // this.traverseOneLayer(this.targets, currentStep, this::backward);
        // 批量计算
        this.traverseOneLayerBatch(this.targets, currentStep, this::backwardBatch);
        currentStep.swithDirection();

        this.afterTraverse(currentStep, false);
    }

    public abstract EdgeStep nextStep(boolean forward);

    public void beforeTraverse(boolean forward) {
        this.clearNewVertices();
    }

    public void traverseOneLayer(Map<Id, List<HugeTraverser.Node>> vertices,
                                 EdgeStep step,
                                 BiConsumer<Id, EdgeStep> consumer) {
        this.traverseStrategy.traverseOneLayer(vertices, step, consumer);
    }

    public void traverseOneLayerBatch(
            Map<Id, List<HugeTraverser.Node>> vertices,
            EdgeStep step,
            BiConsumer<Iterator<Id>, EdgeStep> consumer) {
        this.traverseStrategy.traverseOneLayerBatch(vertices, step, consumer);
    }

    public void afterTraverse(EdgeStep step, boolean forward) {
        this.reInitCurrentStepIfNeeded(step, forward);
        this.stepCount++;
    }

    private void forward(Id v, EdgeStep step) {
        this.traverseOne(v, step, true);
    }

    private void backward(Id v, EdgeStep step) {
        this.traverseOne(v, step, false);
    }

    private void traverseOne(Id v, EdgeStep step, boolean forward) {
        if (this.reachLimit()) {
            return;
        }

        Iterator<Edge> edges = this.traverser.edgesOfVertex(v, step, false);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            Id target = edge.id().otherVertexId();

            this.processOne(v, target, forward);
        }
    }

    private void processOne(Id source, Id target, boolean forward) {
        if (forward) {
            this.processOneForForward(source, target);
        } else {
            this.processOneForBackward(source, target);
        }
    }

    private void forwardBatch(Iterator<Id> ites, EdgeStep step) {
        this.traverseBatch(ites, step, true);
    }

    private void backwardBatch(Iterator<Id> ites, EdgeStep step) {
        this.traverseBatch(ites, step, false);
    }

    private void traverseBatch(Iterator<Id> v, EdgeStep step, boolean forward) {
        if (this.reachLimit()) {
            return;
        }

        List<Id> labelIds = step.labels().keySet().stream().collect(
                Collectors.toList());

        HugeTraverser.EdgesOfVerticesIterator edgeIts =
                this.traverser.edgesOfVertices(v, step.direction(), labelIds,
                                               step.limit(), false);

        AdjacentVerticesBatchConsumer consumer1 =
                new AdjacentVerticesBatchConsumerTemplatePaths(this, forward);

        edgeIts.setAvgDegreeSupplier(consumer1::getAvgDegree);
        //使用单线程来做
        this.traverseBatchCurrentThread(edgeIts, consumer1, "traverse-ite-edge", 1);
    }

    protected abstract void processOneForForward(Id source, Id target);

    protected abstract void processOneForBackward(Id source, Id target);

    protected abstract void reInitCurrentStepIfNeeded(EdgeStep step,
                                                      boolean forward);

    public void clearNewVertices() {
        this.newVertices = this.newMultiValueMap();
    }

    public void addNodeToNewVertices(Id id, HugeTraverser.Node node) {
        this.addNode(this.newVertices, id, node);
    }

    public Map<Id, List<HugeTraverser.Node>> newMultiValueMap() {
        return this.traverseStrategy.newMultiValueMap();
    }

    public Set<HugeTraverser.Path> newPathSet() {
        return this.traverseStrategy.newPathSet();
    }

    public void addNode(Map<Id, List<HugeTraverser.Node>> vertices, Id id,
                        HugeTraverser.Node node) {
        this.traverseStrategy.addNode(vertices, id, node);
    }

    public void addNewVerticesToAll(Map<Id, List<HugeTraverser.Node>> targets) {
        this.traverseStrategy.addNewVerticesToAll(this.newVertices, targets);
    }

    public Set<HugeTraverser.Path> paths() {
        return this.paths;
    }

    public int pathCount() {
        return this.paths.size();
    }

    protected boolean finished() {
        return this.stepCount >= this.totalSteps || this.reachLimit();
    }

    protected boolean reachLimit() {
        HugeTraverser.checkCapacity(this.capacity, this.accessedNodes(),
                                    "template paths");
        if (this.limit == NO_LIMIT || this.pathCount() < this.limit) {
            return false;
        }
        return true;
    }

    protected int accessedNodes() {
        int size = 0;
        for (List<HugeTraverser.Node> value : this.sourcesAll.values()) {
            size += value.size();
        }
        for (List<HugeTraverser.Node> value : this.targetsAll.values()) {
            size += value.size();
        }
        return size;
    }

    class AdjacentVerticesBatchConsumerTemplatePaths
            extends AdjacentVerticesBatchConsumer {

        private PathTraverser pathTraverser;
        private boolean forward;

        public AdjacentVerticesBatchConsumerTemplatePaths(PathTraverser pathTraverser,
                                                          boolean forward) {
            this.pathTraverser = pathTraverser;
            this.forward = forward;
        }

        @Override
        public void accept(CIter<Edge> edges) {
            if (this.reachLimit()) {
                return;
            }

            long degree = 0;
            Id ownerId = null;

            while (!this.reachLimit() && edges.hasNext()) {
                degree++;
                HugeEdge e = (HugeEdge) edges.next();

                Id source = e.id().ownerVertexId();
                Id target = e.id().otherVertexId();
                this.pathTraverser.processOne(source, target, this.forward);

                Id owner = e.id().ownerVertexId();
                if (ownerId == null || ownerId.compareTo(owner) != 0) {
                    this.avgDegree = this.avgDegreeRatio * this.avgDegree + (1 - this.avgDegreeRatio) * degree;
                    degree = 0;
                    ownerId = owner;
                }
            }
        }

        private boolean reachLimit() {
            return this.pathTraverser.reachLimit();
        }
    }

    // almost same as OltpTraverser.AdjacentVerticesBatchConsumer
    class AdjacentVerticesBatchConsumer implements Consumer<CIter<Edge>> {

        protected double avgDegreeRatio;
        protected double avgDegree;

        public AdjacentVerticesBatchConsumer() {
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
                degree++;
                HugeEdge e = (HugeEdge) edges.next();

                Id owner = e.id().ownerVertexId();
                if (ownerId == null || ownerId.compareTo(owner) != 0) {
                    this.avgDegree = this.avgDegreeRatio * this.avgDegree + (1 - this.avgDegreeRatio) * degree;
                    degree = 0;
                    ownerId = owner;
                }
            }
        }

        @Override
        public Consumer<CIter<Edge>> andThen(Consumer<? super CIter<Edge>> after) {
            java.util.Objects.requireNonNull(after);
            return (CIter<Edge> t) -> {
                accept(t);
                after.accept(t);
            };
        }
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
}
