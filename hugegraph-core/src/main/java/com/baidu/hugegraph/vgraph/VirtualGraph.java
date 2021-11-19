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
package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.baidu.hugegraph.backend.id.Id;
import org.slf4j.Logger;

public class VirtualGraph {
    private static final int INIT_VERTEX_CAPACITY = 10000;
    private static final Logger LOG = Log.logger(VirtualGraph.class);

    private ConcurrentMap<Id, VirtualVertex> vertexMap;
    private ConcurrentMap<Id, VirtualEdge> edgeMap;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    private HugeGraphParams graphParams;
    public AbstractSerializer serializer;

    public VirtualGraph(HugeGraphParams graphParams) {
        assert graphParams != null;
        this.graphParams = graphParams;
        this.serializer = this.graphParams.serializer();
        this.init();
    }

    public void init(){
        this.vertexMap = new ConcurrentHashMap<>(INIT_VERTEX_CAPACITY);
        this.edgeMap = new ConcurrentHashMap<>(INIT_VERTEX_CAPACITY);
        this.listenChanges();
    }

    public int getVertexSize() {
        return this.vertexMap.size();
    }

    public int getEdgeSize() {
        return this.edgeMap.size();
    }

    public HugeVertex queryVertexById(Id vId, VirtualVertexStatus status) {
        assert vId != null;
        return toHuge(this.vertexMap.get(vId), status);
    }

    public boolean updateIfPresentVertex(HugeVertex vertex) {
        assert vertex != null;
        return this.vertexMap.computeIfPresent(vertex.id(), (id, old) -> toVirtual(old, vertex)) != null;
    }

    public void putVertex(HugeVertex vertex) {
        assert vertex != null;
        this.vertexMap.compute(vertex.id(), (id, old) -> toVirtual(old, vertex));
    }

    public void putVerteies(Iterator<HugeVertex> values) {
        assert values != null;
        values.forEachRemaining(vertex -> this.vertexMap.compute(vertex.id(), (id, old) -> toVirtual(old, vertex)));
    }

    public void updateIfPresentEdge(Iterator<HugeEdge> edges) {
        assert edges != null;
        edges.forEachRemaining(edge ->
                this.edgeMap.computeIfPresent(edge.id(),
                        (id, old) -> toVirtual(old, edge)));
    }

    public VirtualEdge putEdge(HugeEdge edge) {
        assert edge != null;
        return this.edgeMap.compute(edge.id(), (id, old) -> toVirtual(old, edge));
    }

    public void putEdges(Iterator<HugeEdge> values) {
        assert values != null;
        values.forEachRemaining(this::putEdge);
    }

    public void invalidateVertex(Id vId) {
        assert vId != null;
        this.vertexMap.remove(vId);
    }

    public HugeEdge queryEdgeById(Id eId) {
        assert eId != null;
        return toHuge(this.edgeMap.get(eId));
    }

    public void invalidateEdge(Id eId) {
        assert eId != null;
        this.edgeMap.remove(eId);
    }

//    public ArrayList<VirtualEdge> queryEdgesByVertex(Id vId) {
//        return this.vertexMap.get(vId).edges;
//    }

    public void clear(){
        this.vertexMap.clear();
        this.edgeMap.clear();
    }

    private HugeVertex toHuge(VirtualVertex vertex, VirtualVertexStatus status) {

        if (vertex == null) {
            return null;
        }

        if (status.match(vertex.getStatus())) {
            return vertex.getVertex();
        } else {
            return null;
        }

    }

    private HugeEdge toHuge(VirtualEdge virtualEdge) {
        if (virtualEdge == null) {
            return null;
        }
        return virtualEdge.getEdge();
    }

    private VirtualVertex toVirtual(VirtualVertex old, HugeVertex hugeVertex) {
        VirtualVertex newVertex = new VirtualVertex(hugeVertex, VirtualVertexStatus.Id.code());
        VirtualVertex result = mergeVVEdge(old, newVertex, hugeVertex);
        result = mergeVVProp(old, result, hugeVertex);
        assert result != null;
        return result;
    }

    private VirtualEdge toVirtual(VirtualEdge old, HugeEdge hugeEdge) {
        return new VirtualEdge(hugeEdge, VirtualEdgeStatus.OK.code());
    }

    private VirtualVertex mergeVVEdge(VirtualVertex oldV, VirtualVertex newV, HugeVertex hugeVertex) {
        if (oldV == newV) {
            return oldV;
        }
        if (hugeVertex.existsEdges()) {
            hugeVertex.getEdges().forEach(e -> {
                VirtualEdge edge = this.putEdge(e);
                assert edge != null;
                newV.getEdges().add(edge);
            });
            newV.orStatus(VirtualVertexStatus.Edge);
        }
        if (oldV == null) {
            return newV;
        }
        // new vertex has no edges
        if (!VirtualVertexStatus.Edge.match(newV.getStatus())) {
            return oldV;
        }
        return newV;
    }

    private VirtualVertex mergeVVProp(VirtualVertex oldV, VirtualVertex newV, HugeVertex hugeVertex) {
        if (oldV == newV) {
            return oldV;
        }
        if (hugeVertex.isPropLoaded()) {
            newV.orStatus(VirtualVertexStatus.Property);
        }
        if (oldV == null) {
            return newV;
        }
        // new vertex is not loaded
        if (!VirtualVertexStatus.Property.match(newV.getStatus())) {
            return oldV;
        }
        return newV;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                Events.STORE_CLEAR,
                Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear graph cache on event '{}'",
                        this.graphParams.graph(), event.name());
                this.clearCache(null, true);
                return true;
            }
            return false;
        };
        this.graphParams.loadGraphStore().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received graph cache event: {}",
                    this.graphParams.graph(), event);
            Object[] args = event.args();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                    "Expect event action argument");
            if (Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class, Object.class);
                HugeType type = (HugeType) args[1];
                if (type.isVertex()) {
                    // Invalidate vertex cache
                    Object arg2 = args[2];
                    if (arg2 instanceof Id) {
                        Id id = (Id) arg2;
                        this.vertexMap.remove(id);
                    } else if (arg2 != null && arg2.getClass().isArray()) {
                        int size = Array.getLength(arg2);
                        for (int i = 0; i < size; i++) {
                            Object id = Array.get(arg2, i);
                            E.checkArgument(id instanceof Id,
                                    "Expect instance of Id in array, " +
                                            "but got '%s'", id.getClass());
                            this.vertexMap.remove((Id) id);
                        }
                    } else {
                        E.checkArgument(false,
                                "Expect Id or Id[], but got: %s",
                                arg2);
                    }
                } else if (type.isEdge()) {
                    // Invalidate vertex cache
                    Object arg2 = args[2];
                    if (arg2 instanceof Id) {
                        Id id = (Id) arg2;
                        this.edgeMap.remove(id);
                    } else if (arg2 != null && arg2.getClass().isArray()) {
                        int size = Array.getLength(arg2);
                        for (int i = 0; i < size; i++) {
                            Object id = Array.get(arg2, i);
                            E.checkArgument(id instanceof Id,
                                    "Expect instance of Id in array, " +
                                            "but got '%s'", id.getClass());
                            this.edgeMap.remove((Id) id);
                        }
                    } else {
                        E.checkArgument(false,
                                "Expect Id or Id[], but got: %s",
                                arg2);
                    }
                }
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class);
                HugeType type = (HugeType) args[1];
                this.clearCache(type, false);
                return true;
            }
            return false;
        };
        EventHub graphEventHub = this.graphParams.graphEventHub();
        if (!graphEventHub.containsListener(Events.CACHE)) {
            graphEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.graphParams.loadGraphStore().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub graphEventHub = this.graphParams.graphEventHub();
        graphEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    public void notifyChanges(String action, HugeType type, Id[] ids) {
        EventHub graphEventHub = this.graphParams.graphEventHub();
        graphEventHub.notify(Events.CACHE, action, type, ids);
    }

    private void clearCache(HugeType type, boolean notify) {
        if (type == null || type == HugeType.VERTEX) {
            this.vertexMap.clear();
        }
        if (type == null || type == HugeType.EDGE) {
            this.edgeMap.clear();
        }

        if (notify) {
            this.notifyChanges(Cache.ACTION_CLEARED, null, null);
        }
    }
}