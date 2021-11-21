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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

    public HugeVertex queryHugeVertexById(Id vId, VirtualVertexStatus status) {
        VirtualVertex vertex = queryVertexById(vId, status);
        if (vertex != null) {
            return vertex.getVertex();
        }
        else {
            return null;
        }
    }

    public VirtualVertex queryVertexById(Id vId, VirtualVertexStatus status) {
        assert vId != null;
        VirtualVertex vertex = this.vertexMap.get(vId);
        if (vertex == null) {
            return null;
        }

        if (status.match(vertex.getStatus())) {
            return vertex;
        } else {
            return null;
        }
    }

    public boolean updateIfPresentVertex(HugeVertex vertex, Iterator<HugeEdge> inEdges) {
        assert vertex != null;
        return this.vertexMap.computeIfPresent(vertex.id(), (id, old) -> toVirtual(old, vertex, inEdges)) != null;
    }

    public void putVertex(HugeVertex vertex, Iterator<HugeEdge> inEdges) {
        assert vertex != null;
        this.vertexMap.compute(vertex.id(), (id, old) -> toVirtual(old, vertex, inEdges));
    }

    public void putVerteiesWithoutEdges(Iterator<HugeVertex> values) {
        assert values != null;
        values.forEachRemaining(vertex ->
                this.vertexMap.compute(vertex.id(), (id, old) ->
                        toVirtual(old, vertex, null, null)));
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

    public HugeEdge queryEdgeById(Id eId, VirtualEdgeStatus status) {
        assert eId != null;
        return toHuge(this.edgeMap.get(eId), status);
    }

    public void invalidateEdge(Id eId) {
        assert eId != null;
        this.edgeMap.remove(eId);
    }

    public void clear(){
        this.vertexMap.clear();
        this.edgeMap.clear();
    }

    private HugeEdge toHuge(VirtualEdge edge, VirtualEdgeStatus status) {
        if (edge == null) {
            return null;
        }
        if (status.match(edge.getStatus())) {
            return edge.getEdge();
        } else {
            return null;
        }
    }

    private VirtualVertex toVirtual(VirtualVertex old, HugeVertex hugeVertex,
                                    Iterator<HugeEdge> inEdges) {
        return toVirtual(old, hugeVertex, hugeVertex.getEdges(), inEdges);
    }

    private VirtualVertex toVirtual(VirtualVertex old, HugeVertex hugeVertex,
                                    Collection<HugeEdge> outEdges,
                                    Iterator<HugeEdge> inEdges) {
        if (outEdges == null && hugeVertex.existsEdges()) {
            throw new IllegalArgumentException(
                    "Argument outEdges is null but hugeVertex exists out-edges.");
        }
        VirtualVertex newVertex = new VirtualVertex(hugeVertex, VirtualVertexStatus.Id.code());
        VirtualVertex result = mergeVVOutEdge(old, newVertex, outEdges);
        result = mergeVVInEdge(old, result, inEdges);
        mergeVVProp(old, result, hugeVertex);
        assert result != null;
        return result;
    }

    private VirtualEdge toVirtual(VirtualEdge old, HugeEdge hugeEdge) {
        VirtualEdge newEdge = new VirtualEdge(hugeEdge, VirtualEdgeStatus.Id.code());
        VirtualEdge result = mergeVEProp(old, newEdge, hugeEdge);
        assert result != null;
        return result;
    }

    private VirtualVertex mergeVVOutEdge(VirtualVertex oldV, VirtualVertex newV,
                                         Collection<HugeEdge> outEdges) {
        if (outEdges != null) {
            List<VirtualEdge> outEdgeList = new ArrayList<>();
            outEdges.forEach(e -> {
                VirtualEdge edge = this.putEdge(e);
                assert edge != null;
                outEdgeList.add(edge);
            });
            newV.addOutEdges(outEdgeList);
            newV.orStatus(VirtualVertexStatus.OutEdge);
        }
        if (oldV == null) {
            return newV;
        }
        // new vertex has no out-edges
        if (!VirtualVertexStatus.OutEdge.match(newV.getStatus())) {
            return oldV;
        }
        return newV;
    }

    private VirtualVertex mergeVVInEdge(VirtualVertex oldV, VirtualVertex resultV, Iterator<HugeEdge> inEdges) {
        if (inEdges != null) {
            List<VirtualEdge> inEdgeList = new ArrayList<>();
            inEdges.forEachRemaining(e -> {
                VirtualEdge edge = this.putEdge(e);
                assert edge != null;
                inEdgeList.add(edge);
            });
            resultV.addInEdges(inEdgeList);
            resultV.orStatus(VirtualVertexStatus.InEdge);
        }

        if (oldV == null) {
            return resultV;
        }

        // resultV vertex has no in-edges, copy from old
        if (!VirtualVertexStatus.InEdge.match(resultV.getStatus())) {
            resultV.copyInEdges(oldV);
        }
        return resultV;
    }

    private void mergeVVProp(VirtualVertex oldV, VirtualVertex resultV, HugeVertex hugeVertex) {
        if (hugeVertex.isPropLoaded()) {
            resultV.orStatus(VirtualVertexStatus.Property);
            resultV.getVertex().copyProperties(hugeVertex);
        } else if (oldV != null && oldV != resultV
                && VirtualVertexStatus.Property.match(oldV.getStatus())) {
            resultV.orStatus(VirtualVertexStatus.Property);
            resultV.getVertex().copyProperties(oldV.getVertex());
        }
    }

    private VirtualEdge mergeVEProp(VirtualEdge oldE, VirtualEdge newE, HugeEdge hugeEdge) {
        if (oldE == newE) {
            return oldE;
        }

        if (oldE == null) {
            return newE;
        }

        if (hugeEdge.isPropLoaded()) {
            newE.orStatus(VirtualEdgeStatus.Property);
        }
        else {
            // new edge's properties is not loaded and old edge's properties is loaded,
            // copy properties from old
            if (VirtualVertexStatus.Property.match(oldE.getStatus())) {
                newE.orStatus(VirtualEdgeStatus.Property);
                newE.getEdge().copyProperties(oldE.getEdge());
            }
        }

        return newE;
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