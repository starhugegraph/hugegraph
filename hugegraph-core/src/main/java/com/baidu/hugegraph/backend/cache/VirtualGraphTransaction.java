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

package com.baidu.hugegraph.backend.cache;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.ram.RamTable;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.ListIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.vgraph.VirtualVertexStatus;
import com.baidu.hugegraph.vgraph.VirtualGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;


public final class VirtualGraphTransaction extends GraphTransaction {

//    private static final int MAX_CACHE_PROPS_PER_VERTEX = 10000;
//    private static final int MAX_CACHE_EDGES_PER_QUERY = 100;

    private final VirtualGraph vgraph;

    public VirtualGraphTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        this.vgraph = graph.vgraph();
    }

    @Override
    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVerticesByIds((IdQuery) query);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    private Iterator<HugeVertex> queryVerticesByIds(IdQuery query) {
        IdQuery newQuery = new IdQuery(HugeType.VERTEX, query);
        List<HugeVertex> vertices = new ArrayList<>();
        for (Id vertexId : query.ids()) {
            HugeVertex vertex = this.vgraph.queryVertexById(
                    vertexId, VirtualVertexStatus.Id);
            if (vertex == null) {
                newQuery.query(vertexId);
            } else if (vertex.expired()) {
                newQuery.query(vertexId);
                this.vgraph.invalidateVertex(vertexId);
            } else {
                vertices.add(vertex);
            }
        }

        // Join results from cache and backend
        ExtendableIterator<HugeVertex> results = new ExtendableIterator<>();
        if (!vertices.isEmpty()) {
            results.extend(vertices.iterator());
        } else {
            // Just use the origin query if find none from the cache
            newQuery = query;
        }

        if (!newQuery.empty()) {
            Iterator<HugeVertex> rs = super.queryVerticesFromBackend(newQuery);
            // Generally there are not too much data with id query
            ListIterator<HugeVertex> listIterator = QueryResults.toList(rs);
            this.vgraph.putVerteies(listIterator.list().iterator());
            results.extend(listIterator);
        }

        return results;
    }

    @Override
    @Watched
    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        RamTable ramtable = this.params().ramtable();
        if (ramtable != null && ramtable.matched(query)) {
            return ramtable.query(query);
        }

        if (query.empty() || query.paging() || query.bigCapacity()) {
            // Query all edges or query edges in paging, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        List<HugeEdge> edges = new ArrayList<>();
        Query newQuery = query;
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            // Query from vgraph
            newQuery = queryEdgesFromVirtualGraphByEIds(query, edges);
        }
        else if (!query.conditions().isEmpty()) {
            newQuery = queryEdgesFromVirtualGraph(query, edges);
        }

        if (newQuery == null) {
            return edges.iterator();
        }

        ExtendableIterator<HugeEdge> results = new ExtendableIterator<>();
        if (!edges.isEmpty()) {
            results.extend(edges.iterator());
        }

        if (!newQuery.empty()) {

            Iterator<HugeEdge> rs = super.queryEdgesFromBackend(newQuery);

            /*
             * Iterator can't be cached, caching list instead
             * there may be super node and too many edges in a query,
             * try fetch a few of the head results and determine whether to cache.
             */
            List<HugeEdge> edgesRS = new ArrayList<>();
            rs.forEachRemaining(edgesRS::add);
            if (!edgesRS.isEmpty()) {
                putEdgesToVirtualGraph(newQuery, edgesRS.listIterator());
                results.extend(edgesRS.listIterator());
            }
        }
        return results;
    }

    private void putEdgesToVirtualGraph(Query query, Iterator<HugeEdge> edges) {
        if (query instanceof ConditionQuery) {
            ConditionQuery coditionQuery = (ConditionQuery) query;
            Id vId = coditionQuery.condition(HugeKeys.OWNER_VERTEX);
            if (vId != null) {
                HugeVertex vertex = HugeVertex.create(this, vId, VertexLabel.undefined(this.graph()));
                edges.forEachRemaining(vertex::addEdge);
                this.vgraph.putVertex(vertex);
            }
        }
        else {
            this.vgraph.putEdges(edges);
        }
    }

    private Query queryEdgesFromVirtualGraphByEIds(Query query, List<HugeEdge> edges) {
        IdQuery newQuery = new IdQuery(HugeType.EDGE, query);
        for (Id edgeId : query.ids()) {
            HugeEdge edge = this.vgraph.queryEdgeById(edgeId);
            if (edge == null) {
                newQuery.query(edgeId);
            } else if (edge.expired()) {
                newQuery.query(edgeId);
                this.vgraph.invalidateEdge(edgeId);
            } else {
                edges.add(edge);
            }
        }
        return newQuery;
    }

    private Query queryEdgesFromVirtualGraph(Query query, List<HugeEdge> results) {
        if (query instanceof ConditionQuery) {
            ConditionQuery conditionQuery = (ConditionQuery) query;
            Id vId = conditionQuery.condition(HugeKeys.OWNER_VERTEX);
            if (vId != null) {
                this.getQueryEdgesFromVirtualGraph(vId, conditionQuery, results);
                if (results.size() <= 0) {
                    // query all edges of this vertex from backend
                    ConditionQuery vertexAllEdgeQuery = constructEdgesQuery(vId, Directions.BOTH);
                    Iterator<HugeEdge> allEdges = super.queryEdgesFromBackend(vertexAllEdgeQuery);
                    putEdgesToVirtualGraph(vertexAllEdgeQuery, allEdges);
                    this.getQueryEdgesFromVirtualGraph(vId, conditionQuery, results);
                }
                return null;
            }
        }

        return query;
    }

    private void getQueryEdgesFromVirtualGraph(Id vId, ConditionQuery query, List<HugeEdge> results) {
        HugeVertex vertex = this.vgraph.queryVertexById(vId, VirtualVertexStatus.Edge);
        if (vertex != null) {
            if (vertex.expired()) {
                this.vgraph.invalidateVertex(vId);
            } else {
                vertex.getEdges().forEach(e -> {
                    if (query.test(e)) {
                        results.add(e);
                    }
                });
            }
        }
    }

    @Override
    protected void commitMutation2Backend(BackendMutation... mutations) {
        // Collect changes before commit
        Collection<HugeVertex> updates = this.verticesInTxUpdated();
        Collection<HugeVertex> deletions = this.verticesInTxRemoved();
        Collection<HugeEdge> updatesE = this.edgesInTxUpdated();
        Collection<HugeEdge> deletionsE = this.edgesInTxRemoved();

        try {
            super.commitMutation2Backend(mutations);

            // Update vertex cache
            for (HugeVertex vertex : updates) {
//                if (vertex.sizeOfSubProperties() > MAX_CACHE_PROPS_PER_VERTEX) {
//                    // Skip large vertex
//                    this.vgraph.invalidateVertex(vertex.id());
//                    continue;
//                }
                this.vgraph.updateIfPresentVertex(vertex);
            }
            // Update edge cache
            this.vgraph.updateIfPresentEdge(updatesE.iterator());

        } finally {
            // invalidate removed vertex in cache whatever success or fail
            int vertexOffset = 0;
            int edgeOffset = 0;
            Id[] vertexIdsDeleted = new Id[deletions.size()];
            Id[] edgeIdsDeleted = new Id[deletionsE.size()];

            for (HugeVertex vertex : deletions) {
                vertexIdsDeleted[vertexOffset++] = vertex.id();
            }
            for (HugeEdge edge : deletionsE) {
                edgeIdsDeleted[edgeOffset++] = edge.id();
            }
            if (vertexOffset > 0) {
                this.vgraph.notifyChanges(Cache.ACTION_INVALID,
                                   HugeType.VERTEX, vertexIdsDeleted);
            }
            if (edgeOffset > 0) {
                this.vgraph.notifyChanges(Cache.ACTION_INVALID,
                        HugeType.EDGE, edgeIdsDeleted);
            }
        }
    }
}
