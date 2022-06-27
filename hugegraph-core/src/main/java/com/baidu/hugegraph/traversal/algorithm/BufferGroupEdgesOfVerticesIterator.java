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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.collection.CollectionFactory;
import com.baidu.hugegraph.util.collection.MappingFactory;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;

public class BufferGroupEdgesOfVerticesIterator implements Iterator<CIter<Edge>> {
     private static final int MAX_LOAD_ITEMS = 1000*10000;
    private static final int LOAD_ITEM_ONCE = 200;
    private int verticesOffset = 0;
    private volatile int loadedEdgesCount = 0;
    private boolean concurrent = false;
    private boolean haveMoreData = true;
    private HugeTraverser.EdgesOfVerticesIterator edgeIts;
    private List<Id> vertices;
    private List<Iterator<Edge>> edgeIters;
    private Map<Integer, List<Edge>> bufferedData;
    private Map<Integer, AtomicInteger> remainingDataCount;
    private final ObjectIntMapping<Id> idMapping;
    private long degree = HugeTraverser.NO_LIMIT;

    public BufferGroupEdgesOfVerticesIterator(HugeTraverser.EdgesOfVerticesIterator edgeIts,
                                              List<Id> requireVertices, long degree) {
        this.edgeIts = edgeIts;
        this.vertices = requireVertices;
        this.degree = degree;
        if (vertices == null) {
            vertices = CollectionFactory.newList(CollectionType.JCF);
        }
        this.edgeIters = CollectionFactory.newList(CollectionType.JCF);
        this.idMapping = MappingFactory.newObjectIntMapping(concurrent);
        this.bufferedData = CollectionFactory.newMap(CollectionType.JCF);
        this.remainingDataCount = CollectionFactory.newMap(CollectionType.JCF);

        for (Id id : requireVertices) {
            bufferedData.put(code(id), CollectionFactory.newList(CollectionType.JCF));
            remainingDataCount.put(code(id), new AtomicInteger(0));
        }
        while(edgeIts.hasNext()){
            edgeIters.add(edgeIts.next());
        }
    }

    public BufferGroupEdgesOfVerticesIterator(HugeTraverser.EdgesOfVerticesIterator edgeIts,
                                              List<Id> requireVertices) {
        this(edgeIts, requireVertices, HugeTraverser.NO_LIMIT);
    }

    private int code(Id id) {
        return idMapping.object2Code(id);
    }

    private boolean doBufferData(int fetchLen) {
        int count = 0;
        while (count < fetchLen) {
            boolean loaded = false;
            for (int i = 0; i < edgeIters.size() && count < fetchLen && loadedEdgesCount < MAX_LOAD_ITEMS; ++i) {
                Iterator<Edge> edges = edgeIters.get(i);
                if (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id id = edge.id().ownerVertexId();
                    List<Edge> edgeList = bufferedData.get(code(id));
                    if (edgeList != null) {
                        edgeList.add(edge);
                    }
                    loaded = true;
                    ++loadedEdgesCount;
                    ++count;
                }
            }
            if (loaded == false) {
                return false;
            }
        }
        // may load more data
        return true;
    }

    protected synchronized boolean loadMore(){
        int beforeLoadedCount = loadedEdgesCount;
        if(haveMoreData == true && loadedEdgesCount < MAX_LOAD_ITEMS){
            /* load not balance */
            haveMoreData = doBufferData(LOAD_ITEM_ONCE);
        }
        return loadedEdgesCount > beforeLoadedCount;
    }

    private CIter<Edge> getNext() {
        return new CIterWrapper(get(vertices.get(verticesOffset++)));
    }

    @Override
    public boolean hasNext() {
        return verticesOffset < vertices.size();
    }

    @Override
    public CIter<Edge> next() {
        if (hasNext()) {
            return getNext();
        } else {
            return null;
        }
    }

    public Iterator<Edge> get(Id id) {
        return new AutoLoadIterator(this, bufferedData.get(code(id)), degree);
    }

    private class CIterWrapper implements CIter<Edge> {
        private Iterator<Edge> origin;

        public CIterWrapper(Iterator<Edge> origin) {
            this.origin = origin;
        }

        @Override
        public Object metadata(String meta, Object... args) {
            return null;
        }

        @Override
        public void close() throws Exception {
        }

        @Override
        public boolean hasNext() {
            return origin.hasNext();
        }

        @Override
        public Edge next() {
            return origin.next();
        }
    }

    private class AutoLoadIterator implements Iterator<Edge>{
        private boolean more = true;
        private long degree = -1;
        private AtomicInteger offset;
        private BufferGroupEdgesOfVerticesIterator bufferWrapIter;
        private List<Edge> edges;

        public AutoLoadIterator(BufferGroupEdgesOfVerticesIterator bufferWrapIter,
                                List<Edge> edges, long degree){
            this.bufferWrapIter = bufferWrapIter;
            this.edges = edges;
            this.offset = new AtomicInteger(0);
            this.degree = degree;
        }

        @Override
        public synchronized boolean hasNext() {
            if( degree != HugeTraverser.NO_LIMIT && offset.get()>= degree){
                return false;
            }
            int remaining = edges.size() - offset.get() - 1;
            if(remaining > 0){
                return true;
            }else if(more == false){
                return false;
            }
            while(true){
                boolean loaded = bufferWrapIter.loadMore();;
                more = (edges.size() - offset.get()) > 0;
                if(loaded == false || more == true){
                    break;
                }
            }
            return more;
        }

        @Override
        public Edge next() {
            return edges.get(offset.getAndAdd(1));
        }
    }
}
