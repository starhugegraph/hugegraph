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

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.collection.CollectionFactory;

public class MergingEdgesOfVerticesIterator implements Iterator<CIter<Edge>>, AutoCloseable {
    private List<Id> vertices;
    private int currentVerticesIndex;

    private Iterator<CIter<Edge>> edgeIts;
    private List<Iterator<Edge>> edgeIters;
    private List<Edge> edgeItersFirstItem;
    private List<Integer> idOffsetToIterIndex;
    private Edge next;
    boolean hasNextCIter = true;

    public MergingEdgesOfVerticesIterator(Iterator<CIter<Edge>> edgeIts,
                                          List<Id> requireVertices) {
        this.edgeIts = edgeIts;
        this.vertices = requireVertices;
        this.edgeIters = CollectionFactory.newList(CollectionType.JCF);
        this.edgeItersFirstItem = CollectionFactory.newList(CollectionType.JCF);
        this.idOffsetToIterIndex = CollectionFactory.newList(CollectionType.JCF);
        for (Id id : vertices) {
            idOffsetToIterIndex.add(-1);
        }
        while (edgeIts.hasNext()) {
            edgeIters.add(edgeIts.next());
            edgeItersFirstItem.add(null);
        }
    }

    private Edge getNextFromIter(int index, Id source) {
        if (edgeItersFirstItem.get(index) != null) {
            HugeEdge edge = (HugeEdge) edgeItersFirstItem.get(index);
            if (edge.id().ownerVertexId().equals(source)) {
                edgeItersFirstItem.set(index, null);
                return edge;
            }
        } else {
            if (edgeIters.get(index).hasNext() == false) {
                return null;
            }
            HugeEdge edge = (HugeEdge) edgeIters.get(index).next();
            if (edge.id().ownerVertexId().equals(source)) {
                return edge;
            }
            edgeItersFirstItem.set(index, edge);
        }
        return null;
    }

    private Edge load() {
        for (; currentVerticesIndex < vertices.size(); currentVerticesIndex++) {
            if (idOffsetToIterIndex.get(currentVerticesIndex) >= 0) {
                Edge edge = getNextFromIter(idOffsetToIterIndex.get(currentVerticesIndex),
                        vertices.get(currentVerticesIndex));
                if (edge != null) {
                    return edge;
                }
                continue;
            }
            /* vertice first read next */
            for (int i = 0; i < edgeIters.size(); ++i) {
                Edge edge = getNextFromIter(i, vertices.get(currentVerticesIndex));
                if (edge != null) {
                    idOffsetToIterIndex.set(currentVerticesIndex, i);
                    return edge;
                }
            }
        }
        return null;
    }

    private synchronized void loadMore() {
        if (next == null) {
            next = load();
        }
    }

    protected boolean haveMore() {
        loadMore();
        return next != null;
    }

    protected Edge getNext() {
        loadMore();
        Edge edge = next;
        next = null;
        return edge;
    }

    @Override
    public void close() throws Exception {
        for (Iterator<Edge> iter : edgeIters) {
            ((CIter<Edge>) iter).close();
        }
    }

    @Override
    public boolean hasNext() {
        return hasNextCIter;
    }

    @Override
    public CIter<Edge> next() {
        hasNextCIter = false;
        return new CIterWrapper(new AutoLoadIterator());
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

    private class AutoLoadIterator implements Iterator<Edge> {

        @Override
        public synchronized boolean hasNext() {
            return MergingEdgesOfVerticesIterator.this.haveMore();
        }

        @Override
        public Edge next() {
            return MergingEdgesOfVerticesIterator.this.getNext();
        }
    }
}
