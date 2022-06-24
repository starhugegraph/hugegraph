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
    private static final int LOAD_ITEM_ONCE = 200;
    private int verticesOffset = 0;
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
        for(int i=0; i< fetchLen ;++i){
            boolean loaded = false;
            for(Iterator<Edge> edges : edgeIters){
                if (edges.hasNext()){
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id id = edge.id().ownerVertexId();
                    List<Edge> edgeList = bufferedData.get(code(id));
                    if (edgeList != null) {
                        edgeList.add(edge);
                    }
                    loaded = true;
                }
            }
            if (loaded == false){
                return false;
            }
        }
        // may load more data
        return true;
    }

    protected void loadMore(){
        if(haveMoreData == true){
            /* load not balance */
            haveMoreData = doBufferData(LOAD_ITEM_ONCE);
        }
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
            bufferWrapIter.loadMore();
            more = (edges.size() - offset.get()) > 0;
            return more;
        }

        @Override
        public Edge next() {
            return edges.get(offset.getAndAdd(1));
        }
    }
}
