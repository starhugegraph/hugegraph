package com.baidu.hugegraph.vgraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.structure.HugeVertex;

public class VirtualVertex extends VirtualElement {

    private HugeVertex vertex;
    private List<VirtualEdge> outEdges;
    private List<VirtualEdge> inEdges;

    public VirtualVertex(HugeVertex vertex, byte status) {
        super( // propertyBuf,
                status);
        this.vertex = vertex;
        this.outEdges = null;
        this.inEdges = null;
    }

    public HugeVertex getVertex() {
        return vertex;
    }

    public boolean expired() {
        return vertex.expired();
    }

    public Iterator<VirtualEdge> getEdges() {
        ExtendableIterator<VirtualEdge> result = new ExtendableIterator<>();
        if (this.outEdges != null) {
            result.extend(this.outEdges.listIterator());
        }
        if (this.inEdges != null) {
            result.extend(this.inEdges.listIterator());
        }
        return result;
    }

    public void addOutEdges(List<VirtualEdge> edges) {
        assert edges != null;
        this.outEdges = new ArrayList<>(edges);;
    }

    public void addInEdges(List<VirtualEdge> edges) {
        this.inEdges = new ArrayList<>(edges);
    }

    public void copyInEdges(VirtualVertex other) {
        this.inEdges = other.inEdges;
    }

    void orStatus(VirtualVertexStatus status) {
        this.status = status.or(this.status);
    }
}
