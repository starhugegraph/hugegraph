package com.baidu.hugegraph.vgraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;

public class VirtualVertex extends VirtualElement {

    private Id id;
    private VertexLabel label;
    private List<VirtualEdge> outEdges;
    private List<VirtualEdge> inEdges;

    public VirtualVertex(HugeVertex vertex, byte status) {
        super(vertex, status);
        assert vertex != null;
        this.id = vertex.id();
        this.label = vertex.schemaLabel();
        this.outEdges = null;
        this.inEdges = null;
    }

    public HugeVertex getVertex(HugeGraph graph) {
        HugeVertex vertex = new HugeVertex(graph, id, label);
        vertex.expiredTime(this.expiredTime);
        this.fillProperties(vertex);
        return vertex;
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
