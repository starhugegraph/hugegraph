package com.baidu.hugegraph.vgraph;

import java.util.ArrayList;

import com.baidu.hugegraph.structure.HugeVertex;

public class VirtualVertex extends VirtualElement {

    private HugeVertex vertex;
    private ArrayList<VirtualEdge> edges;

    public VirtualVertex(HugeVertex vertex, VirtualElementStatus status) {
        super( // propertyBuf,
                status);
        this.vertex = vertex;
        this.edges = new ArrayList<>();
    }

    public HugeVertex getVertex() {
        return vertex;
    }

    public ArrayList<VirtualEdge> getEdges() {
        return edges;
    }
}
