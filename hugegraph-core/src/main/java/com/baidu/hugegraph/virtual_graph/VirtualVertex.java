package com.baidu.hugegraph.virtual_graph;

import java.util.ArrayList;

import com.baidu.hugegraph.backend.id.Id;

public class VirtualVertex {
    private short status;
    public Id id;
    public byte[] propertyBuf;
    public ArrayList<VirtualEdge> edges;
    public VirtualVertex() {

    }
}
