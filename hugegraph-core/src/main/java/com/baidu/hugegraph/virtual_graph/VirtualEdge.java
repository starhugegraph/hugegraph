package com.baidu.hugegraph.virtual_graph;

public class VirtualEdge {
    private short status;
    public VirtualVertex source;
    public VirtualVertex target;
    public Byte[] propertyBuf;
    public VirtualEdge() {

    }
}
