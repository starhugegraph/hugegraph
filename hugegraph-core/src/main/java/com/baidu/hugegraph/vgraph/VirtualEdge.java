package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.structure.HugeEdge;

public class VirtualEdge extends VirtualElement {

    private HugeEdge edge;
//    private Id source;
//    private Id target;

    public VirtualEdge( // Id source, Id target, byte[] propertyBuf,
                        HugeEdge edge,
                        VirtualElementStatus status) {
        super( // propertyBuf,
                status);
        this.edge = edge;
    }

    public HugeEdge getEdge() {
        return edge;
    }

//    public Id getSource() {
//        return source;
//    }
//
//    public void setSource(Id source) {
//        this.source = source;
//    }
//
//    public Id getTarget() {
//        return target;
//    }
//
//    public void setTarget(Id target) {
//        this.target = target;
//    }
}
