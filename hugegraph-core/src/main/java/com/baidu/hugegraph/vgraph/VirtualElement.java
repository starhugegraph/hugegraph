package com.baidu.hugegraph.vgraph;


public abstract class VirtualElement {

    protected byte status;
    // protected byte[] propertyBuf;

    protected VirtualElement( // byte[] propertyBuf,
                             byte status) {
        // this.propertyBuf = propertyBuf;
        this.status = status;
    }

//    public byte[] getPropertyBuf() {
//        return propertyBuf;
//    }

    public byte getStatus() {
        return status;
    }
}
