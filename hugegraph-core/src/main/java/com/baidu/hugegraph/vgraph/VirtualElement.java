package com.baidu.hugegraph.vgraph;


public abstract class VirtualElement {

    protected byte status;
    // protected byte[] propertyBuf;

    protected VirtualElement( // byte[] propertyBuf,
                             VirtualElementStatus status) {
        // this.propertyBuf = propertyBuf;
        this.status = status.code();
    }

    public VirtualElementStatus getStatus() {
        return VirtualElementStatus.fromCode(status);
    }

    public void setStatus(VirtualElementStatus status) {
        this.status = status.code();
    }

//    public byte[] getPropertyBuf() {
//        return propertyBuf;
//    }

}
