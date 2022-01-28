package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import java.nio.ByteBuffer;
import java.util.Collection;

public abstract class VirtualElement {

    protected static final BinarySerializer SERIALIZER = new BinarySerializer();

    protected byte status;
    protected long expiredTime;
    protected BytesBuffer propertyBuf;


    protected VirtualElement(HugeElement element,
                             byte status) {
        assert element != null;
        this.expiredTime = element.expiredTime();
        this.status = status;
        if (element.hasProperties()) {
            setProperties(element.getProperties().values());
        }
    }

    protected VirtualElement() { }

    public void fillProperties(HugeElement owner) {
        if (propertyBuf != null) {
            ByteBuffer byteBuffer = propertyBuf.asByteBuffer();
            BytesBuffer wrapedBuffer = BytesBuffer.wrap(byteBuffer.array(), 0, byteBuffer.position());
            SERIALIZER.parseProperties(wrapedBuffer, owner);
        }
    }

    public void setProperties(Collection<HugeProperty<?>> properties) {
        if (properties != null && !properties.isEmpty()) {
            BytesBuffer buffer = new BytesBuffer();
            SERIALIZER.formatProperties(properties, buffer);
            propertyBuf = buffer;
        } else {
            propertyBuf = null;
        }
    }

    public void copyProperties(VirtualElement element) {
        propertyBuf = element.propertyBuf;
    }

    public byte getStatus() {
        return status;
    }
}
