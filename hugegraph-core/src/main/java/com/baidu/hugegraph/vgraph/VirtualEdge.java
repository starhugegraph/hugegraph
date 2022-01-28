package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.SerialEnum;

public class VirtualEdge extends VirtualElement {

    private Id ownerVertexId;
    private Directions directions;
    private EdgeLabel edgeLabel;
    private String name;
    private Id otherVertexId;

    public VirtualEdge(HugeEdge edge, byte status) {
        super(edge, status);
        this.ownerVertexId = edge.id().ownerVertexId();
        this.directions = edge.direction();
        this.edgeLabel = edge.schemaLabel();
        this.name = edge.name();
        this.otherVertexId = edge.id().otherVertexId();
    }

    private VirtualEdge() {
        super();
    }

    public Id getOwnerVertexId() {
        return this.ownerVertexId;
    }

    public HugeEdge getEdge(HugeVertex owner) {
        assert owner.id().equals(this.ownerVertexId);
        boolean direction = EdgeId.isOutDirectionFromCode(this.directions.type().code());
        HugeEdge edge = HugeEdge.constructEdge(owner, direction, this.edgeLabel, this.name, this.otherVertexId);
        edge.expiredTime(this.expiredTime);
        this.fillProperties(edge);
        return edge;
    }

    void orStatus(VirtualEdgeStatus status) {
        this.status = status.or(this.status);
    }

    public void writeToBuffer(BytesBuffer buffer) {
        buffer.writeVLong(this.expiredTime);
        buffer.write(this.status);
        buffer.write(this.directions.code());
        buffer.writeId(this.edgeLabel.id());
        buffer.writeString(this.name);
        buffer.writeId(this.otherVertexId);
        buffer.writeBoolean(this.propertyBuf != null);
        if (this.propertyBuf != null) {
            buffer.writeBytes(this.propertyBuf.bytes());
        }
    }

    public static VirtualEdge readFromBuffer(BytesBuffer buffer, HugeGraph graph, Id ownerVertexId) {
        VirtualEdge edge = new VirtualEdge();
        edge.ownerVertexId = ownerVertexId;
        edge.expiredTime = buffer.readVLong();
        edge.status = buffer.read();
        edge.directions = SerialEnum.fromCode(Directions.class, buffer.read());
        edge.edgeLabel = graph.edgeLabelOrNone(buffer.readId());
        edge.name = buffer.readString();
        edge.otherVertexId = buffer.readId();
        if (buffer.readBoolean()) {
            byte[] propertyBytes = buffer.readBytes();
            edge.propertyBuf = BytesBuffer.wrap(propertyBytes).forReadAll();
        }
        return edge;
    }
}
