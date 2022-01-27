package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;

public class VirtualEdge extends VirtualElement {

    private final Id ownerVertexId;
    private final Directions directions;
    private final EdgeLabel edgeLabel;
    private final String name;
    private final Id otherVertexId;

    public VirtualEdge(HugeEdge edge, byte status) {
        super(edge, status);
        this.ownerVertexId = edge.id().ownerVertexId();
        this.directions = edge.direction();
        this.edgeLabel = edge.schemaLabel();
        this.name = edge.name();
        this.otherVertexId = edge.id().otherVertexId();
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
}
