package com.baidu.hugegraph.vgraph;

public enum VirtualEdgeStatus {
    None(0x00, "none"),
    OK(0x01, "ok");

    private byte code;
    private String name;

    VirtualEdgeStatus(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public boolean match(VirtualEdgeStatus other) {
        if (other == OK) {
            return this == OK;
        }
        return (this.code & other.code) != 0;
    }

    public boolean match(byte other) {
        if (other == OK.code) {
            return this == OK;
        }
        return (this.code & other) != 0;
    }

    public VirtualEdgeStatus or(VirtualEdgeStatus other) {
        return fromCode(or(other.code));
    }

    public byte or(byte other) {
        return ((byte) (this.code | other));
    }

    public static VirtualEdgeStatus fromCode(byte code) {
        switch (code) {
            case 0x00:
                return None;
            case 0x01:
                return OK;
            default:
                throw new IllegalStateException("Unexpected value: " + code);
        }
    }
}
