package com.baidu.hugegraph.vgraph;

public enum VirtualElementStatus {
    Init(0x00, "init"),
    OK(0x01, "ok");

    private byte code;
    private String name;

    VirtualElementStatus(int code, String name) {
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

    public static VirtualElementStatus fromCode(byte code) {
        switch (code) {
            case 0x00:
                return Init;
            case 0x08:
                return OK;
            default:
                throw new IllegalStateException("Unexpected value: " + code);
        }
    }
}
