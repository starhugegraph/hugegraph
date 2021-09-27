package com.baidu.hugegraph.backend.store.hstore;

import com.alipay.sofa.jraft.Iterator;

public interface HstoreBackendIterator extends Iterator {
    boolean   isOwningHandle();

    boolean isValid();

    byte[] key();
}
