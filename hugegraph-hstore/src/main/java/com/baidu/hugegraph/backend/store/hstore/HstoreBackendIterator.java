package com.baidu.hugegraph.backend.store.hstore;


import java.util.Iterator;

public interface HstoreBackendIterator extends Iterator {

    byte[] key();

    byte[] value();
}
