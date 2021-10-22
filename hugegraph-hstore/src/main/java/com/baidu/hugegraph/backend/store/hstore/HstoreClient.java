package com.baidu.hugegraph.backend.store.hstore;


import com.baidu.hugegraph.store.HgStoreSession;

public interface HstoreClient {

    void close();
    HgStoreSession open(String graphName);
}
