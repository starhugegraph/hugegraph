package com.baidu.hugegraph.backend.store.hstore;


public interface HstoreClient {

    void close();
    HstoreGraph open(String graphName);
}
