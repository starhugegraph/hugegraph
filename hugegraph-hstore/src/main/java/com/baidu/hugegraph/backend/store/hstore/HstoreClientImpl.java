package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.store.HgSessionManager;
import com.baidu.hugegraph.store.HgStoreConfig;
import com.baidu.hugegraph.store.HgStoreSession;

public class HstoreClientImpl implements HstoreClient {


    @Override
    public void close() {
        assert graph != null;

    }

    HstoreGraph graph;

    @Override
    public HstoreGraph open(String graphName) {
        HgSessionManager manager = HgSessionManager.configOf(HgStoreConfig.of());
        HgStoreSession session = manager.openSession(graphName);
        graph = new HstoreGraphImpl(session);
        return graph;
    }
}
