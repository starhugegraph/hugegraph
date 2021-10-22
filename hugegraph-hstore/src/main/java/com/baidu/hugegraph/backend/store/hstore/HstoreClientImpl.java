package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.store.HgSessionManager;
import com.baidu.hugegraph.store.HgStoreConfig;
import com.baidu.hugegraph.store.HgStoreSession;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class HstoreClientImpl implements HstoreClient {

    @Override
    public void close() {
        assert graph != null;

    }
    HstoreGraph graph;
    @Override
    public HgStoreSession open(String graphName) {
        HgSessionManager manager = HgSessionManager.getInstance();
        HgStoreSession session = manager.openSession(graphName);
        return session;
    }
}
