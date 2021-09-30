package com.baidu.hugegraph.backend.store.rocksdb;

import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgSessionManager;
import com.baidu.hugegraph.store.HgStoreConfig;
import com.baidu.hugegraph.store.HgStoreSession;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HstoreClient implements Closeable {
    public static String storeAddrs[] = {
        "localhost:9080",
        "localhost:9081"
    };
    static HgSessionManager hgSessionManagers[];
    static {
        hgSessionManagers = new HgSessionManager[storeAddrs.length];
        for (int i = 0; i < storeAddrs.length; i++) {
            hgSessionManagers[i] = HgSessionManager.configOf(HgStoreConfig.of(storeAddrs[i]));
        }
    }

    public HstoreClient create(String graphName){
        HstoreClient client = new HstoreClient();
        client.open(graphName);

        return client;
    }

    private HgStoreSession sessions[];
    public void open(String graphName){
        for (int i = 0; i < hgSessionManagers.length; i++) {
            sessions[i] = hgSessionManagers[i].openSession(graphName);
        }
    }

    public boolean put(String table, byte[] key, byte[] value){
        int parId = key[0] % sessions.length;
        return sessions[parId].put(table, key, value);
    }

    public byte[] get(String table, byte[] key) {
        int parId = key[0] % sessions.length;
        return sessions[parId].get(table, key);
    }

    public List<HgKvEntry> scanAll(String table){
        List<HgKvEntry> entries = new LinkedList<>();
        for(HgStoreSession session : sessions){
            entries.addAll(session.scanAll(table));
        }
        return entries;
    }

    public List<HgKvEntry> scan(String table, byte[] startKey, byte[] endKey, int limit){
        List<HgKvEntry> entries = new LinkedList<>();
        for(HgStoreSession session : sessions){
            entries.addAll(session.scan(table, startKey, endKey, limit));
        }
        return entries;
    }

    public List<HgKvEntry> scanPrefix(String table, byte[] keyPrefix){
        List<HgKvEntry> entries = new LinkedList<>();
        for(HgStoreSession session : sessions){
            entries.addAll(session.scanPrefix(table, keyPrefix));
        }
        return entries;
    }


    @Override
    public void close() throws IOException {

    }
}
