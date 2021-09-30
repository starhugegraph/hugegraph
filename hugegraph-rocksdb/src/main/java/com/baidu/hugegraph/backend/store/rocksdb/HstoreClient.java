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

    public static HstoreClient create(String graphName){
        HstoreClient client = new HstoreClient();
        client.open(graphName);

        return client;
    }

    private HgStoreSession sessions[];
    public void open(String graphName){
        sessions =  new HgStoreSession[hgSessionManagers.length];
        for (int i = 0; i < hgSessionManagers.length; i++) {
            sessions[i] = hgSessionManagers[i].openSession(graphName);
        }
    }

    /**
     * 根据key获取所在分区
     * 暂时简化取第一个字节
     */
    protected int getPartitionId(byte[] key){
        return (int)key[0] & 0xFF % sessions.length;
    }

    public boolean put(String table, byte[] key, byte[] value){
        int partId = getPartitionId(key);
        return sessions[partId].put(table, key, value);
    }

    public byte[] get(String table, byte[] key) {
        int partId = getPartitionId(key);
        return sessions[partId].get(table, key);
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


    public static void main(String[] args){
        HstoreClient client = HstoreClient.create("test");
        client.put("a", "0".getBytes(), "0".getBytes());
        client.put("a", "1".getBytes(), "1".getBytes());
        List<HgKvEntry>  entries = client.scanAll("a");
        for(HgKvEntry entry : entries){
            System.out.println(new String(entry.key()) + " -- " + new String(entry.value()));
        }
        byte[] key = new byte[1]; key[0] = (byte) -1;
        System.out.printf("partId " + client.getPartitionId(key));
    }
}
