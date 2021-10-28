package com.baidu.hugegraph.backend.store.rocksdb;

import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.store.*;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

public class HstoreClient implements Closeable {

    static boolean initialized = false;

    private static void initNodeManager(HugeConfig config) {
        if (initialized) return;
        String peers[] = config.get(RocksDBOptions.PD_PEERS).split(",");
        HgStoreNodeManager node = HgStoreNodeManager.getInstance();
        for (String peer : peers) {
            node.addNode("hugegraph/g", node.getNodeBuilder().setAddress(peer).build());
        }
        node.addNode("hugegraph/s", node.getNodeBuilder().setAddress(peers[0]).build());
        node.addNode("hugegraph/m", node.getNodeBuilder().setAddress(peers[0]).build());
        initialized = true;
    }

    public static HstoreClient create(HugeConfig config, String graphName){
        initNodeManager(config);
        HstoreClient client = new HstoreClient();
        client.open(graphName);

        return client;
    }

    ThreadLocal<Map<String, Map<HgOwnerKey, byte[]>>> threadLocalBuffers = new ThreadLocal<>();

    private HgStoreSession session;
    public void open(String graphName){
        session = HgSessionManager.getInstance().openSession(graphName);
    }



    /**
     * 1、Storeproxy获取Key所属的点ID
     * 2、Storeproxy计算点ID所属PartitionID
     * 3、Storeproxy从PD获取Partition Leader所在的HgStore
     * 4、Storeproxy发送PartitionID、KV给HgStore
     * 5、HgStore判断PartitionID是否属于该HgStore
     * 6、HgStore修改Key，增加PartitonID作为前缀
     * 7、HgStore调用rocksdb存储修改后的KV
     */
    public boolean put(String table, byte[] owner, byte[] key, byte[] value){
        Map<String, Map<HgOwnerKey, byte[]>> buffers = threadLocalBuffers.get();
        if ( buffers == null ){
            buffers = new HashMap();
            threadLocalBuffers.set(buffers);
        }
        Map<HgOwnerKey, byte[]> entries = buffers.get(table);
        if ( entries == null ) {
            entries = new HashMap<>();
            buffers.put(table, entries);
        }
        entries.put(new HgOwnerKey(owner, key), value);
        return true;
        //return session.put(table, new HgOwnerKey(owner, key), value);
    }

    public byte[] get(String table, byte[] key) {

        return session.get(table,new HgOwnerKey(HgStoreClientConst.ALL_NODE_OWNER,
                                                key));
    }

    public  boolean commit() {
        boolean result = false;
        Map<String, Map<HgOwnerKey, byte[]>> buffers = threadLocalBuffers.get();
        if ( buffers != null && buffers.size() > 0)
            result = session.batchPutOwner(buffers);
        else{
            result = false;
        }
        buffers.clear();
        return result;
    }

    public HgKvIterator<HgKvEntry> scanAll(String table){
//        return session.scanIterator(table);
        return null;
    }

    public List<HgKvEntry> scan(String table, byte[] startKey, byte[] endKey, int limit){
//        return session.scanIterator(table, startKey, endKey, limit);
        return null;
    }

    public List<HgKvEntry> scanPrefix(String table, byte[] owner, byte[] keyPrefix){
//        return session.scanPrefix(table, keyPrefix);
        return null;
    }


    @Override
    public void close() throws IOException {

    }


    public static void main(String[] args){
        HgStoreSession session = HgSessionManager.getInstance().openSession("hugegraph/g");

        session.put("a", new HgOwnerKey("0".getBytes(),"0".getBytes()),"0".getBytes());
        session.put("a", new HgOwnerKey("0".getBytes(),"0".getBytes()),"1".getBytes());


    }
}
