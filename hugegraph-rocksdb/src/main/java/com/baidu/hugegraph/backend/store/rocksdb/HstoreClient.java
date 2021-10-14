package com.baidu.hugegraph.backend.store.rocksdb;

import com.baidu.hugegraph.store.*;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;
import javafx.util.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HstoreClient implements Closeable {
    public static String storeAddrs[] = {
        "localhost:9080"
    };
    static HgSessionManager hgSessionManagers[];
    static {
        HgStoreNodeManager node = HgStoreNodeManager.getInstance();
        node.addNode("hugegraph/g", node.getNodeBuilder().setAddress("localhost:9180").build());
        node.addNode("hugegraph/g", node.getNodeBuilder().setAddress("localhost:9280").build());
        node.addNode("hugegraph/g", node.getNodeBuilder().setAddress("localhost:9380").build());
        node.addNode("hugegraph/s", node.getNodeBuilder().setAddress("localhost:9180").build());
        node.addNode("hugegraph/m", node.getNodeBuilder().setAddress("localhost:9180").build());
    }

    public static HstoreClient create(String graphName){
        HstoreClient client = new HstoreClient();
        client.open(graphName);

        return client;
    }

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

        return session.put(table, new HgOwnerKey(owner, key), value);
    }

    public byte[] get(String table, byte[] key) {

        return session.get(table, key);
    }

    public boolean batchPut(String table, Map<byte[],byte[]> pairs){
        int partId = 0;
        Map<String, Map<byte[], byte[]>> entries = new HashMap<>();
        entries.put(table, pairs);
        return session.batchPut(entries);
    }

    public List<HgKvEntry> scanAll(String table){
        return session.scanAll(table);
    }

    public List<HgKvEntry> scan(String table, byte[] startKey, byte[] endKey, int limit){
        return session.scan(table, startKey, endKey, limit);
    }

    public List<HgKvEntry> scanPrefix(String table, byte[] keyPrefix){
        return session.scanPrefix(table, keyPrefix);
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
