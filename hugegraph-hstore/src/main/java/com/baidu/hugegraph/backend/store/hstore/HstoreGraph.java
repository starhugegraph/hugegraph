package com.baidu.hugegraph.backend.store.hstore;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public interface HstoreGraph {


    void batchPut(Map<String, Map<byte[], byte[]>> putBatch);

    void batchDelete(Map<String, Set<byte[]>> deleteBatch);

    void deletePrefix(Map<String, Set<byte[]>> key);

    void deleteRange(String table, byte[] keyFrom, byte[] valueTo);

    void put(String table, byte[] key, byte[] value);

    byte[] get(String table, byte[] key);

    Iterator scan(String table);

    Iterator scanPrefix(String table, byte[] key);

    void merge(String table, byte[] key, byte[] value);

    void deleteRange(Map<String, Pair<byte[], byte[]>> deleteRangeBatch);

    Iterator scan(String table, byte[] keyFrom, byte[] keyTo, int scanType);
}
