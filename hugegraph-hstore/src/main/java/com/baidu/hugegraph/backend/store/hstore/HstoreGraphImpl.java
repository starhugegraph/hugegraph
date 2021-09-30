package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.store.HgStoreSession;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Set;

public class HstoreGraphImpl implements HstoreGraph {

    HgStoreSession session;

    public HstoreGraphImpl(HgStoreSession hgStoreSession) {
        this.session = hgStoreSession;
    }

    @Override
    public void batchPut(Map<String, Map<byte[], byte[]>> putBatch) {

    }

    @Override
    public void batchDelete(Map<String, Set<byte[]>> deleteBatch) {

    }

    @Override
    public void deletePrefix(Map<String, Set<byte[]>> key) {

    }

    @Override
    public void deleteRange(String table, byte[] keyFrom, byte[] valueTo) {

    }

    @Override
    public void put(String table, byte[] key, byte[] value) {
        this.session.put(table, key, value);
    }

    @Override
    public byte[] get(String table, byte[] key) {
        return this.session.get(table, key);
    }

    @Override
    public HstoreBackendIterator scan(String table) {
        return new HstoreIterator(this.session.scanAll(table));
    }

    @Override
    public HstoreBackendIterator scanPrefix(String table, byte[] key) {
        return new HstoreIterator(this.session.scanPrefix(table, key));
    }

    @Override
    public void merge(String table, byte[] key, byte[] value) {

    }

    @Override
    public void deleteRange(Map<String, Pair<byte[], byte[]>> deleteRangeBatch) {

    }

    @Override
    public HstoreBackendIterator scan(String table, byte[] keyFrom, byte[] keyTo, int scanType) {
        return new HstoreIterator(this.session.scan(table, keyFrom, keyTo, scanType));
    }
}
