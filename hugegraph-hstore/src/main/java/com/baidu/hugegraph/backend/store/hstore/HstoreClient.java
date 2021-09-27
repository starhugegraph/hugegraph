package com.baidu.hugegraph.backend.store.hstore;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.RocksIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface HstoreClient {

    void close();

    void batchPut(Map<ByteString, HashMap<ByteString, ByteString>> putBatch);

    void batchDelete(Map<ByteString, HashSet<ByteString>> deleteBatch);

    void deletePrefix(Map<ByteString, HashSet<ByteString>> key);

    void deleteRange(ByteString key, ByteString value);

    void put(ByteString key, ByteString value);

    Optional<ByteString> get(String table,byte[] key);

    Iterator scan(ByteString key);

    Iterator scanPrefix(String table,byte[] key);

    void open();

    void merge(String table, byte[] key, byte[] value);

    void deleteRange(Map<ByteString, Pair<ByteString, ByteString>> deleteRangeBatch);

    Iterator scan(String table, byte[] keyFrom, byte[] keyTo, int scanType);
}
