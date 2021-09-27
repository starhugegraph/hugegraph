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
    HstoreGraph open(String graphName);
}
