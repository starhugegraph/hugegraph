/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.hstore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

public class HstoreSessionsImpl extends HstoreSessions {

    private final HugeConfig config;
    private HstoreSession session;
    private final Map<String, Integer> tables;
    private final AtomicInteger refCount;
    private static int tableCode = 0;
    private String graphName;

    public HstoreSessionsImpl(HugeConfig config, String database, String store) {
        super(config, database, store);
        this.config = config;
        this.graphName = database;
//        TiConfiguration conf = TiConfiguration.createRawDefault(
//                this.config.get(HstoreOptions.TIKV_PDS));
//        conf.setBatchGetConcurrency(
//                this.config.get(HstoreOptions.TIKV_BATCH_GET_CONCURRENCY));
//        conf.setBatchPutConcurrency(
//                this.config.get(HstoreOptions.TIKV_BATCH_PUT_CONCURRENCY));
//        conf.setBatchDeleteConcurrency(
//                this.config.get(HstoreOptions.TIKV_BATCH_DELETE_CONCURRENCY));
//        conf.setBatchScanConcurrency(
//                this.config.get(HstoreOptions.TIKV_BATCH_SCAN_CONCURRENCY));
//        conf.setDeleteRangeConcurrency(
//                this.config.get(HstoreOptions.TIKV_DELETE_RANGE_CONCURRENCY));
        this.session = new HstoreSession(this.config, database);
        this.tables = new ConcurrentHashMap<>();
        this.refCount = new AtomicInteger(1);
    }

    @Override
    public void open() throws Exception {
        this.session.open();
    }

    @Override
    protected boolean opened() {
        return this.session != null;
    }

    @Override
    public Set<String> openedTables() {
        return this.tables.keySet();
    }

    @Override
    public synchronized void createTable(String... tables) {
        for (String table : tables) {
            this.tables.put(table, tableCode++);
        }
    }

    @Override
    public synchronized void dropTable(String... tables) {
        for (String table : tables) {
            this.tables.remove(table);
        }
    }

    @Override
    public boolean existsTable(String table) {
        return this.tables.containsKey(table);
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final Session newSession() {
        return new HstoreSession(this.config(), this.graphName);
    }

    @Override
    protected synchronized void doClose() {
        this.checkValid();

        if (this.refCount.decrementAndGet() > 0) {
            return;
        }
        assert this.refCount.get() == 0;
        this.tables.clear();
        this.session.close();
    }

    private void checkValid() {
    }

    public static final byte[] encode(String string) {
        return StringEncoding.encode(string);
    }

    public static final String decode(byte[] bytes) {
        return StringEncoding.decode(bytes);
    }

    /**
     * HstoreSession implement for hstore
     */
    private final class HstoreSession extends Session {

        private Map<String, Map<byte[], byte[]>> putBatch;
        private Map<String, Set<byte[]>> deleteBatch;
        private Map<String, Set<byte[]>> deletePrefixBatch;
        private Map<String, Pair<byte[], byte[]>> deleteRangeBatch;
        private volatile HstoreClient client;
        private HstoreGraph graph;
        private String graphName;

        public HstoreSession(HugeConfig conf, String graphName) {
            this.putBatch = new HashMap<>();
            this.deleteBatch = new HashMap<>();
            this.deletePrefixBatch = new HashMap<>();
            this.deleteRangeBatch = new HashMap<>();
            this.graphName = graphName;
        }

        public HstoreClient getClient() {
            return client;
        }

        @Override
        public void open() {
            this.graph=this.client.open(this.graphName);
            this.opened = true;
        }

        @Override
        public void close() {
            assert this.closeable();
            try {
                this.client.close();
                this.opened = false;
            } catch (Exception e) {

            }
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public void reset() {
            this.putBatch = new HashMap();
            this.deleteBatch = new HashMap<>();
            this.deletePrefixBatch = new HashMap<>();
            this.deleteRangeBatch = new HashMap<>();
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.size() > 0;
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {

            int count = this.size();
            if (count <= 0) {
                return 0;
            }

            if (this.putBatch.size() > 0) {
                this.graph.batchPut(this.putBatch);
                this.putBatch.clear();
            }

            if (this.deleteBatch.size() > 0) {
                this.graph.batchDelete(this.deleteBatch);
                this.deleteBatch.clear();
            }

            if (this.deletePrefixBatch.size() > 0) {
                this.graph.deletePrefix(this.deletePrefixBatch);
                this.deletePrefixBatch.clear();
            }

            if (this.deleteRangeBatch.size() > 0) {
                this.graph.deleteRange(this.deleteRangeBatch);
                this.deleteRangeBatch.clear();
            }

            return count;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            this.putBatch.clear();
            this.deleteBatch.clear();
            this.deletePrefixBatch.clear();
            this.deleteRangeBatch.clear();
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            // TODO: get first key and lastkey of fake tikv table
            return null;
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] key, byte[] value) {
            byte[] bs = toKey(table);
            Map valueMap = this.putBatch.get(table.getBytes());
            if (valueMap == null) {
                valueMap = new HashMap();
                this.putBatch.put(table, valueMap);
            }
            valueMap.put(key, value);
        }

        @Override
        public synchronized void increase(String table, byte[] key, byte[] value) {
            this.graph.merge(table, key, value);
        }

        @Override
        public void delete(String table, byte[] key) {
            Map valueMap = this.putBatch.get(table);
            if (valueMap != null) {
                if (valueMap.remove(key) != null) {
                    return;
                }
            }
            Set valueSet = this.deleteBatch.get(table);
            if (valueSet == null) {
                valueSet = new HashSet();
                this.deleteBatch.put(table, valueSet);
            }
            valueSet.add(key);
        }

        @Override
        public void deletePrefix(String table, byte[] key) {
            Set valueSet = this.deletePrefixBatch.get(table);
            if (valueSet == null) {
                valueSet = new HashSet();
                this.deletePrefixBatch.put(table, valueSet);
            }
            valueSet.add(key);
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void deleteRange(String table, byte[] keyFrom, byte[] keyTo) {
            this.deleteRangeBatch.put(table, new MutablePair<byte[], byte[]>(
                    keyFrom,
                    keyTo
            ));
        }

        @Override
        public byte[] get(String table, byte[] key) {
            byte[] values = this.graph.get(table, key);
            return values != null ? values : new byte[0];
        }

        @Override
        public BackendColumnIterator scan(String table) {
            assert !this.hasChanges();
            Iterator results = this.graph.scan(table);
            return new ColumnIterator(table, (HstoreBackendIterator) results);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            Iterator results = this.graph.scanPrefix(table, prefix);
            return new ColumnIterator(table, (HstoreBackendIterator) results);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            assert !this.hasChanges();
            Iterator results = this.graph.scan(table, keyFrom, keyTo, scanType);
            return new ColumnIterator(table, (HstoreBackendIterator) results, keyFrom, keyTo, scanType);
        }

        private byte[] toKey(String key) {
            return key.getBytes();
        }

        private int size() {
            return this.putBatch.size() + this.deleteBatch.size() +
                    this.deletePrefixBatch.size() + this.deleteRangeBatch.size();
        }
    }

    private static class ColumnIterator<T extends HstoreBackendIterator>
            implements BackendColumnIterator, Countable {

        private final String table;
        private final T iter;

        private final byte[] keyBegin;
        private final byte[] keyEnd;
        private final int scanType;

        private byte[] position;
        private byte[] value;
        private boolean matched;

        public ColumnIterator(String table, T results) {
            this(table, results, null, null, 0);
        }

        public T iter() {
            return iter;
        }

        public ColumnIterator(String table, T results,
                              byte[] keyBegin, byte[] keyEnd, int scanType) {
            E.checkNotNull(results, "results");
            this.table = table;
            this.iter = results;
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.scanType = scanType;
            this.position = keyBegin;
            this.value = null;
            this.matched = false;

            this.checkArguments();
        }

        private void checkArguments() {
//            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
//                            this.match(Session.SCAN_PREFIX_END)),
//                    "Can't set SCAN_PREFIX_WITH_BEGIN and " +
//                            "SCAN_PREFIX_WITH_END at the same time");
//
//            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
//                            this.match(Session.SCAN_GT_BEGIN)),
//                    "Can't set SCAN_PREFIX_WITH_BEGIN and " +
//                            "SCAN_GT_BEGIN/SCAN_GTE_BEGIN at the same time");
//
//            E.checkArgument(!(this.match(Session.SCAN_PREFIX_END) &&
//                            this.match(Session.SCAN_LT_END)),
//                    "Can't set SCAN_PREFIX_WITH_END and " +
//                            "SCAN_LT_END/SCAN_LTE_END at the same time");
//
//            if (this.match(Session.SCAN_PREFIX_BEGIN)) {
//                E.checkArgument(this.keyBegin != null,
//                        "Parameter `keyBegin` can't be null " +
//                                "if set SCAN_PREFIX_WITH_BEGIN");
//                E.checkArgument(this.keyEnd == null,
//                        "Parameter `keyEnd` must be null " +
//                                "if set SCAN_PREFIX_WITH_BEGIN");
//            }
//
//            if (this.match(Session.SCAN_PREFIX_END)) {
//                E.checkArgument(this.keyEnd != null,
//                        "Parameter `keyEnd` can't be null " +
//                                "if set SCAN_PREFIX_WITH_END");
//            }
//
//            if (this.match(Session.SCAN_GT_BEGIN)) {
//                E.checkArgument(this.keyBegin != null,
//                        "Parameter `keyBegin` can't be null " +
//                                "if set SCAN_GT_BEGIN or SCAN_GTE_BEGIN");
//            }
//
//            if (this.match(Session.SCAN_LT_END)) {
//                E.checkArgument(this.keyEnd != null,
//                        "Parameter `keyEnd` can't be null " +
//                                "if set SCAN_LT_END or SCAN_LTE_END");
//            }
        }

        private boolean match(int expected) {
            return Session.matchScanType(expected, this.scanType);
        }

        @Override
        public boolean hasNext() {
            this.matched = true;
            if (this.iter().hasNext()) {
                this.position = this.iter().key();
                if (!this.match(Session.SCAN_ANY)) {
                    this.matched = this.filter(this.position);
                }
            }
            if (!this.matched) {
                this.position = null;
                this.close();
            }
            return this.matched;
        }

        private boolean filter(byte[] key) {
            if (this.match(Session.SCAN_PREFIX_BEGIN)) {
                /*
                 * Prefix with `keyBegin`?
                 * TODO: use custom prefix_extractor instead
                 *       or use ReadOptions.prefix_same_as_start
                 */
                return Bytes.prefixWith(key, this.keyBegin);
            } else if (this.match(Session.SCAN_PREFIX_END)) {
                /*
                 * Prefix with `keyEnd`?
                 * like the following query for range index:
                 *  key > 'age:20' and prefix with 'age'
                 */
                assert this.keyEnd != null;
                return Bytes.prefixWith(key, this.keyEnd);
            } else if (this.match(Session.SCAN_LT_END)) {
                /*
                 * Less (equal) than `keyEnd`?
                 * NOTE: don't use BytewiseComparator due to signed byte
                 */
                assert this.keyEnd != null;
                if (this.match(Session.SCAN_LTE_END)) {
                    // Just compare the prefix, can be there are excess tail
                    key = Arrays.copyOfRange(key, 0, this.keyEnd.length);
                    return Bytes.compare(key, this.keyEnd) <= 0;
                } else {
                    return Bytes.compare(key, this.keyEnd) < 0;
                }
            } else {
                assert this.match(Session.SCAN_ANY) ||
                        this.match(Session.SCAN_GT_BEGIN) ||
                        this.match(Session.SCAN_GTE_BEGIN) :
                        "Unknow scan type";
                return true;
            }
        }

        @Override
        public BackendEntry.BackendColumn next() {
            if (!this.matched && !this.hasNext()) {
                throw new NoSuchElementException();
            }
            BackendEntry.BackendColumn col = BackendEntry.BackendColumn.of(this.iter.key(),
                    this.iter.value());
            this.iter.next();
            this.matched = false;
            return col;
        }

        @Override
        public long count() {
            long count = 0L;
            while (this.hasNext()) {
                this.next();
                count++;
                this.matched = false;
                BackendEntryIterator.checkInterrupted();
            }
            return count;
        }

        @Override
        public byte[] position() {
            return this.position;
        }

        @Override
        public void close() {
        }
    }
}
