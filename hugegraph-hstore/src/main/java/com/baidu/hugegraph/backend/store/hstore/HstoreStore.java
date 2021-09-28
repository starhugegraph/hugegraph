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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.AbstractBackendStore;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessions.Session;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class HstoreStore extends AbstractBackendStore<Session> {

    private static final Logger LOG = Log.logger(HstoreStore.class);

    private static final BackendFeatures FEATURES = new HstoreFeatures();

    private final String store;
    private final String namespace;

    private final BackendStoreProvider provider;
    private final Map<HugeType, HstoreTable> tables;

    private HstoreSessions sessions;

    public HstoreStore(final BackendStoreProvider provider,
                       final String namespace, final String store) {
        this.tables = new HashMap<>();

        this.provider = provider;
        this.namespace = namespace;
        this.store = store;
        this.sessions = null;

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            HstoreMetrics metrics = new HstoreMetrics(this.sessions);
            return metrics.metrics();
        });
    }

    protected void registerTableManager(HugeType type, HstoreTable table) {
        this.tables.put(type, table);
    }

    @Override
    protected final HstoreTable table(HugeType type) {
        assert type != null;
        HstoreTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected Session session(HugeType type) {
        this.checkOpened();
        return this.sessions.session();
    }

    protected List<String> tableNames() {
        return this.tables.values().stream().map(t -> t.table())
                                            .collect(Collectors.toList());
    }

    public String namespace() {
        return this.namespace;
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.namespace;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        E.checkNotNull(config, "config");

        if (this.sessions == null) {
            this.sessions = new HstoreStdSessions(config, this.namespace, this.store);
        }

        assert this.sessions != null;
        if (!this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        try {
            // NOTE: won't throw error even if connection refused
            this.sessions.open();
        } catch (Exception e) {
            if (!e.getMessage().contains("Column family not found")) {
                LOG.error("Failed to open HBase '{}'", this.store, e);
                throw new ConnectionException("Failed to connect to HBase", e);
            }
            if (this.isSchemaStore()) {
                LOG.info("Failed to open HBase '{}' with database '{}', " +
                         "try to init CF later", this.store, this.namespace);
            }
        }

        this.sessions.session();
        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        this.checkOpened();
        this.sessions.close();

        LOG.debug("Store closed: {}", this.store);
    }

    @Override
    public boolean opened() {
        this.checkConnectionOpened();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkOpened();
        Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(Session session, BackendAction item) {
        BackendEntry entry = item.entry();
        HstoreTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry);
                break;
            case DELETE:
                table.delete(session, entry);
                break;
            case APPEND:
                table.append(session, entry);
                break;
            case ELIMINATE:
                table.eliminate(session, entry);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();

        Session session = this.sessions.session();
        HstoreTable table = this.table(HstoreTable.tableType(query));
        return table.query(session, query);
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        Session session = this.sessions.session();
        HstoreTable table = this.table(HstoreTable.tableType(query));
        return table.queryNumber(session, query);
    }

    @Override
    public void init() {
        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public boolean initialized() {
        return true;
    }

    @Override
    public void truncate() {
        LOG.debug("Store truncated: {}", this.store);
    }

    private void enableTables() {
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        this.checkOpened();
        Session session = this.sessions.session();

        session.commit();
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();
        Session session = this.sessions.session();

        session.rollback();
    }

    private final void checkConnectionOpened() {
    }

    /***************************** Store defines *****************************/

    public static class TikvSchemaStore extends HstoreStore {

        private final HstoreTables.Counters counters;

        public TikvSchemaStore(BackendStoreProvider provider,
                               String namespace, String store) {
            super(provider, namespace, store);

            this.counters = new HstoreTables.Counters(namespace);

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new HstoreTables.VertexLabel(namespace));
            registerTableManager(HugeType.EDGE_LABEL,
                                 new HstoreTables.EdgeLabel(namespace));
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new HstoreTables.PropertyKey(namespace));
            registerTableManager(HugeType.INDEX_LABEL,
                                 new HstoreTables.IndexLabel(namespace));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HstoreTables.SecondaryIndex(store));
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.counters.table());
            return tableNames;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            super.checkOpened();
            this.counters.increaseCounter(super.sessions.session(),
                                          type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            super.checkOpened();
            return this.counters.getCounter(super.sessions.session(), type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class TikvGraphStore extends HstoreStore {

        public TikvGraphStore(BackendStoreProvider provider,
                              String namespace, String store) {
            super(provider, namespace, store);

            registerTableManager(HugeType.VERTEX,
                                 new HstoreTables.Vertex(store));

            registerTableManager(HugeType.EDGE_OUT,
                                 HstoreTables.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                                 HstoreTables.Edge.in(store));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HstoreTables.SecondaryIndex(store));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX,
                                 new HstoreTables.VertexLabelIndex(store));
            registerTableManager(HugeType.EDGE_LABEL_INDEX,
                                 new HstoreTables.EdgeLabelIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new HstoreTables.RangeIntIndex(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new HstoreTables.RangeFloatIndex(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new HstoreTables.RangeLongIndex(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new HstoreTables.RangeDoubleIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new HstoreTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new HstoreTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new HstoreTables.UniqueIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "TikvGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "TikvGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "TikvGraphStore.getCounter()");
        }
    }
}