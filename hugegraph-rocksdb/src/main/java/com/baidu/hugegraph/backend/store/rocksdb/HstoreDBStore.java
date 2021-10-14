package com.baidu.hugegraph.backend.store.rocksdb;


import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;

import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdPrefixQuery;
import com.baidu.hugegraph.backend.query.IdRangeQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinaryEntryIterator;
import com.baidu.hugegraph.backend.store.*;
import com.baidu.hugegraph.config.HugeConfig;

import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.store.HgKvEntry;

import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.InsertionOrderUtil;

import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class HstoreDBStore extends AbstractBackendStore<RocksDBSessions.Session> {
    private final String store;
    private final String database;
    private final BackendStoreProvider provider;
    private static final BackendFeatures FEATURES = new HstoreFeatures();
    private HugeConfig config;
    public String NODES = "nodes";
    public String CLUSTER_ID = "cluster_id";
    public String SERVERS = "servers";
    public String SERVER_LOCAL = "local";
    public String SERVER_CLUSTER = "cluster";

    HstoreClient hstoreClient;

    public HstoreDBStore(final BackendStoreProvider provider,
                         final String database, final String store) {
        this.provider = provider;
        this.database = database;
        this.store = store;
        this.registerMetaHandlers();

    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("flush", (session, meta, args) -> {
            Map<String, Object> results = InsertionOrderUtil.newMap();
            results.put(NODES, 1);
            results.put(CLUSTER_ID, SERVER_LOCAL);
            results.put(SERVERS, ImmutableMap.of(SERVER_LOCAL, "OK"));
            return results;
        });

        this.registerMetaHandler("metrics", (session, meta, args) -> {
            return new BackendMetrics(){
                @Override
                public Map<String, Object> metrics() {
                    Map<String, Object> results = InsertionOrderUtil.newMap();
                    results.put(NODES, 1);
                    results.put(CLUSTER_ID, SERVER_LOCAL);
                    try {
                        Map<String, Object> metrics = new HashMap<>();

                        results.put(SERVERS, ImmutableMap.of(SERVER_LOCAL, metrics));
                    } catch (Throwable e) {
                        results.put(EXCEPTION, e.toString());
                    }
                    return results;
                }
            }.metrics();
        });
    }

    @Override
    protected BackendTable<RocksDBSessions.Session, ?> table(HugeType type) {
        return null;
    }

    @Override
    protected RocksDBSessions.Session session(HugeType type) {
        return null;
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public boolean isSchemaStore() {
        return false;
    }

    @Override
    public void open(HugeConfig config) {
        this.config = config;
        hstoreClient = HstoreClient.create(config, this.database + "/" + this.store);
    }

    @Override
    public void close() {


    }

    @Override
    public boolean opened() {
        return hstoreClient != null;
    }

    @Override
    public void init() {

    }

    @Override
    public void clear(boolean clearSpace) {

    }

    @Override
    public boolean initialized() {
        return true;
    }

    @Override
    public void truncate() {

    }

    @Override
    public void mutate(BackendMutation mutation) {
        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext(); ) {
            this.mutate(it.next());
        }
    }

    protected synchronized void mutate(BackendAction item) {
        BackendEntry entry = item.entry();
        switch (item.action()) {
            case INSERT:
            case APPEND:
                insert(entry);
                break;
            case DELETE:
            case ELIMINATE:
                break;
            default:
                throw new AssertionError(String.format(
                        "Unsupported mutate action: %s", item.action()));
        }
    }

    protected void insert(BackendEntry entry) {
        // 基于表名生成分区规则
        String tableName = entry.type().string();
        byte[] ownerId = getOwnerId(entry);
        for (BackendEntry.BackendColumn col : entry.columns()) {
            hstoreClient.put(tableName, ownerId, col.name, col.value);
            System.out.println(this.toString() + "/" + tableName + " ownId= " + new String(ownerId)
                    + " name= " + new String(col.name) + "/0x" + Bytes.toHex(col.name)
                    + " value = " + new String(col.value) + "/0x" + Bytes.toHex(col.value));
        }
    }

    /**
     * 返回Id所属的点ID
     * @param entry
     * @return
     */
    protected byte[] getOwnerId(BackendEntry entry) {
        Id id = null;
        HugeType type = entry.type();
        if (type.isVertex() || type.isEdge())
            id = entry.originId();
        else if (type.isIndex()) {
            id = entry.subId();
        } else {
            id = entry.originId();
        }
        return getOwnerId(id);
    }

    /**
     * 返回Id所属的点ID
     * @param id
     * @return
     */
    protected byte[] getOwnerId(Id id) {
        if ( id instanceof BinaryBackendEntry.BinaryId){
            id = ((BinaryBackendEntry.BinaryId)id).origin();
        }
        if (id.edge()) {
            id = ((EdgeId) id).ownerVertexId();
        }
        return id != null ? id.asBytes() : new byte[]{0};
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        System.out.println(this.toString() + "--" + query);

        if (query.empty()) {
            return queryAll(query);
        }

        // Query by prefix
        if (query instanceof IdPrefixQuery) {
            IdPrefixQuery pq = (IdPrefixQuery) query;
            assert true;
            return queryByPrefix(pq);
        }

        // Query by range
        if (query instanceof IdRangeQuery) {
            IdRangeQuery rq = (IdRangeQuery) query;
            assert true;
            return this.queryByRange(rq);
        }

        // Query by id
        if (query.conditions().isEmpty()) {
            assert !query.ids().isEmpty();
            assert true;

            return queryById(query);
        }

        assert true;
        // Query by condition (or condition + id)
        ConditionQuery cq = (ConditionQuery) query;
        //  return this.queryByCond(session, cq);
        return new ArrayList<BackendEntry>().iterator();
    }

    protected String getQueryTableName(Query query) {
        return BackendTable.tableType(query).string();
    }

    protected Iterator<BackendEntry> queryAll(Query query) {
        String tableName = getQueryTableName(query);
        List<HgKvEntry> entries = hstoreClient.scanAll(tableName);
        if (!query.noLimit() && query.limit() < entries.size())
            entries = entries.subList(0, (int) query.limit());
        return newEntryIterator(new ColumnIterator(tableName, entries.iterator()), query);
    }

    protected Iterator<BackendEntry> queryById(Query query) {
        String tableName = getQueryTableName(query);
        return newEntryIterator(
                new BackendEntry.BackendColumnIteratorWrapper(
                        new FlatMapperIterator<>(
                                query.ids().iterator(), id -> this.queryById(tableName, id))), query
        );
    }
    protected BackendEntry.BackendColumnIterator queryById(String tableName, Id id) {
        byte[] ownerId = getOwnerId(id);
        List<HgKvEntry> entries = hstoreClient.scanPrefix(tableName, ownerId, id.asBytes());
        if (entries == null)
            entries = new ArrayList<>();

        return new ColumnIterator(tableName, entries.iterator());
    }

    protected Iterator<BackendEntry> queryByPrefix(IdPrefixQuery query) {
        String tableName = getQueryTableName(query);
        byte[] ownerId = getOwnerId(query.prefix());
        List<HgKvEntry> entries = hstoreClient.scanPrefix(tableName, ownerId, query.prefix().asBytes());
        if (entries == null)
            entries = new ArrayList<>();

        return newEntryIterator(new ColumnIterator(tableName, entries.iterator()), query);
    }

    protected Iterator<BackendEntry> queryByRange(IdRangeQuery query){
        throw new UnsupportedOperationException();

    }

    protected static final BackendEntryIterator newEntryIterator(
            BackendEntry.BackendColumnIterator cols,
            Query query) {
        return new BinaryEntryIterator<>(cols, query, (entry, col) -> {
            if (entry == null || !entry.belongToMe(col)) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, col.name);
            }
            entry.columns(col);
            return entry;
        });
    }

    private static class ColumnIterator implements BackendEntry.BackendColumnIterator {
        private final String table;
        private final Iterator<HgKvEntry> iter;

        public ColumnIterator(String table, Iterator<HgKvEntry> results) {
            this.table = table;
            this.iter = results;
        }

        @Override
        public void close() {

        }

        @Override
        public byte[] position() {
            return new byte[0];
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public BackendEntry.BackendColumn next() {
            HgKvEntry entry = this.iter.next();

            BackendEntry.BackendColumn col = BackendEntry.BackendColumn.of(entry.key(),
                    entry.value() == null ? new byte[0] : entry.value());
            return col;
        }
    }

    private class HgKvEntryWrap implements HgKvEntry {
        private byte[] key;
        private byte[] value;

        HgKvEntryWrap(byte[] k, byte[] v) {
            this.key = k;
            this.value = v;
        }

        @Override
        public byte[] key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HgKvEntryWrap hgKvEntry = (HgKvEntryWrap) o;
            return Arrays.equals(key, hgKvEntry.key) && Arrays.equals(value, hgKvEntry.value);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(key);
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }

        @Override
        public String toString() {
            return "HgKvEntryImpl{" +
                    "key=" + Arrays.toString(key) +
                    ", value=" + Arrays.toString(value) +
                    '}';
        }
    }

    @Override
    public Number queryNumber(Query query) {
        return null;
    }

    @Override
    public void beginTx() {

    }

    @Override
    public void commitTx() {

    }

    @Override
    public void rollbackTx() {

    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public Id nextId(HugeType type) {
        return super.nextId(type);
    }

    @Override
    public void increaseCounter(HugeType type, long num) {
        throw new UnsupportedOperationException(
                "RocksDBGraphStore.increaseCounter()");
    }

    @Override
    public long getCounter(HugeType type) {
        throw new UnsupportedOperationException(
                "RocksDBGraphStore.getCounter()");
    }

    public static class HstoreSchemaStore extends HstoreDBStore {
        private static String counterTbl = "c";

        public HstoreSchemaStore(BackendStoreProvider provider,
                                 String namespace, String store) {
            super(provider, namespace, store);
        }

        private byte[] b(long value) {
            return ByteBuffer.allocate(Long.BYTES)
                    .order(ByteOrder.nativeOrder())
                    .putLong(value).array();
        }

        private long l(byte[] bytes) {
            assert bytes.length == Long.BYTES;
            return ByteBuffer.wrap(bytes)
                    .order(ByteOrder.nativeOrder())
                    .getLong();
        }

        @Override
        public synchronized void increaseCounter(HugeType type, long increment) {

            long old = 0L;
            byte[] key = new byte[]{type.code()};
            byte[] value = this.hstoreClient.get(counterTbl, key);
            if (value != null && value.length != 0) {
                old = this.l(value);
            }
            this.hstoreClient.put(counterTbl, new byte[]{0}, key, this.b(old + increment));
        }

        @Override
        public long getCounter(HugeType type) {
            byte[] key = new byte[]{type.code()};
            byte[] value = this.hstoreClient.get(counterTbl, key);
            if (value != null && value.length > 0)
                return this.l(value);
            return 0L;
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }
}
