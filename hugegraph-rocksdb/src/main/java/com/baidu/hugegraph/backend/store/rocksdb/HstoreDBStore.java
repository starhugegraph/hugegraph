package com.baidu.hugegraph.backend.store.rocksdb;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdPrefixQuery;
import com.baidu.hugegraph.backend.query.IdRangeQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.*;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableMap;

import javax.naming.ldap.HasControls;
import java.util.*;

public class HstoreDBStore extends AbstractBackendStore<RocksDBSessions.Session> {
    private final String store;
    private final String database;
    private final BackendStoreProvider provider;
    private static final BackendFeatures FEATURES = new RocksDBFeatures();

    public String NODES = "nodes";
    public String CLUSTER_ID = "cluster_id";
    public String SERVERS = "servers";
    public String SERVER_LOCAL = "local";
    public String SERVER_CLUSTER = "cluster";

    public HstoreDBStore(final BackendStoreProvider provider,
                        final String database, final String store){
        this.provider = provider;
        this.database = database;
        this.store = store;
        this.registerMetaHandlers();

    }

    private void registerMetaHandlers(){
        this.registerMetaHandler("flush", (session, meta, args) -> {
            Map<String, Object> results = InsertionOrderUtil.newMap();
            results.put(NODES, 1);
            results.put(CLUSTER_ID, SERVER_LOCAL);
            results.put(SERVERS, ImmutableMap.of(SERVER_LOCAL, "OK"));
            return results;
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

    }

    @Override
    public void close() {

    }

    @Override
    public boolean opened() {
        return false;
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
        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
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
            default:
                throw new AssertionError(String.format(
                        "Unsupported mutate action: %s", item.action()));
        }
    }

    private Map<String, Map<Id, BackendEntry>> tables = new HashMap<>();

    protected  void insert(BackendEntry entry){
        String tableName = entry.type().string();
        if ( !tables.containsKey( tableName) )
            tables.put(tableName, new HashMap<>());
        Map<Id, BackendEntry> table = tables.get(tableName);
        table.put(entry.id(), entry);

        for (BackendEntry.BackendColumn col : entry.columns()) {
            System.out.println(tableName + ", " + entry.id() + ", "
                    + StringEncoding.decode(col.name) + ", " + StringEncoding.decode(col.value)) ;
        }
    }
    @Override
    public Iterator<BackendEntry> query(Query query) {
        String tableName = query.resultType().string();
        if (query.empty()) {
            return tables.get(tableName).values().iterator();
        }

        // Query by prefix
        if (query instanceof IdPrefixQuery) {
            IdPrefixQuery pq = (IdPrefixQuery) query;
           // return this.queryByPrefix(pq);
        }

        // Query by range
        if (query instanceof IdRangeQuery) {
            IdRangeQuery rq = (IdRangeQuery) query;
         //   return this.queryByRange(rq);
        }

        // Query by id
        if (query.conditions().isEmpty()) {
            assert !query.ids().isEmpty();
            return queryById(query.ids(), tables.get(tableName));
        }

        // Query by condition (or condition + id)
        ConditionQuery cq = (ConditionQuery) query;
      //  return this.queryByCond(session, cq);
        return new ArrayList<BackendEntry>().iterator();
    }

    protected Iterator<BackendEntry> queryById(Set<Id> ids, Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = InsertionOrderUtil.newMap();

        for (Id id : ids) {
            assert !id.number();
            if (entries.containsKey(id)) {
                rs.put(id, entries.get(id));
            }
        }
        return rs.values().iterator();
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
    public void increaseCounter(HugeType type, long increment) {

    }

    @Override
    public long getCounter(HugeType type) {
        return 0;
    }
}
