package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.ExecutorUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import static com.baidu.hugegraph.type.HugeType.VERTEX;
import static com.baidu.hugegraph.type.HugeType.EDGE;

public class VirtualGraphBatcher {

    private static final int BATCH_BUFFER_MAX_SIZE = 1000;
    private static final int BATCH_SIZE = 50;
    private static final int BATCH_TIME_MS = 100;

    private final HugeGraphParams graphParams;
    private final VirtualGraph vGraph;
    private final ThreadLocal<GraphTransaction> transaction;
    private final java.util.Timer batchTimer;
    private final ExecutorService batchExecutor;

    private final LinkedBlockingQueue<VirtualGraphQueryTask> batchQueue;

    public VirtualGraphBatcher(HugeGraphParams graphParams, VirtualGraph vGraph) {
        assert graphParams != null;
        assert vGraph != null;

        this.graphParams = graphParams;
        this.vGraph = vGraph;
        this.transaction = ThreadLocal.withInitial(() -> null);
        this.batchTimer = new Timer();
        this.batchQueue = new LinkedBlockingQueue<>(BATCH_BUFFER_MAX_SIZE);
        this.batchExecutor = ExecutorUtil.newFixedThreadPool(
                1, "virtual-graph-batch-worker-" + this.graphParams.graph().name());
        this.start();
    }

    public void add(VirtualGraphQueryTask task) {
        assert task != null;
        this.batchQueue.add(task);

        if (this.batchQueue.size() >= BATCH_SIZE) {
            List<VirtualGraphQueryTask> taskList = new ArrayList<>(BATCH_SIZE);
            this.batchQueue.drainTo(taskList);
            if (taskList.size() > 0) {
                this.batchExecutor.submit(() ->this.batchProcess(taskList));
            }
        }
    }

    public void start() {
        this.batchTimer.scheduleAtFixedRate(new IntervalGetTask(), BATCH_TIME_MS, BATCH_TIME_MS);
    }

    public void close() {
        this.batchTimer.cancel();
    }

    private GraphTransaction getOrNewTransaction() {
        GraphTransaction transaction = this.transaction.get();
        if (transaction == null) {
            transaction = new GraphTransaction(this.graphParams, this.graphParams.loadGraphStore());
            this.transaction.set(transaction);
        }
        return transaction;
    }

    private void batchProcess(List<VirtualGraphQueryTask> tasks) {

        try {
            Map<Id, VirtualVertex> vertexMap = new HashMap<>();
            Map<Id, VirtualEdge> edgeMap = new HashMap<>();

            for (VirtualGraphQueryTask task : tasks) {
                switch (task.getHugeType()) {
                    case VERTEX:
                        task.getIds().forEach(id -> vertexMap.put((Id) id, null));
                        break;
                    case EDGE:
                        task.getIds().forEach(id -> edgeMap.put((Id) id, null));
                        break;
                    default:
                        throw new AssertionError(String.format(
                                "Invalid huge type: '%s'", task.getHugeType()));
                }
            }

            GraphTransaction tran = getOrNewTransaction();
            queryFromBackend(tran, vertexMap, edgeMap);

            for (VirtualGraphQueryTask task : tasks) {
                switch (task.getHugeType()) {
                    case VERTEX:
                        MapperIterator<Id, VirtualVertex> vertexIterator = new MapperIterator<Id, VirtualVertex>(
                                task.getIds().iterator(), id -> vertexMap.get(id));
                        task.getFuture().complete(vertexIterator);
                        break;
                    case EDGE:
                        MapperIterator<Id, VirtualEdge> edgeIterator = new MapperIterator<Id, VirtualEdge>(
                                task.getIds().iterator(), id -> edgeMap.get(id));
                        task.getFuture().complete(edgeIterator);
                        break;
                    default:
                        throw new AssertionError(String.format(
                                "Invalid huge type: '%s'", task.getHugeType()));
                }
            }
        } catch (Exception ex) {
            for (VirtualGraphQueryTask task : tasks) {
                task.getFuture().completeExceptionally(ex);
            }
            throw ex;
        }
    }

    private void queryFromBackend(GraphTransaction tran, Map<Id, VirtualVertex> vertexMap,
                                  Map<Id, VirtualEdge> edgeMap) {

        synchronized (tran) {
            if (vertexMap.size() > 0) {
                IdQuery query = new IdQuery(VERTEX, vertexMap.keySet());
                Iterator<Vertex> vertexIterator = tran.queryVertices(query);
                vertexIterator.forEachRemaining(vertex ->
                        vertexMap.put((Id) vertex.id(), this.vGraph.putVertex((HugeVertex) vertex, null)));
            }

            if (edgeMap.size() > 0) {
                IdQuery query = new IdQuery(EDGE, edgeMap.keySet());
                Iterator<Edge> edgeIterator = tran.queryEdges(query);
                edgeIterator.forEachRemaining(edge ->
                        edgeMap.put((Id) edge.id(), this.vGraph.putEdge((HugeEdge) edge)));
            }
        }
    }

    class IntervalGetTask extends TimerTask {

        @Override
        public void run() {
            while (batchQueue.size() > 0) {
                List<VirtualGraphQueryTask> taskList = new ArrayList<>(BATCH_SIZE);
                batchQueue.drainTo(taskList, BATCH_SIZE);
                if (taskList.size() > 0) {
                    batchProcess(taskList);
                }
            }
        }
    }
}
