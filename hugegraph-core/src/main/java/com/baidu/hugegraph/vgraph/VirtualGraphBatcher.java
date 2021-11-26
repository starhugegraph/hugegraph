package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
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

    private final int batchBufferSize;
    private final int batchSize;
    private final int batchTimeMS;

    private final HugeGraphParams graphParams;
    private final VirtualGraph vGraph;
    private final java.util.Timer batchTimer;
    private final ExecutorService batchExecutor;

    private final LinkedBlockingQueue<VirtualGraphQueryTask> batchQueue;

    public VirtualGraphBatcher(HugeGraphParams graphParams, VirtualGraph vGraph) {
        assert graphParams != null;
        assert vGraph != null;

        this.graphParams = graphParams;
        this.vGraph = vGraph;
        this.batchBufferSize = this.graphParams.configuration().get(CoreOptions.VIRTUAL_GRAPH_BATCH_BUFFER_SIZE);
        this.batchSize = this.graphParams.configuration().get(CoreOptions.VIRTUAL_GRAPH_BATCH_SIZE);
        this.batchTimeMS = this.graphParams.configuration().get(CoreOptions.VIRTUAL_GRAPH_BATCH_TIME_MS);
        this.batchTimer = new Timer();
        this.batchQueue = new LinkedBlockingQueue<>(batchBufferSize);
        this.batchExecutor = ExecutorUtil.newFixedThreadPool(
                1, "virtual-graph-batch-worker-" + this.graphParams.graph().name());
        this.start();
    }

    public void add(VirtualGraphQueryTask task) {
        assert task != null;
        this.batchQueue.add(task);

        if (this.batchQueue.size() >= batchSize) {
            List<VirtualGraphQueryTask> taskList = new ArrayList<>(batchSize);
            this.batchQueue.drainTo(taskList);
            if (taskList.size() > 0) {
                this.batchExecutor.submit(() ->this.batchProcess(taskList));
            }
        }
    }

    public void start() {
        this.batchTimer.scheduleAtFixedRate(new IntervalGetTask(), batchTimeMS, batchTimeMS);
    }

    public void close() {
        this.batchTimer.cancel();
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

            queryFromBackend(vertexMap, edgeMap);

            for (VirtualGraphQueryTask task : tasks) {
                switch (task.getHugeType()) {
                    case VERTEX:
                        MapperIterator<Id, VirtualVertex> vertexIterator = new MapperIterator<Id, VirtualVertex>(
                                task.getIds().iterator(), vertexMap::get);
                        task.getFuture().complete(vertexIterator);
                        break;
                    case EDGE:
                        MapperIterator<Id, VirtualEdge> edgeIterator = new MapperIterator<Id, VirtualEdge>(
                                task.getIds().iterator(), edgeMap::get);
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

    private void queryFromBackend(Map<Id, VirtualVertex> vertexMap,
                                  Map<Id, VirtualEdge> edgeMap) {
        try( GraphTransaction tran = new GraphTransaction(this.graphParams,
                this.graphParams.loadGraphStore())) {
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
                List<VirtualGraphQueryTask> taskList = new ArrayList<>(batchSize);
                batchQueue.drainTo(taskList, batchSize);
                if (taskList.size() > 0) {
                    batchProcess(taskList);
                }
            }
        }
    }
}
