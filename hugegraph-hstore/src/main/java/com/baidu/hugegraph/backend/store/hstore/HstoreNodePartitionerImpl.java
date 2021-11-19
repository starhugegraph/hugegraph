package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.client.*;
import com.baidu.hugegraph.store.client.type.HgNodeStatus;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.pd.common.HgPair;

import java.util.List;

public class HstoreNodePartitionerImpl implements HgStoreNodePartitioner, HgStoreNodeProvider, HgStoreNodeNotifier {

    private PDClient pdClient;
    private HgStoreNodeManager nodeManager;

    public HstoreNodePartitionerImpl(HgStoreNodeManager nodeManager, String pdPeers) {
        this.nodeManager = nodeManager;
        pdClient = PDClient.create(PDConfig.of(pdPeers));
    }

    /**
     * 查询分区信息，结果通过HgNodePartitionerBuilder返回
     */
    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName, byte[] startKey, byte[] endKey) {
        try {
            if (HgStoreClientConst.ALL_PARTITION_OWNER == startKey) {
                List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
                stores.forEach(e -> {
                    builder.add(e.getId(), -1);
                });
            } else if (startKey == endKey) {
                HgPair<Metapb.Partition, Metapb.Shard> partShard = pdClient.getPartition(graphName, startKey);
                Metapb.Shard leader = partShard.getValue();
                builder.add(leader.getStoreId(), partShard.getKey().getId());
            } else {
                pdClient.scanPartitions(graphName, startKey, endKey).forEach(e -> {
                    builder.add(e.getValue().getStoreId(), e.getKey().getId());
                });
            }
        } catch (PDException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return 0;
    }

    /**
     * 查询hgstore信息
     * @return hgstore
     */
    @Override
    public HgStoreNode apply(String graphName, Long nodeId) {
        try {
            Metapb.Store store = pdClient.getStore(nodeId);
            return nodeManager.getNodeBuilder().setNodeId(store.getId())
                    .setAddress(store.getAddress()).build();
        } catch (PDException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 通知更新缓存
     */
    @Override
    public int notice(String graphName, HgStoreNotice storeNotice) {
        storeNotice.getPartitionLeaders().forEach((partId, leader) -> {
            pdClient.updatePartitionLeader(graphName, partId, leader);
        });
        return 0;
    }
}




class FakeHstoreNodePartitionerImpl extends HstoreNodePartitionerImpl{
    private String pdPeers;
    HgStoreNodeManager nodeManager;
    static Long DefaultStoreID = 1L;
    static int DefaultPartitionId = 1;

    public FakeHstoreNodePartitionerImpl(HgStoreNodeManager nodeManager, String pdPeers) {
        super(nodeManager, pdPeers);
        this.pdPeers = pdPeers;
        this.nodeManager = nodeManager;
    }


    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName, byte[] startKey, byte[] endKey) {
        builder.add(DefaultStoreID,DefaultPartitionId);
        return 0;
    }

    @Override
    public HgStoreNode apply(String graphName, Long nodeId) {
        return nodeManager.getNodeBuilder().setNodeId(DefaultStoreID)
                .setAddress(pdPeers).build();
    }

    @Override
    public int notice(String graphName, HgStoreNotice storeNotice) {
        return 0;
    }
}