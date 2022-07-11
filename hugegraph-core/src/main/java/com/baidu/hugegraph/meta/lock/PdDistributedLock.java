package com.baidu.hugegraph.meta.lock;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.pd.client.KvClient;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.kv.LockResponse;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangyingjie
 * @date 2022/6/18
 **/
public class PdDistributedLock extends AbstractDistributedLock {

    private final KvClient client;
    ScheduledExecutorService service = Executors.newScheduledThreadPool(0);

    public PdDistributedLock(KvClient client) {
        this.client = client;
    }

    @Override
    public LockResult lock(String key, long ttl) {
        try {
            LockResponse response = this.client.lock(key, ttl);
            boolean succeed = response.getSucceed();
            LockResult result = new LockResult();
            if (succeed) {
                result.setLeaseId(response.getClientId());
                result.lockSuccess(true);
                long period = ttl - ttl / 4;
                ScheduledFuture<?> future = service.scheduleAtFixedRate(() -> {
                    keepAlive(key);
                }, 10, period, TimeUnit.MILLISECONDS);
                result.setFuture(future);
            }
            return result;
        } catch (PDException e) {
            throw new HugeException("Failed to lock '%s' to pd", e, key);
        }
    }

    @Override
    public void unLock(String key, LockResult lockResult) {
        try {
            LockResponse response = this.client.unlock(key);
            boolean succeed = response.getSucceed();
            if (succeed == false) {
                throw new HugeException("Failed to unlock '%s' to pd", key);
            }
            if (lockResult.getFuture() != null) lockResult.getFuture().cancel(true);
        } catch (PDException e) {
            throw new HugeException("Failed to unlock '%s' to pd", e, key);
        }
    }

    //@Override
    public boolean keepAlive(String key) {
        try {
            LockResponse alive = this.client.keepAlive(key);
            return alive.getSucceed();
        } catch (PDException e) {
            throw new HugeException("Failed to keepAlive '%s' to pd", key);
        }
    }


}
