package com.baidu.hugegraph.meta.lock;

import com.baidu.hugegraph.pd.client.KvClient;
import io.etcd.jetcd.Client;

/**
 * @author zhangyingjie
 * @date 2022/6/18
 **/
public abstract class AbstractDistributedLock {

    private static volatile AbstractDistributedLock DEFAULT_LOCK;

    public abstract LockResult lock(String key, long ttl);

    public abstract void unLock(String key, LockResult lockResult);

    public static AbstractDistributedLock getInstance(AutoCloseable client) {
        if (DEFAULT_LOCK==null){
            synchronized (AbstractDistributedLock.class){
                if (DEFAULT_LOCK == null) {
                    if (client instanceof Client){
                        DEFAULT_LOCK = DistributedLock.getInstance((Client)client);
                    } else {
                        DEFAULT_LOCK = new PdDistributedLock((KvClient) client);
                    }

                }
            }
        }
        return DEFAULT_LOCK;
    }
}
