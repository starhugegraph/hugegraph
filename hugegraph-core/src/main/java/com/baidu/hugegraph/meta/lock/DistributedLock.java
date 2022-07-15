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

package com.baidu.hugegraph.meta.lock;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.KV;

import com.baidu.hugegraph.util.Log;

public class DistributedLock {

    protected static final Logger LOG = Log.logger(DistributedLock.class);

    private static DistributedLock lockProvider = null;
    private static final long UNLIMIT_TIMEOUT = -1L;
    private final static Object mutex = new Object();

    private final KV kvClient;
    private final Lock lockClient;
    private final Lease leaseClient;

    private DistributedLock(Client client) {
        this.kvClient = client.getKVClient();
        this.lockClient = client.getLockClient();
        this.leaseClient = client.getLeaseClient();
    }

    public static DistributedLock getInstance(Client client) {
        synchronized (mutex) {
            if (null == lockProvider) {
                lockProvider = new DistributedLock(client);
            }
        }
        return lockProvider;
    }

    public LockResult tryLock(String lockName, long ttl, long timeout) {
        LockResult lockResult = new LockResult();
        ScheduledExecutorService service =
                Executors.newSingleThreadScheduledExecutor();

        lockResult.lockSuccess(false);
        lockResult.setService(service);

        Long leaseId;

        try {
            leaseId = this.leaseClient.grant(ttl).get().getID();
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Thread {} failed to create lease for {} with ttl {}", e,
                     Thread.currentThread().getName(), lockName, ttl);
            return lockResult;
        }

        lockResult.setLeaseId(leaseId);

        long period = ttl - ttl / 5;
        service.scheduleAtFixedRate(new KeepAliveTask(this.leaseClient, leaseId),
                                    period, period, TimeUnit.SECONDS);

        try {
            if (timeout == UNLIMIT_TIMEOUT) {
                this.lockClient.lock(toByteSequence(lockName), leaseId).get();

            } else {
                this.lockClient.lock(toByteSequence(lockName), leaseId)
                               .get(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Thread {} failed to lock {}", e,
                     Thread.currentThread().getName(), lockName);
            service.shutdown();
            return lockResult;
        } catch (TimeoutException e) {
            // 获取锁超时
            LOG.warn("Thread {} timeout to lock {}", e,
                     Thread.currentThread().getName(), lockName);
            service.shutdown();
            return lockResult;
        }

        lockResult.lockSuccess(true);

        return lockResult;
    }

    public LockResult lock(String lockName, long ttl) {

        return tryLock(lockName, ttl, UNLIMIT_TIMEOUT);
    }

    public void unLock(String lockName, LockResult lockResult) {
        LOG.debug("Thread {} start to unlock {}",
                  Thread.currentThread().getName(), lockName);

        lockResult.getService().shutdown();

        if (lockResult.getLeaseId() != 0L) {
            try {
                this.leaseClient.revoke(lockResult.getLeaseId()).get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("Thread {} failed to unlock {}", e,
                         Thread.currentThread().getName(), lockName);
            }
        }

        LOG.debug("Thread {} unlock {} successfully",
                  Thread.currentThread().getName(), lockName);
    }

    private static ByteSequence toByteSequence(String content) {
        return ByteSequence.from(content, Charset.defaultCharset());
    }

    public static class KeepAliveTask implements Runnable {

        private Lease leaseClient;
        private long leaseId;

        KeepAliveTask(Lease leaseClient, long leaseId) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        @Override
        public void run() {
            this.leaseClient.keepAliveOnce(this.leaseId);
        }
    }
}
