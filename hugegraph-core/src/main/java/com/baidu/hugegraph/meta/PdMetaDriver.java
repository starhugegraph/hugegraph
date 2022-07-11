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

package com.baidu.hugegraph.meta;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.meta.lock.LockResult;
import com.baidu.hugegraph.meta.lock.PdDistributedLock;
import com.baidu.hugegraph.pd.client.KvClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.kv.KResponse;
import com.baidu.hugegraph.pd.grpc.kv.LockResponse;
import com.baidu.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import com.baidu.hugegraph.pd.grpc.kv.TTLResponse;
import com.baidu.hugegraph.pd.grpc.kv.WatchEvent;
import com.baidu.hugegraph.pd.grpc.kv.WatchResponse;
import com.baidu.hugegraph.pd.grpc.kv.WatchType;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class PdMetaDriver implements MetaDriver {

    KvClient<WatchResponse> client = null;
    private PdDistributedLock lock;

    public PdMetaDriver(String pdPeer) {
        PDConfig pdConfig = PDConfig.of(pdPeer);
        this.client = new KvClient<>(pdConfig);
        lock = new PdDistributedLock(this.client);
    }

    @Override
    public void put(String key, String value) {
        try {
            this.client.put(key, value);
        } catch (PDException e) {
            throw new HugeException("Failed to put '%s:%s' to pd", e, key, value);
        }
    }

    @Override
    public String get(String key) {
        try {
            KResponse response = this.client.get(key);
            return response.getValue();
        } catch (PDException e) {
            throw new HugeException("Failed to get '%s' from pd", e, key);
        }
    }

    @Override
    public void delete(String key) {
        try {
            this.client.delete(key);
        } catch (PDException e) {
            throw new HugeException("Failed to delete '%s' to pd", e, key);
        }
    }

    @Override
    public void deleteWithPrefix(String prefix) {
        try {
            this.client.deletePrefix(prefix);
        } catch (PDException e) {
            throw new HugeException("Failed to delete '%s' to pd", e, prefix);
        }
    }

    @Override
    public Map<String, String> scanWithPrefix(String prefix) {
        try {
            ScanPrefixResponse response = this.client.scanPrefix(prefix);
            return response.getKvsMap();
        } catch (PDException e) {
            throw new HugeException("Failed to scanWithPrefix '%s' from pd", e, prefix);
        }
    }

    @Override
    public <T> void listen(String key, Consumer<T> consumer) {
        try {
            this.client.listen(key, (Consumer<WatchResponse>) consumer);
        } catch (PDException e) {
            throw new HugeException("Failed to listen '%s' to pd", e, key);
        }
    }

    @Override
    public <T> void listenPrefix(String prefix, Consumer<T> consumer) {
        try {
            this.client.listenPrefix(prefix, (Consumer<WatchResponse>) consumer);
        } catch (PDException e) {
            throw new HugeException("Failed to listen '%s' to pd", e, prefix);
        }
    }

    @Override
    public <T> List<String> extractValuesFromResponse(T response) {
        List<String> values = new ArrayList<>();
        WatchResponse res = (WatchResponse) response;
        for (WatchEvent event : res.getEventsList()) {
            if (!event.getType().equals(WatchType.Put)) {
                return null;
            }
            String value = event.getCurrent().getValue();
            values.add(value);
        }
        return values;
    }

    @Override
    public <T> Map<String, String> extractKVFromResponse(T response) {
        Map<String, String> resultMap = new HashMap<>();
        WatchResponse res = (WatchResponse) response;
        for (WatchEvent event : res.getEventsList()) {
            // Skip if not etcd PUT event
            if (!event.getType().equals(WatchType.Put)) {
                continue;
            }

            String key = event.getCurrent().getKey();
            String value = event.getCurrent().getValue();
            if (Strings.isNullOrEmpty(key)) {
                continue;
            }
            resultMap.put(key, value);
        }
        return resultMap;
    }

    @Override
    public LockResult lock(String key, long ttl) {
        return this.lock.lock(key, ttl);
    }

    @Override
    public void unlock(String key, LockResult lockResult) {
        this.lock.unLock(key, lockResult);
    }

    @Override
    public long keepAlive(String key, long lease) {
        try {
            LockResponse lockResponse = this.client.keepAlive(key);
            boolean succeed = lockResponse.getSucceed();
            if (!succeed) {
                throw new HugeException("Failed to keepAlive '%s' to pd", key);
            }
            return lockResponse.getClientId();
        } catch (PDException e) {
            throw new HugeException("Failed to keepAlive '%s' to pd", e, key);
        }
    }

    public boolean keepTTLAlive(String key) {
        try {
            TTLResponse response = this.client.keepTTLAlive(key);
            return response.getSucceed();
        } catch (PDException e) {
            throw new HugeException("Failed to keepTTLAlive '%s' to pd", e, key);
        }
    }

    public boolean putTTL(String key, String value, long ttl) {
        try {
            TTLResponse response = this.client.putTTL(key, value, ttl);
            return response.getSucceed();
        } catch (PDException e) {
            throw new HugeException("Failed to keepTTLAlive '%s' to pd", e, key);
        }
    }
}
