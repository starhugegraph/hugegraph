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

package com.baidu.hugegraph.backend.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.space.GraphSpace;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.memory.InMemoryDBStoreProvider;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class BackendProviderFactory {

    private static final Logger LOG = Log.logger(BackendProviderFactory.class);

    private static Map<String, Class<? extends BackendStoreProvider>> providers;

    static {
        providers = new ConcurrentHashMap<>();
    }

    public static BackendStoreProvider open(HugeGraphParams params) {
        HugeConfig config = params.configuration();
        String backend = config.get(CoreOptions.BACKEND).toLowerCase();
        String graph = (StringUtils.isEmpty(params.graph().graphSpace()) ?
                        GraphSpace.DEFAULT_GRAPH_SPACE_NAME : params.graph().graphSpace())
                       + "/" + config.get(CoreOptions.STORE);

        BackendStoreProvider provider = newProvider(config);
        provider.open(graph);
        return provider;
    }

    private static BackendStoreProvider newProvider(HugeConfig config) {
        String backend = config.get(CoreOptions.BACKEND).toLowerCase();
        String graph = config.get(CoreOptions.STORE); // no graph space support

        if (InMemoryDBStoreProvider.matchType(backend)) {
            return InMemoryDBStoreProvider.instance(graph);
        }

        Class<? extends BackendStoreProvider> clazz = providers.get(backend);
        BackendException.check(clazz != null,
                               "Not exists BackendStoreProvider: %s", backend);

        assert BackendStoreProvider.class.isAssignableFrom(clazz);
        BackendStoreProvider instance = null;
        try {
            instance = clazz.newInstance();
        } catch (Exception e) {
            throw new BackendException(e);
        }

        BackendException.check(backend.equals(instance.type()),
                               "BackendStoreProvider with type '%s' " +
                               "can't be opened by key '%s'",
                               instance.type(), backend);
        return instance;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void register(String name, String classPath) {
        ClassLoader classLoader = BackendProviderFactory.class.getClassLoader();
        Class<?> clazz = null;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new BackendException(e);
        }

        // Check subclass
        boolean subclass = BackendStoreProvider.class.isAssignableFrom(clazz);
        BackendException.check(subclass, "Class '%s' is not a subclass of " +
                               "class BackendStoreProvider", classPath);

        // Check exists
        BackendException.check(!providers.containsKey(name),
                               "Exists BackendStoreProvider: %s (%s)",
                               name, providers.get(name));

        // Register class
        providers.put(name, (Class) clazz);
    }
}
