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

package com.baidu.hugegraph.traversal.algorithm;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConcurrentMultiValuedMap<K, V>
        extends ConcurrentHashMap<K, List<V>> {

    private static final long serialVersionUID = -7249946839643493614L;

    public ConcurrentMultiValuedMap() {
        super();
    }

    public void add(K key, V value) {
        List<V> values = this.getValues(key);
        values.add(value);
    }

    public void addAll(K key, List<V> value) {
        List<V> values = this.getValues(key);
        values.addAll(value);
    }

    public List<V> getValues(K key) {
        List<V> values = this.get(key);
        if (values == null) {
            values = new CopyOnWriteArrayList<>();
            List<V> old = this.putIfAbsent(key, values);
            if (old != null) {
                values = old;
            }
        }
        return values;
    }
}
