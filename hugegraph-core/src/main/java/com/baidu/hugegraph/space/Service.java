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

package com.baidu.hugegraph.space;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.baidu.hugegraph.util.E;

public class Service {

    public static final int DEFAULT_COUNT = 1;
    public static final String DEFAULT_ROUTE_TYPE = "ClusterIP";
    public static final int DEFAULT_PORT = 8080;

    public static final int DEFAULT_CPU_LIMIT = 4;
    public static final int DEFAULT_MEMORY_LIMIT = 8;
    public static final int DEFAULT_STORAGE_LIMIT = 100;

    private String name;
    private ServiceType type;
    private String description;
    private int count;

    private int cpuLimit;
    private int memoryLimit; // GB
    private int storageLimit; // GB

    private String routeType;
    private int port;

    private Set<String> urls;

    public Service(String name, ServiceType type) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of service can't be null or empty");
        E.checkArgumentNotNull(type, "The type of service can't be null");
        this.name = name;
        this.type = type;
        this.count = DEFAULT_COUNT;
        this.routeType = DEFAULT_ROUTE_TYPE;
        this.port = DEFAULT_PORT;
        this.cpuLimit = DEFAULT_CPU_LIMIT;
        this.memoryLimit = DEFAULT_MEMORY_LIMIT;
        this.storageLimit = DEFAULT_STORAGE_LIMIT;
    }

    public Service(String name, String description, ServiceType type,
                   int count, int cpuLimit, int memoryLimit, int storageLimit,
                   String routeType, int port, Set<String> urls) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of service can't be null or empty");
        E.checkArgumentNotNull(type, "The type of service can't be null");
        this.name = name;
        this.description = description;
        this.type = type;
        this.count = count;
        this.cpuLimit = cpuLimit;
        this.memoryLimit = memoryLimit;
        this.storageLimit = storageLimit;
        this.routeType = routeType;
        this.port = port;
        this.urls = urls;
    }

    public String name() {
        return this.name;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    public ServiceType type() {
        return this.type;
    }

    public void type(ServiceType type) {
        this.type = type;
    }

    public int count() {
        return this.count;
    }

    public void count(int count) {
        E.checkArgument(count > 0,
                        "The service count must be > 0, but got: %s", count);
        this.count = count;
    }

    public int cpuLimit() {
        return this.cpuLimit;
    }

    public void cpuLimit(int cpuLimit) {
        E.checkArgument(cpuLimit > 0,
                        "The cpu limit must be > 0, but got: %s", cpuLimit);
        this.cpuLimit = cpuLimit;
    }

    public int memoryLimit() {
        return this.memoryLimit;
    }

    public void memoryLimit(int memoryLimit) {
        E.checkArgument(memoryLimit > 0,
                        "The memory limit must be > 0, but got: %s",
                        memoryLimit);
        this.memoryLimit = memoryLimit;
    }

    public int storageLimit() {
        return this.storageLimit;
    }

    public void storageLimit(int storageLimit) {
        E.checkArgument(storageLimit > 0,
                        "The storage limit must be > 0, but got: %s",
                        storageLimit);
        this.storageLimit = storageLimit;
    }

    public String routeType() {
        return this.routeType;
    }

    public void routeType(String routeType) {
        this.routeType = routeType;
    }

    public int port() {
        return this.port;
    }

    public void port(int port) {
        E.checkArgument(port > 0,
                        "The port must be > 0, but got: %s", port);
        this.port = port;
    }

    public Set<String> urls() {
        return this.urls;
    }

    public void urls(Set<String> urls) {
        this.urls = urls;
    }

    public void url(String url) {
        if (this.urls == null) {
            this.urls = new HashSet<>();
        }
        this.urls.add(url);
    }

    public Map<String, Object> info() {
        Map<String, Object> infos = new LinkedHashMap<>();
        infos.put("name", this.name);
        infos.put("type", this.type);
        infos.put("description", this.description);
        infos.put("count", this.count);

        infos.put("cpu_limit", this.cpuLimit);
        infos.put("memory_limit", this.memoryLimit);
        infos.put("storage_limit", this.storageLimit);

        infos.put("route_type", this.routeType);
        infos.put("port", this.port);
        infos.put("urls", this.urls);

        return infos;
    }

    public enum ServiceType {
        OLTP,
        OLAP,
        STORAGE
    }
}
