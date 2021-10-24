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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.baidu.hugegraph.auth.HugeAccess;
import com.baidu.hugegraph.auth.HugeBelong;
import com.baidu.hugegraph.auth.HugeGroup;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.backend.id.Id;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;

import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.collection.CollectionFactory;

public class MetaManager {

    public static final String META_PATH_DELIMETER = "/";
    public static final String META_PATH_JOIN = "-";

    public static final String META_PATH_HUGEGRAPH = "HUGEGRAPH";
    public static final String META_PATH_GRAPHSPACE = "GRAPHSPACE";
    public static final String META_PATH_GRAPH_CONF = "GRAPH_CONF";
    public static final String META_PATH_CONF = "CONF";
    public static final String META_PATH_GRAPH = "GRAPH";
    public static final String META_PATH_AUTH = "AUTH";
    public static final String META_PATH_USER = "USER";
    public static final String META_PATH_GROUP = "GROUP";
    public static final String META_PATH_TARGET = "TARGET";
    public static final String META_PATH_BELONG = "BELONG";
    public static final String META_PATH_ACCESS = "ACCESS";
    public static final String META_PATH_EVENT = "EVENT";
    public static final String META_PATH_ADD = "ADD";
    public static final String META_PATH_REMOVE = "REMOVE";

    public static final String DEFAULT_NAMESPACE = "default_ns";

    private MetaDriver metaDriver;
    private String cluster;

    private static final MetaManager INSTANCE = new MetaManager();

    public static MetaManager instance() {
        return INSTANCE;
    }

    private MetaManager() {
    }

    public void connect(String cluster, MetaDriverType type, Object... args) {
        E.checkArgument(cluster != null && !cluster.isEmpty(),
                        "The cluster can't be null or empty");
        this.cluster = cluster;

        switch (type) {
            case ETCD:
                this.metaDriver = new EtcdMetaDriver(args);
                break;
            case PD:
                // TODO: uncomment after implement PdMetaDriver
                // this.metaDriver = new PdMetaDriver(args);
                break;
            default:
                throw new AssertionError(String.format(
                          "Invalid meta driver type: %s", type));
        }
    }

    public <T> void listenGraphAdd(Consumer<T> consumer) {
        this.listen(this.graphAddKey(), consumer);
    }

    public <T> void listenGraphRemove(Consumer<T> consumer) {
        this.listen(this.graphRemoveKey(), consumer);
    }

    private <T> void listen(String key, Consumer<T> consumer) {
        this.metaDriver.listen(key, consumer);
    }

    public Map<String, String> graphConfigs() {
        Map<String, String> configs =
                            CollectionFactory.newMap(CollectionType.EC);
        Map<String, String> keyValues = this.metaDriver
                                            .scanWithPrefix(this.confPrefix());
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(META_PATH_DELIMETER);
            configs.put(parts[parts.length - 1], entry.getValue());
        }
        return configs;
    }

    public <T> List<Pair<String, String>> extractGraphsFromResponse(T response) {
        List<Pair<String, String>> graphs = new ArrayList<>();
        for (String value : this.metaDriver.extractValuesFromResponse(response)) {
            String[] values = value.split(META_PATH_JOIN);
            E.checkArgument(values.length == 2,
                            "Graph name must be '{namespace}-{graph}', " +
                            "but got '%s'", value);
            String namespace = values[0];
            String graphName = values[1];
            graphs.add(Pair.of(namespace, graphName));
        }
        return graphs;
    }

    public String getGraphConfig(String graph) {
        return this.metaDriver.get(this.graphConfKey(graph));
    }

    public void addGraphConfig(String graph, String config) {
        this.metaDriver.put(this.graphConfKey(graph), config);
    }

    public void removeGraphConfig(String graph) {
        this.metaDriver.delete(this.graphConfKey(graph));
    }

    public void addGraph(String graph) {
        this.metaDriver.put(this.graphAddKey(), this.nsGraphName(graph));
    }

    public void removeGraph(String graph) {
        this.metaDriver.put(this.graphRemoveKey(), this.nsGraphName(graph));
    }

    private String graphAddKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/ADD
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_ADD);
    }

    private String graphRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/REMOVE
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_REMOVE);
    }

    private String confPrefix() {
        return this.graphConfKey(Strings.EMPTY);
    }

    // TODO: remove after support namespace
    private String graphConfKey(String graph) {
        return this.graphConfKey(DEFAULT_NAMESPACE, graph);
    }

    private String graphConfKey(String namespace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{namespace}/GRAPH_CONF
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           namespace, META_PATH_GRAPH_CONF, graph);
    }

    private String userKey(String name) {
        // HUGEGRAPH/{cluster}/AUTH/USER/{user}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH, META_PATH_USER, name);
    }

    private String userListKey() {
        // HUGEGRAPH/{cluster}/AUTH/USER
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH, META_PATH_USER);
    }

    private String groupKey(String graphSpace, String group) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/GROUP/{group}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_GROUP, group);
    }

    private String groupListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/GROUP
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_GROUP);
    }

    private String targetKey(String graphSpace, String target) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET/{target}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_TARGET, target);
    }

    private String targetListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_TARGET);
    }

    private String belongKey(String graphSpace, String belong) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{belong}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG, belong);
    }

    private String belongListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG);
    }

    private String belongListKeyByUser(String graphSpace, String userName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{userName}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG, userName);
    }

    private String accessKey(String graphSpace, String access) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{access}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, access);
    }

    private String accessListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{access}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH);
    }

    private String accessListKeyByGroup(String graphSpace, String groupName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{groupName}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                META_PATH_AUTH, groupName);
    }

    public String belongId(String userName, String groupName) {
        E.checkArgument(StringUtils.isNotEmpty(userName) &&
                        StringUtils.isNotEmpty(groupName),
                        "The user name '%s' or group name '%s' is empty",
                        userName, groupName);
        return String.join("->", userName, groupName);
    }

    public String accessId(String groupName, String targetName) {
        E.checkArgument(StringUtils.isNotEmpty(groupName) &&
                        StringUtils.isNotEmpty(targetName),
                        "The group name '%s' or target name '%s' is empty",
                        groupName, targetName);
        return String.join("->", groupName, targetName);
    }

    // TODO: remove after support namespace
    private String nsGraphName(String name) {
        return this.nsGraphName(DEFAULT_NAMESPACE, name);
    }

    private String nsGraphName(String namespace, String name) {
        return String.join(META_PATH_JOIN, namespace, name);
    }

    private <T> String serialize(T object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(object);
        return bos.toString();

    }

    private <T> T deserialize(String content)
            throws IOException, ClassNotFoundException {
        byte[] source = content.getBytes();
        ByteArrayInputStream bis = new ByteArrayInputStream(source);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (T) ois.readObject();
    }

    public void createUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The user name '%s' has existed", user.name());
        this.metaDriver.put(userKey(user.name()), serialize(user));

    }

    public void updateUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", user.name());

        this.metaDriver.put(userKey(user.name()), serialize(user));
    }

    public HugeUser deleteUser(Id id)
                               throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(userKey(id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", id.asString());
        this.metaDriver.delete(userKey(id.asString()));
        return deserialize(result);
    }

    public HugeUser findUser(String name)
                             throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(userKey(name));
        if (StringUtils.isEmpty(result)) {
            return null;
        }

        return deserialize(result);
    }

    public List<HugeUser> listUsers(List<Id> ids)
                                    throws IOException,
                                    ClassNotFoundException {
        List<HugeUser> result = new ArrayList<>();
        Map<String, String> userMap =
                            this.metaDriver.scanWithPrefix(userListKey());
        for (Id id : ids) {
            if (userMap.containsKey(id.asString())) {
                HugeUser user = deserialize(userMap.get(id.asString()));
                result.add(user);
            }
        }

        return result;
    }

    public List<HugeUser> listAllUsers(long limit)
                                       throws IOException,
                                       ClassNotFoundException {
        List<HugeUser> result = new ArrayList<>();
        Map<String, String> userMap =
                            this.metaDriver.scanWithPrefix(userListKey());
        for (Map.Entry<String, String> item : userMap.entrySet()) {
            HugeUser user = deserialize(item.getValue());
            result.add(user);
            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public Id createGroup(String graphSpace, HugeGroup group)
                            throws IOException {
        String result = this.metaDriver.get(groupKey(graphSpace, group.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The group name '%s' has existed", group.name());
        this.metaDriver.put(groupKey(graphSpace, group.name()),
                            serialize(group));
        return group.id();
    }

    public Id updateGroup(String graphSpace, HugeGroup group)
                          throws IOException {
        String result = this.metaDriver.get(groupKey(graphSpace, group.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", group.name());
        this.metaDriver.put(groupKey(graphSpace, group.name()),
                            serialize(group));
        return group.id();
    }

    public HugeGroup deleteGroup(String graphSpace, Id id)
                                 throws IOException,
                                 ClassNotFoundException {
        String result = this.metaDriver.get(groupKey(graphSpace,
                                                     id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", id.asString());
        this.metaDriver.delete(groupKey(graphSpace, id.asString()));
        return deserialize(result);
    }

    public HugeGroup getGroup(String graphSpace, Id id)
                              throws IOException,
                              ClassNotFoundException {
        String result = this.metaDriver.get(groupKey(graphSpace,
                                                     id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                "The group name '%s' is not existed", id.asString());
        return deserialize(result);
    }

    public List<HugeGroup> listGroups(String graphSpace, List<Id> ids)
                                      throws IOException,
                                      ClassNotFoundException {
        List<HugeGroup> result = new ArrayList<>();
        Map<String, String> groupMap =
                            this.metaDriver.scanWithPrefix(groupListKey(graphSpace));
        for (Id id : ids) {
            if (groupMap.containsKey(id.asString())) {
                HugeGroup group = deserialize(groupMap.get(id.asString()));
                result.add(group);
            }
        }

        return result;
    }

    public List<HugeGroup> listAllGroups(String graphSpace, long limit)
                                         throws IOException,
                                         ClassNotFoundException {
        List<HugeGroup> result = new ArrayList<>();
        Map<String, String> groupMap =
                            this.metaDriver.scanWithPrefix(groupListKey(graphSpace));
        for (Map.Entry<String, String> item : groupMap.entrySet()) {
            HugeGroup group = deserialize(item.getValue());
            result.add(group);
            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public Id createTarget(String graphSpace, HugeTarget target)
                           throws IOException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      target.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The target name '%s' has existed", target.name());
        this.metaDriver.put(targetKey(graphSpace, target.name()),
                            serialize(target));
        return target.id();
    }

    public Id updateTarget(String graphSpace, HugeTarget target)
                           throws IOException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      target.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", target.name());
        this.metaDriver.put(targetKey(graphSpace, target.name()),
                            serialize(target));
        return target.id();
    }

    public HugeTarget deleteTarget(String graphSpace, Id id)
                                   throws IOException,
                                   ClassNotFoundException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", id.asString());
        this.metaDriver.delete(targetKey(graphSpace, id.asString()));
        return deserialize(result);
    }

    public HugeTarget getTarget(String graphSpace, Id id)
                                throws IOException,
                                ClassNotFoundException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", id.asString());
        return deserialize(result);
    }

    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids)
                                        throws IOException,
                                        ClassNotFoundException {
        List<HugeTarget> result = new ArrayList<>();
        Map<String, String> targetMap =
                            this.metaDriver.scanWithPrefix(targetListKey(graphSpace));
        for (Id id : ids) {
            if (targetMap.containsKey(id.asString())) {
                HugeTarget target = deserialize(targetMap.get(id.asString()));
                result.add(target);
            }
        }

        return result;
    }

    public List<HugeTarget> listAllTargets(String graphSpace, long limit)
                                           throws IOException,
                                           ClassNotFoundException {
        List<HugeTarget> result = new ArrayList<>();
        Map<String, String> targetMap =
                            this.metaDriver.scanWithPrefix(targetListKey(graphSpace));
        for (Map.Entry<String, String> item : targetMap.entrySet()) {
            HugeTarget target = deserialize(item.getValue());
            result.add(target);
            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public Id createBelong(String graphSpace, HugeBelong belong)
                           throws IOException, ClassNotFoundException {
        HugeUser user = this.findUser(belong.source().asString());
        E.checkArgument(user != null,
                        "The user name '%s' is not existed",
                        belong.source().asString());
        HugeGroup group = this.getGroup(graphSpace, belong.target());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        belong.target().asString());

        String belongId = belongId(user.name(), group.name());
        String result = this.metaDriver.get(belongKey(graphSpace, belongId));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The belong name '%s' has existed", belongId);
        this.metaDriver.put(belongKey(graphSpace, belongId), serialize(belong));
        return belong.id();
    }

    public Id updateBelong(String graphSpace, HugeBelong belong)
                           throws IOException, ClassNotFoundException {
        HugeUser user = this.findUser(belong.source().asString());
        E.checkArgument(user != null,
                        "The user name '%s' is not existed",
                        belong.source().asString());
        HugeGroup group = this.getGroup(graphSpace, belong.target());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        belong.target().asString());

        String belongId = belongId(user.name(), group.name());
        String result = this.metaDriver.get(belongKey(graphSpace, belongId));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", belongId);
        this.metaDriver.put(belongKey(graphSpace, belongId), serialize(belong));
        return belong.id();
    }

    public HugeBelong deleteBelong(String graphSpace, Id id)
                                   throws IOException,
                                   ClassNotFoundException {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", id.asString());
        this.metaDriver.delete(belongKey(graphSpace, id.asString()));
        return deserialize(result);
    }

    public HugeBelong getBelong(String graphSpace, Id id)
                                throws IOException,
                                ClassNotFoundException {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", id.asString());
        return deserialize(result);
    }

    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids)
                                       throws IOException,
                                       ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap =
                            this.metaDriver.scanWithPrefix(belongListKey(graphSpace));
        for (Id id : ids) {
            if (belongMap.containsKey(id.asString())) {
                HugeBelong belong = deserialize(belongMap.get(id.asString()));
                result.add(belong);
            }
        }

        return result;
    }

    public List<HugeBelong> listAllBelong(String graphSpace, long limit)
                                          throws IOException,
                                          ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap =
                            this.metaDriver.scanWithPrefix(belongListKey(graphSpace));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            HugeBelong belong = deserialize(item.getValue());
            result.add(belong);
            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public List<HugeBelong> listBelongByUser(String graphSpace,
                                             Id user, long limit)
                                             throws IOException,
                                             ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(
                            belongListKeyByUser(graphSpace, user.asString()));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            HugeBelong belong = deserialize(item.getValue());
            result.add(belong);
            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public String groupFromBelong(String belongKey) {
        E.checkArgument(StringUtils.isNotEmpty(belongKey),
                        "The belong name '%s' is empty", belongKey);
        E.checkArgument(belongKey.contains("->"),
                        "The belong name '%s' is invalid", belongKey);
        String[] items = belongKey.split("->");
        E.checkArgument(items.length == 2,
                        "The belong name '%s' is invalid", belongKey);
        return items[1];
    }

    public List<HugeBelong> listBelongByGroup(String graphSpace,
                                              Id group, long limit)
                                              throws IOException,
                                              ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(
                                        belongListKey(graphSpace));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            String groupName = groupFromBelong(item.getKey());
            if (groupName.equals(group.asString())) {
                HugeBelong belong = deserialize(item.getValue());
                result.add(belong);
            }

            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public Id createAccess(String graphSpace, HugeAccess access)
                           throws IOException, ClassNotFoundException {
        HugeGroup group = this.getGroup(graphSpace, access.source());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        access.source().asString());

        HugeTarget target = this.getTarget(graphSpace, access.target());
        E.checkArgument(target != null,
                        "The target name '%s' is not existed",
                        access.target().asString());

        String accessId = accessId(group.name(), target.name());
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The access name '%s' has existed", accessId);
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(access));
        return access.id();
    }

    public Id updateAccess(String graphSpace, HugeAccess access)
                           throws IOException, ClassNotFoundException {
        HugeGroup group = this.getGroup(graphSpace, access.source());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        access.source().asString());

        HugeTarget target = this.getTarget(graphSpace, access.target());
        E.checkArgument(target != null,
                        "The target name '%s' is not existed",
                        access.target().asString());

        String accessId = accessId(group.name(), target.name());
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", accessId);
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(access));
        return access.id();
    }

    public HugeAccess deleteAccess(String graphSpace, Id id)
                                   throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        this.metaDriver.delete(accessKey(graphSpace, id.asString()));
        return deserialize(result);
    }

    public HugeAccess getAccess(String graphSpace, Id id)
                                throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        return deserialize(result);
    }

    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids)
                                       throws IOException,
                                       ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap =
                            this.metaDriver.scanWithPrefix(accessListKey(graphSpace));
        for (Id id : ids) {
            if (accessMap.containsKey(id.asString())) {
                HugeAccess access = deserialize(accessMap.get(id.asString()));
                result.add(access);
            }
        }

        return result;
    }

    public List<HugeAccess> listAllAccess(String graphSpace, long limit)
                                          throws IOException,
                                          ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap =
                            this.metaDriver.scanWithPrefix(accessListKey(graphSpace));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            HugeAccess access = deserialize(item.getValue());
            result.add(access);
            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public List<HugeAccess> listAccessByGroup(String graphSpace,
                                              Id group, long limit)
                                              throws IOException,
                                              ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap = this.metaDriver.scanWithPrefix(
                            accessListKeyByGroup(graphSpace, group.asString()));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            HugeAccess access = deserialize(item.getValue());
            result.add(access);
            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public String targetFromAccess(String accessKey) {
        E.checkArgument(StringUtils.isNotEmpty(accessKey),
                        "The access name '%s' is empty", accessKey);
        E.checkArgument(accessKey.contains("->"),
                        "The access name '%s' is invalid", accessKey);
        String[] items = accessKey.split("->");
        E.checkArgument(items.length == 2,
                        "The access name '%s' is invalid", accessKey);
        return items[1];
    }

    public List<HugeAccess> listAccessByTarget(String graphSpace,
                                               Id target, long limit)
                                               throws IOException,
                                               ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap = this.metaDriver.scanWithPrefix(
                                        accessListKey(graphSpace));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            String targetName = targetFromAccess(item.getKey());
            if (targetName.equals(target.asString())) {
                HugeAccess access = deserialize(item.getValue());
                result.add(access);
            }

            if (result.size() >= limit) {
                break;
            }
        }

        return result;
    }

    public enum MetaDriverType {
        ETCD,
        PD
    }
}
