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

package com.baidu.hugegraph.k8s;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.yaml.snakeyaml.Yaml;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Strings;

import io.fabric8.kubernetes.api.model.Namespace;

public class K8sManager {

    private static final HugeGraphLogger LOGGER = Log.getLogger(K8sManager.class);

    private K8sDriver k8sDriver;

    private static final K8sManager INSTANCE = new K8sManager();

    private String operatorTemplate;
    private static final String TEMPLATE_NAMESPACE = "hugegraph-computer-operator-system";
    private static final String TEMPLATE_OPERATOR_IMAGE = "image: hugegraph/hugegraph-computer-operator:latest";

    public static K8sManager instance() {
        return INSTANCE;
    }

    private K8sManager() {
    }

    public void connect(String oltpImage,
                        String olapImage, String storageImage,
                        K8sDriver.CA ca) {
        this.k8sDriver = new K8sDriver();
        this.k8sDriver.ca(ca);
        this.k8sDriver.oltpImage(oltpImage);
        this.k8sDriver.olapImage(olapImage);
        this.k8sDriver.storageImage(storageImage);
    }

    private void loadOperatorTemplate() {
        if (!Strings.isNullOrEmpty(this.operatorTemplate)) {
            return;
        }
        try {
            File file = new File(CoreOptions.K8S_OPERATOR_TEMPLATE.defaultValue());
            FileReader reader = new FileReader(file);
            int length = (int)file.length();
            char[] buffer = new char[length];
            reader.read(buffer, 0, length);
            this.operatorTemplate = new String(buffer);
            reader.close();
        } catch (IOException ignored) {
        }
    }

    public Namespace namespace(String ns) {
        return this.k8sDriver.namespace(ns);
    }

    public Namespace createNamespace(String namespace, Map<String, String> labelMap) {
        return this.k8sDriver.createNamespace(namespace, labelMap);
    }

    @SuppressWarnings("unchecked")
    public Set<String> createOltpService(GraphSpace graphSpace,
                                        Service service,
                                        List<String> metaServers,
                                        String cluster) {
        
        if (null == k8sDriver) {
            LOGGER.logCriticalError(new HugeException("k8sDriver is not initialized!"), "startOltpService");
            return Collections.EMPTY_SET;
        }
        return this.k8sDriver.createOltpService(graphSpace, service,
                                               metaServers, cluster);
    }

    @SuppressWarnings("unchecked")
    public Set<String> startOltpService(GraphSpace graphSpace,
                                        Service service,
                                        List<String> metaServers,
                                        String cluster) {
        if (null == k8sDriver) {
            LOGGER.logCriticalError(new HugeException("k8sDriver is not initialized!"), "startOltpService");
            return Collections.EMPTY_SET;
        }
        return this.k8sDriver.startOltpService(graphSpace, service,
                                               metaServers, cluster);
    }

    public Set<String> createService(GraphSpace graphSpace, Service service,
                                    List<String> metaServers, String cluster) {
        switch (service.type()) {
            case OLTP:
                return this.createOltpService(graphSpace, service,
                                              metaServers, cluster);
            case OLAP:
            case STORAGE:
            default:
                throw new AssertionError(String.format(
                          "Invalid service type '%s'", service.type()));
        }
    }

    public Set<String> startService(GraphSpace graphSpace, Service service,
                                    List<String> metaServers, String cluster) {
        switch (service.type()) {
            case OLTP:
                return this.startOltpService(graphSpace, service,
                                             metaServers, cluster);
            case OLAP:
            case STORAGE:
            default:
                throw new AssertionError(String.format(
                          "Invalid service type '%s'", service.type()));
        }
    }

    public void stopService(GraphSpace graphSpace, Service service) {
        if (null == k8sDriver) {
            LOGGER.logCriticalError(new HugeException("k8sDriver is not initialized!"), "stopService");
            return;
        }
        switch (service.type()) {
            case OLTP:
                this.k8sDriver.stopOltpService(graphSpace, service);
            case OLAP:
            case STORAGE:
            default:
                LOGGER.logCustomDebug("Cannot stop service other than OLTP", "K8sManager");
        }
    }

    public void deleteService(GraphSpace graphSpace, Service service) {
        if (null == k8sDriver) {
            LOGGER.logCriticalError(new HugeException("k8sDriver is not initialized!"), "stopService");
            return;
        }
        switch (service.type()) {
            case OLTP:
                this.k8sDriver.deleteOltpService(graphSpace, service);
                break;
            case OLAP:
            case STORAGE:
            default:
                LOGGER.logCustomDebug("Cannot stop service other than OLTP", "K8sManager");
        }
    }

    public int podsRunning(GraphSpace graphSpace, Service service) {
        if (null == k8sDriver) {
            throw new HugeException("k8sDriver is not initialized!");
        }
        return this.k8sDriver.podsRunning(graphSpace, service);
    }

    public void createOperatorPod(String namespace, String imagePath) {
        if (Strings.isNullOrEmpty(imagePath)) {
            LOGGER.logCriticalError(new IllegalArgumentException("imagePath should not be empty"), "Cannot create operator pod");
            return;
        }
        this.loadOperator(namespace, imagePath);         
    }

    public void loadOperator(String namespace, String imagePath)
                throws HugeException {
        try {
            this.loadOperatorTemplate();
            if (Strings.isNullOrEmpty(this.operatorTemplate)) {
                throw new HugeException("Cannot generate yaml config for operator: template load failed");
            }

            String content = this.operatorTemplate.replaceAll(TEMPLATE_NAMESPACE,
                                                              namespace);
            String image = "image: " + imagePath;
            content = content.replaceAll(TEMPLATE_OPERATOR_IMAGE, image);

            k8sDriver.createOrReplaceByYaml(content);
        } catch (IOException e) {
            LOGGER.logCriticalError(e, "IO Exception when create operator");
        } catch (Exception e) {
            LOGGER.logCriticalError(e, "Unknown Exception when create operator");
        }
    }

    @SuppressWarnings("unchecked")
    public void loadResourceQuota(String namespace, int cpuLimit, int memoryLimit) throws HugeException {
        Yaml yaml = new Yaml();
        FileInputStream inputStream = null;

        try {

            String fileName = CoreOptions.K8S_QUOTA_TEMPLATE.defaultValue();

            inputStream = new FileInputStream(fileName);
            Map<String, Object> quotaMap = yaml.load(inputStream);
            Map<String, Object> metaData = (Map<String, Object>)quotaMap.get("metadata");
            Map<String, Object> spec = (Map<String, Object>)quotaMap.get("spec");
            Map<String, Object> hard = (Map<String, Object>)spec.get("hard");

            metaData.put("name", namespace + "-resource-quota");

            String cpuLimitStr = String.valueOf(cpuLimit);
            String memLimitStr = String.valueOf(memoryLimit) + "Gi";
            hard.put("requests.cpu", cpuLimitStr);
            hard.put("limits.cpu", cpuLimitStr);
            hard.put("requests.memory", memLimitStr);
            hard.put("limits.memory", memLimitStr);

            StringWriter writer = new StringWriter();
            yaml.dump(quotaMap, writer);
            String yamlStr = writer.toString();
            k8sDriver.createResourceQuota(namespace, yamlStr);
        } catch (Exception e) {
            LOGGER.logCriticalError(e, "Failed to load resource quota!");
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
