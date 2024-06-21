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

package com.baidu.hugegraph.job;

import static com.baidu.hugegraph.util.JsonUtil.fromJson;
import static com.baidu.hugegraph.util.JsonUtil.toJson;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.task.TaskManager;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.JobState;
import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.job.computer.Computer;
import com.baidu.hugegraph.job.computer.ComputerPool;
import com.baidu.hugegraph.k8s.K8sDriverProxy;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

/**
 * This class is used for call k8s-api to run OLAP algorithms, now it holds
 * one driver/proxy to watch all task results in async mode
 *
 * TODO: We should refactor the K8sProxy and make K8sDriver be singleton
 * TODO: Renamed it to ComputerJob & rename the older one to another
 */
public class ComputerDisJob extends UserJob<Object> {

    private static final Logger LOG = Log.logger(ComputerDisJob.class);

    public static final String COMPUTER_DIS = "computer-dis";
    public static final String INNER_STATUS = "inner.status";
    public static final String INNER_JOB_ID = "inner.job.id";
    public static final String FAILED_STATUS = "FAILED";

    private String innerJobId;

    private static K8sDriverProxy k8sDriverProxy;

    public static boolean check(String name, Map<String, Object> parameters) {
        Computer computer = ComputerPool.instance().find(name);
        if (computer == null) {
            return false;
        }
        computer.checkParameters(parameters);
        return true;
    }

    @Override
    public String type() {
        return COMPUTER_DIS;
    }

    @Override
    protected void cancelled() {
        super.cancelled();
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = fromJson(input, Map.class);
        String algorithm = map.get("algorithm").toString();
        String graph = map.get("graph").toString();
        String pdPeers = map.get("pd.peers").toString();
        String token = map.get("token").toString();
        int worker = Integer.parseInt(map.get("worker").toString());
        String namespace = map.get("namespace").toString();
        Object value = map.get("params");
        E.checkArgument(value instanceof Map,
                        "Invalid computer parameters '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) value;
        Map<String, String> k8sParams = new HashMap<>();
        for (Map.Entry<String, Object> item : params.entrySet()) {
            k8sParams.put(item.getKey(), item.getValue().toString());
        }

        k8sParams.put("hugegraph.name", graph);
        k8sParams.put("pd.peers", pdPeers);
        k8sParams.put("hugegraph.token", token);
        k8sParams.put("k8s.worker_instances", String.valueOf(worker));
        if (map.containsKey(INNER_JOB_ID)) {
            String jobId = (String) map.get(INNER_JOB_ID);
            K8sDriverProxy k8sDriverProxy =
            new K8sDriverProxy(String.valueOf(worker * 2), algorithm);
            boolean flag = k8sDriverProxy.getK8sDriver(namespace).cancelJob(jobId,
                                                                   k8sParams);
            // TODO: cancel api is not work now, need fix it later
            if (!flag) {
                LOG.warn("Cancel computer task failed, please check manually");
            }
            k8sDriverProxy.close(namespace);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        Map<String, Object> map = fromJson(input, Map.class);
        String status = map.containsKey(INNER_STATUS) ?
               map.get(INNER_STATUS).toString() : null;
        String jobId = map.containsKey(INNER_JOB_ID) ?
               map.get(INNER_JOB_ID).toString() : null;
        Object value = map.get("params");
        E.checkArgument(value instanceof Map,
                        "Invalid computer parameters '%s'", value);
        Map<String, Object> params = (Map<String, Object>) value;
        String algorithm = map.get("algorithm").toString();
        String graph = map.get("graph").toString();
        String pdPeers = map.get("pd.peers").toString();
        String token = map.get("token").toString();
        String namespace = map.get("namespace").toString();
        int worker = Integer.parseInt(String.valueOf(map.get("worker")));

        Map<String, String> k8sParams = new HashMap<>();
        k8sParams.put("job.partitions_count", String.valueOf(worker * 2));
        for (Map.Entry<String, Object> item : params.entrySet()) {
            k8sParams.put(item.getKey(), item.getValue().toString());
        }
        k8sParams.put("hugegraph.name", graph);
        k8sParams.put("pd.peers", pdPeers);
        k8sParams.put("hugegraph.token", token);
        k8sParams.put("k8s.worker_instances", String.valueOf(worker));

        String algorithmUrl = map.getOrDefault("k8s.algorithm_image_url", "").toString();


        if (status == null || k8sDriverProxy == null) {
            // TODO: We should reuse driver here, use one driver (DO TASK?)
            k8sDriverProxy = new K8sDriverProxy(String.valueOf(worker * 2),
                                                algorithm);
        }
        k8sParams.put("algorithm.params_class",
                      K8sDriverProxy.getAlgorithmClass(algorithm));

        if (jobId == null) {
            jobId = k8sDriverProxy
                                  .getK8sDriver(namespace, algorithmUrl)
                                  .submitJob(algorithm, k8sParams);
            LOG.info("New computerDisJob {} is submitted to namespace {} with params {}",
                                        jobId, namespace, k8sParams);
            this.innerJobId = jobId;
            map = fromJson(this.task().input(), Map.class);
            map.put(INNER_JOB_ID, jobId);
            this.task().input(toJson(map));
            LOG.info("Submit a new computer job, ID is {}", jobId);
        }

        // Watch job status here, return a future
        k8sDriverProxy.getK8sDriver(namespace).waitJobAsync(jobId, k8sParams,
                                                   this::onJobStateChanged);

        map = fromJson(this.task().input(), Map.class);
        status = map.get(INNER_STATUS).toString();
        if (FAILED_STATUS.equals(status)) {
            throw new Exception("Computer-dis job failed.");
        }
        return status;
    }

    /**
     * Update all job status immediately when K8s event return new state info
     */
    @SuppressWarnings("unchecked")
    private void onJobStateChanged(JobState observer) {
        // 异步执行任务保存，context默认为null
        // 强制提升任务权限  2022-07-12
        TaskManager.useFakeContext();

        JobStatus jobStatus = observer.jobStatus();
        Map<String, Object> innerMap = fromJson(this.task().input(), Map.class);
        innerMap.put(INNER_STATUS, jobStatus);
        // Update jobId is missing
        String jobId = innerMap.containsKey(INNER_JOB_ID) ?
               innerMap.get(INNER_JOB_ID).toString() : null;
        if (null == jobId && this.innerJobId != null) {
            innerMap.put(INNER_JOB_ID, this.innerJobId);
        }
        
        this.task().input(toJson(innerMap));

        // We overwrite the task status by observer (maybe improve later)
        switch (jobStatus) {
            case INITIALIZING:
            case RUNNING:
                this.task().result(TaskStatus.RUNNING, jobStatus.name());
                break;
            case CANCELLED:
                this.task().result(TaskStatus.CANCELLED, jobStatus.name());
                break;
            case FAILED:
                this.task().result(TaskStatus.FAILED, jobStatus.name());
                break;
            case SUCCEEDED:
                this.task().result(TaskStatus.SUCCESS, jobStatus.name());
                break;
            default:
                // do nothing now
        }
        // Update computer stage info
        this.save();
        LOG.debug("Task {} stage changed, current status is {}}",
                  this.task().id(), jobStatus);
    }
}
