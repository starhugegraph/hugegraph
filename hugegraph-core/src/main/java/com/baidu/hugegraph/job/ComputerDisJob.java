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

import com.baidu.hugegraph.computer.driver.DefaultJobState;
import com.baidu.hugegraph.computer.driver.JobObserver;
import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.job.computer.Computer;
import com.baidu.hugegraph.job.computer.ComputerPool;
import com.baidu.hugegraph.k8s.K8sDriverProxy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import org.mockito.Mockito;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ComputerDisJob extends SysJob<Object> {

    private ExecutorService executorService= Executors.newSingleThreadExecutor();
    CompletableFuture<Void> future = null;

    public static final String COMPUTER = "computer-proxy";

    public static boolean check(String name, Map<String, Object> parameters) {
        Computer computer = ComputerPool.instance().find(name);
        if (computer == null) {
            return false;
        }
        computer.checkParameters(parameters);
        return true;
    }

    public String computerConfigPath() {
        return this.params().configuration().get(CoreOptions.COMPUTER_CONFIG);
    }

    @Override
    public String type() {
        return COMPUTER;
    }

    @Override
    protected void cancelled() {
        super.cancelled();
        if (future != null) {
            future.getNow(null);
            executorService.shutdown();
        }


        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);
        Object value = map.get("parameters");
        E.checkArgument(value instanceof Map, "Invalid computer parameters '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, Object> parameters = (Map<String, Object>) value;
        String args = parameters.get("arguments").toString();
        JsonObject jsonObject = Json.createReader(new StringReader(args)).readObject();

        Map<String, String> params = new HashMap<>();
        params.put("k8s.worker_instances", jsonObject.get("k8s.worker_instances") == null ?
                "1" : jsonObject.get("k8s.worker_instances").toString());

        if (map.containsKey("inner.job_id")) {
            String jobId = (String) map.get("inner.job_id");
            K8sDriverProxy k8sDriverProxy = new K8sDriverProxy();
            k8sDriverProxy.getKubernetesDriver().cancelJob(jobId, params);
        }
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);
        String status = map.containsKey("inner.status") ?
                (String) map.get("inner.status") : null;
        String jobId = map.containsKey("inner.job_id") ?
                (String) map.get("inner.job_id") : null;
        Object value = map.get("parameters");
        E.checkArgument(value instanceof Map, "Invalid computer parameters '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, Object> parameters = (Map<String, Object>) value;

        String computer = (String)map.get("computer");

        String args = parameters.get("arguments").toString();
        JsonObject jsonObject = Json.createReader(new StringReader(args)).readObject();

        Map<String, String> params = new HashMap<>();
        params.put("computer", computer);
        params.put("k8s.worker_instances", jsonObject.get("k8s.worker_instances") == null ?
                "1" : jsonObject.get("k8s.worker_instances").toString() );
        params.put("transport.server_port", jsonObject.get("transport.server_port") == null ?
                "0" : jsonObject.get("transport.server_port").toString());
        params.put("rpc.server_port", jsonObject.get("rpc.server_port") == null ?
                "0" : jsonObject.get("rpc.server_port").toString());

        if (status == null || !status.equals("0")) {
            // TODO: DO TASK
            K8sDriverProxy k8sDriverProxy = new K8sDriverProxy();
            if (jobId == null) {
                jobId = k8sDriverProxy.getKubernetesDriver().submitJob(computer, params);
                map.put("inner.job_id", jobId);
                this.task().input(JsonUtil.toJson(map));
            }

            JobObserver jobObserver = Mockito.mock(JobObserver.class);
            String finalJobId = jobId;
            future = CompletableFuture.runAsync(() -> {
                k8sDriverProxy.getKubernetesDriver().waitJob(finalJobId, params, jobObserver);
            }, executorService);

            /*
            DefaultJobState jobState = new DefaultJobState();
            jobState.jobStatus(JobStatus.INITIALIZING);
            Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
                    .onJobStateChanged(Mockito.eq(jobState));
             */

            DefaultJobState jobState2 = new DefaultJobState();
            jobState2.jobStatus(JobStatus.SUCCEEDED);
            Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
                    .onJobStateChanged(Mockito.eq(jobState2));

            future.getNow(null);

        }

        return 0;
    }
}
