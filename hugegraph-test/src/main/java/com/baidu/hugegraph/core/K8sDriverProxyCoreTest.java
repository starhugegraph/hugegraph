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

package com.baidu.hugegraph.core;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.computer.driver.DefaultJobState;
import com.baidu.hugegraph.computer.driver.JobObserver;
import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.k8s.K8sDriverProxy;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.util.ExecutorUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

public class K8sDriverProxyCoreTest extends BaseCoreTest {

    private static String NAMESPACE = "hugegraph-computer-system";
    private static String KUBE_CONFIG = "conf/kube.kubeconfig";
    private static String HUGEGRAPH_URL = "127.0.0.1:8080";
    private static String ENABLE_INTERNAL_ALGORITHM = "true";
    private static String INTERNAL_ALGORITHM_IMAGE_URL = "hugegraph/" +
            "hugegraph-computer-based-algorithm:beta1";

    private static String INTERNAL_ALGORITHM = "[pagerank]";
    private static String PARAMS_CLASS = "com.baidu.hugegraph.computer." +
                                         "algorithm.rank.pagerank." +
                                         "PageRankParams";

    private static String COMPUTER = "pagerank";

    private static ExecutorService POOL;

    @BeforeClass
    public static void init() {
        POOL = ExecutorUtil.newFixedThreadPool(1, "k8s-driver-test-pool");
    }

    @Before
    @Override
    public void setup() {
        super.setup();

        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        Iterator<HugeTask<Object>> iter = scheduler.tasksProxy(null, -1, null);
        while (iter.hasNext()) {
            scheduler.delete(iter.next().id());
        }

        try {
            K8sDriverProxy.setCubeConfig(NAMESPACE,
                                        KUBE_CONFIG,
                                        HUGEGRAPH_URL,
                                        ENABLE_INTERNAL_ALGORITHM,
                                        INTERNAL_ALGORITHM_IMAGE_URL);
        } catch (IOException e) {
            // ignore
        }
    }

    @Test
    public void testK8sTask() throws TimeoutException {
        Map<String, String> params = new HashMap<>();
        params.put("computer", COMPUTER);
        params.put("k8s.worker_instances", "2");
        params.put("transport.server_port", "0");
        params.put("rpc.server_port", "0");
        K8sDriverProxy k8sDriverProxy = new K8sDriverProxy("2",
                                                            INTERNAL_ALGORITHM,
                                                            PARAMS_CLASS);
        String jobId = k8sDriverProxy.getKubernetesDriver()
                .submitJob(COMPUTER, params);


        JobObserver jobObserver = Mockito.mock(JobObserver.class);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            k8sDriverProxy.getKubernetesDriver().waitJob(jobId, params, jobObserver);
        }, POOL);

        DefaultJobState jobState = new DefaultJobState();
        jobState.jobStatus(JobStatus.INITIALIZING);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
                .onJobStateChanged(Mockito.eq(jobState));

        DefaultJobState jobState2 = new DefaultJobState();
        jobState2.jobStatus(JobStatus.SUCCEEDED);
        Mockito.verify(jobObserver, Mockito.timeout(15000L).atLeast(1))
                .onJobStateChanged(Mockito.eq(jobState2));

        future.getNow(null);
        k8sDriverProxy.close();
    }
}
