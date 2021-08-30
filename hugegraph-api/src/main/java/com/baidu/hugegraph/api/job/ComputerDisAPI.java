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

package com.baidu.hugegraph.api.job;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.page.PageInfo;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.job.ComputerDisJob;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import org.apache.groovy.util.Maps;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.*;

import static com.baidu.hugegraph.backend.query.Query.NO_LIMIT;

@Path("graphs/{graph}/jobs/computerdis")
@Singleton
public class ComputerDisAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Path("/{computer}")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> post(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                @PathParam("computer") String computer,
                                Map<String, Object> parameters) {
        LOG.debug("Graph [{}] schedule computer dis job: {}", graph, parameters);
        E.checkArgument(computer != null && !computer.isEmpty(),
                        "The computer name can't be empty");
        E.checkArgument(parameters != null,
                "The parameters can't be empty");
        Map<String, Object> input = ImmutableMap.of("computer", computer,
                "parameters", parameters, "inner.status", "1");
        HugeGraph g = graph(manager, graph);
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name("computer-proxy:" + computer)
                .input(JsonUtil.toJson(input))
                .job(new ComputerDisJob());
        HugeTask<Object> task = builder.schedule();
        return ImmutableMap.of("task_id", task.id(),
                "message", "success");
    }

    @DELETE
    @Timed
    @Path("/{taskId}")
    @Status(Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> delete(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                @PathParam("taskId") String taskId) {
        LOG.debug("Graph [{}] delete computer job: {}", graph, taskId);
        E.checkArgument(taskId != null && !taskId.isEmpty(),
                "The computer name can't be empty");
        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        HugeTask<?> task = scheduler.delete(IdGenerator.of(taskId));
        E.checkArgument(task != null, "There is no task with id '%s'", taskId);
        return ImmutableMap.of("task_id", taskId, "message", "success");
    }

    @PUT
    @Timed
    @Path("/{taskId}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> cancel(@Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      @PathParam("taskId") String taskId) {
        LOG.debug("Graph [{}] cancel computer job: {}", graph, taskId);
        E.checkArgument(taskId != null && !taskId.isEmpty(),
                "The computer name can't be empty");

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(taskId));
        if (!task.completed() && !task.cancelling()) {
            scheduler.cancel(task);
            if (task.cancelling()) {
                return task.asMap();
            }
        }

        assert task.completed() || task.cancelling();
        return ImmutableMap.of("task_id", taskId, "message", "success");
    }

    @GET
    @Timed
    @Path("/{taskId}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               @PathParam("taskId") String taskId) {
        LOG.debug("Graph [{}] get task info", graph);
        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        return scheduler.task(IdGenerator.of(taskId)).asMap();
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                @QueryParam("status") String status,
                                @QueryParam("limit") @DefaultValue("100") long limit,
                                @QueryParam("page") String page) {
        LOG.debug("Graph [{}] get task list", graph);
        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        Iterator<HugeTask<Object>> iter;
        if (status == null) {
            iter = scheduler.tasksProxy(null, limit, page);
        } else {
            iter = scheduler.tasksProxy(parseStatus(status), limit, page);
        }

        List<Object> tasks = new ArrayList<>();
        while (iter.hasNext()) {
            HugeTask<Object> hugeTask = iter.next();
            String input = hugeTask.input();
            if (input == null) {
                continue;
            }

            Map<String, Object> map = JsonUtil.fromJson(input, Map.class);
            if (map.containsKey("inner.job_id")) {
                tasks.add(iter.next().asMap(false));
            }
        }
        if (limit != NO_LIMIT && tasks.size() > limit) {
            tasks = tasks.subList(0, (int) limit);
        }

        if (page == null) {
            return Maps.of("tasks", tasks);
        } else {
            return Maps.of("tasks", tasks, "page", PageInfo.pageInfo(iter));
        }
    }

    private static TaskStatus parseStatus(String status) {
        try {
            return TaskStatus.valueOf(status);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Status value must be in %s, but got '%s'",
                    Arrays.asList(TaskStatus.values()), status));
        }
    }
}
