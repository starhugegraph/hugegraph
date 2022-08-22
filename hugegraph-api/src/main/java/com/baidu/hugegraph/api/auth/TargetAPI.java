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

package com.baidu.hugegraph.api.auth;

import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphspaces/{graphspace}/auth/targets")
@Singleton
public class TargetAPI extends API {

    private static final HugeGraphLogger LOGGER
            = Log.getLogger(RestServer.class);
    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonTarget jsonTarget) {
        LOGGER.logCustomDebug("Graph space [{}] create target: {}",
                              RestServer.EXECUTOR, graphSpace, jsonTarget);
        checkCreatingBody(jsonTarget);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeTarget target = jsonTarget.build(graphSpace);
        AuthManager authManager = manager.authManager();
        target.id(authManager.createTarget(graphSpace, target, false));
        return manager.serializer().writeAuthElement(target);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("id") String id,
                         JsonTarget jsonTarget) {
        LOGGER.logCustomDebug("Graph space [{}] update target: {}",
                              RestServer.EXECUTOR, graphSpace, jsonTarget);
        checkUpdatingBody(jsonTarget);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeTarget target;
        AuthManager authManager = manager.authManager();
        try {
            target = authManager.getTarget(graphSpace, UserAPI.parseId(id),
                                           false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
        target = jsonTarget.build(target);
        target = authManager.updateTarget(graphSpace, target, false);
        return manager.serializer().writeAuthElement(target);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOGGER.logCustomDebug("Graph space [{}] list targets",
                              RestServer.EXECUTOR, graphSpace);

        AuthManager authManager = manager.authManager();
        List<HugeTarget> targets = authManager.listAllTargets(graphSpace,
                                                              limit, false);
        return manager.serializer().writeAuthElements("targets", targets);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOGGER.logCustomDebug("Graph space [{}] get target: {}",
                              RestServer.EXECUTOR, graphSpace, id);

        AuthManager authManager = manager.authManager();
        HugeTarget target = authManager.getTarget(graphSpace,
                            UserAPI.parseId(id), false);
        return manager.serializer().writeAuthElement(target);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"space"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOGGER.logCustomDebug("Graph space [{}] delete target: {}",
                              RestServer.EXECUTOR, graphSpace, id);

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteTarget(graphSpace, UserAPI.parseId(id), false);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "target_creator",
                                   "target_create", "target_update"})
    private static class JsonTarget implements Checkable {

        @JsonProperty("target_name")
        private String name;
        @JsonProperty("target_graph")
        private String graph;
        @JsonProperty("target_resources") // error when List<HugeResource>
        private List<Map<String, Object>> resources;

        public HugeTarget build(HugeTarget target) {
            E.checkArgument(this.name == null ||
                            target.name().equals(this.name),
                            "The name of target can't be updated");
            E.checkArgument(this.graph == null ||
                            target.graph().equals(this.graph),
                            "The graph of target can't be updated");
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            return target;
        }

        public HugeTarget build(String graphSpace) {
            HugeTarget target = new HugeTarget(this.name, graphSpace,
                                               this.graph);
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            return target;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of target can't be null");
            E.checkArgumentNotNull(this.graph,
                                   "The graph of target can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(this.resources != null,
                            "Expect one of target url/resources");

        }
    }
}
