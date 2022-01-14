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

package com.baidu.hugegraph.api.space;

import static com.baidu.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_NAME;

import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphspaces")
@Singleton
public class GraphSpaceAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String GRAPH_SPACE_ACTION = "action";
    private static final String UPDATE = "update";
    private static final String GRAPH_SPACE_ACTION_CLEAR = "clear";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context GraphManager manager,
                       @Context SecurityContext sc) {
        LOG.debug("List all graph spaces");

        Set<String> spaces = manager.graphSpaces();
        return ImmutableMap.of("graphSpaces", spaces);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @PathParam("name") String name) {
        LOG.debug("Get graph space by name '{}'", name);

        return manager.serializer().writeGraphSpace(space(manager, name));
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String create(@Context GraphManager manager,
                         JsonGraphSpace jsonGraphSpace) {
        LOG.debug("Create graph space: '{}'", jsonGraphSpace);

        E.checkArgument(!DEFAULT_GRAPH_SPACE_NAME.equals(jsonGraphSpace),
                        "Can't create namespace with name '%s'",
                        DEFAULT_GRAPH_SPACE_NAME);

        jsonGraphSpace.checkCreate(false);

        GraphSpace exist = manager.graphSpace(jsonGraphSpace.name);
        E.checkArgument(exist == null, "The graph space '%s' has existed",
                        jsonGraphSpace.name);
        GraphSpace space = manager.createGraphSpace(
                           jsonGraphSpace.toGraphSpace());
        return manager.serializer().writeGraphSpace(space);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public Map<String, Object> manage(@Context GraphManager manager,
                                      @PathParam("name") String name,
                                      Map<String, Object> actionMap) {
        LOG.debug("Manage graph space with action {}", actionMap);

        E.checkArgument(actionMap != null && actionMap.size() == 2 &&
                        actionMap.containsKey(GRAPH_SPACE_ACTION),
                        "Invalid request body '%s'", actionMap);
        Object value = actionMap.get(GRAPH_SPACE_ACTION);
        E.checkArgument(value instanceof String,
                        "Invalid action type '%s', must be string",
                        value.getClass());
        String action = (String) value;
        switch (action) {
            case "update":
                LOG.debug("Update graph space: '{}'", name);

                E.checkArgument(actionMap.containsKey(UPDATE),
                                "Please pass '%s' for graph space update",
                                UPDATE);
                value = actionMap.get(UPDATE);
                E.checkArgument(value instanceof Map,
                                "The '%s' must be map, but got %s",
                                UPDATE, value.getClass());
                @SuppressWarnings("unchecked")
                Map<String, Object> graphSpaceMap = (Map) value;
                String gsName = (String) graphSpaceMap.get("name");
                E.checkArgument(gsName.equals(name),
                                "Different name in update body with in path");
                GraphSpace exist = manager.graphSpace(name);
                if (exist == null) {
                    throw new NotFoundException("Can't find graph space with name '%s'",
                                                gsName);
                }

                String description = (String) graphSpaceMap.get("description");
                if (description != null &&
                    Strings.isEmpty(description)) {
                    exist.description(description);
                }

                int maxGraphNumber =
                        (int) graphSpaceMap.get("max_graph_number");
                if (maxGraphNumber != 0) {
                    exist.maxGraphNumber(maxGraphNumber);
                }
                int maxRoleNumber = (int) graphSpaceMap.get("max_role_number");
                if (maxRoleNumber != 0) {
                    exist.maxRoleNumber(maxRoleNumber);
                }

                int cpuLimit = (int) graphSpaceMap.get("cpu_limit");
                if (cpuLimit != 0) {
                    exist.cpuLimit(cpuLimit);
                }
                int memoryLimit = (int) graphSpaceMap.get("memory_limit");
                if (memoryLimit != 0) {
                    exist.memoryLimit(memoryLimit);
                }
                int storageLimit = (int) graphSpaceMap.get("storage_limit");
                if (storageLimit != 0) {
                    exist.storageLimit = storageLimit;
                }

                String oltpNamespace =
                        (String) graphSpaceMap.get("oltp_namespace");
                if (oltpNamespace != null &&
                    !Strings.isEmpty(oltpNamespace)) {
                    exist.oltpNamespace(oltpNamespace);
                }
                String olapNamespace =
                        (String) graphSpaceMap.get("olap_namespace");
                if (olapNamespace != null &&
                    !Strings.isEmpty(olapNamespace)) {
                    exist.olapNamespace(olapNamespace);
                }
                String storageNamespace =
                        (String) graphSpaceMap.get("storage_namespace");
                if (storageNamespace != null &&
                    !Strings.isEmpty(storageNamespace)) {
                    exist.storageNamespace(storageNamespace);
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> configs = (Map) graphSpaceMap.get("configs");
                if (configs != null && !configs.isEmpty()) {
                    exist.configs(configs);
                }
                GraphSpace space = manager.createGraphSpace(exist);
                return space.info();
            case GRAPH_SPACE_ACTION_CLEAR:
                LOG.debug("Clear graph space: '{}'", name);

                manager.clearGraphSpace(name);
                return ImmutableMap.of(name, "cleared");
            default:
                throw new AssertionError(String.format("Invalid action: '%s'",
                                                       action));
        }
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @PathParam("name") String name) {
        LOG.debug("Remove graph space by name '{}'", name);

        manager.dropGraphSpace(name);
    }


    private static class JsonGraphSpace implements Checkable {

        @JsonProperty("name")
        public String name;
        @JsonProperty("description")
        public String description;

        @JsonProperty("cpu_limit")
        public int cpuLimit;
        @JsonProperty("memory_limit")
        public int memoryLimit;
        @JsonProperty("storage_limit")
        public int storageLimit;

        @JsonProperty("oltp_namespace")
        public String oltpNamespace;
        @JsonProperty("olap_namespace")
        public String olapNamespace;
        @JsonProperty("storage_namespace")
        public String storageNamespace;

        @JsonProperty("max_graph_number")
        public int maxGraphNumber;
        @JsonProperty("max_role_number")
        public int maxRoleNumber;

        @JsonProperty("auth")
        public boolean auth = false;

        @JsonProperty("configs")
        public Map<String, Object> configs;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.name != null &&
                            !StringUtils.isEmpty(this.name),
                            "The name of graph space can't be null or empty");

            E.checkArgument(this.maxGraphNumber > 0,
                            "The max graph number must > 0");
            E.checkArgument(this.maxRoleNumber > 0,
                            "The max role number must > 0");

            E.checkArgument(this.cpuLimit > 0,
                            "The cpu limit must be > 0, but got: %s",
                            this.cpuLimit);
            E.checkArgument(this.memoryLimit > 0,
                            "The memory limit must be > 0, but got: %s",
                            this.memoryLimit);
            E.checkArgument(this.storageLimit > 0,
                            "The storage limit must be > 0, but got: %s",
                            this.storageLimit);

            E.checkArgument(this.oltpNamespace != null &&
                            !StringUtils.isEmpty(this.oltpNamespace),
                            "The oltp graph space can't be null or empty");
            E.checkArgument(this.olapNamespace != null &&
                            !StringUtils.isEmpty(this.olapNamespace),
                            "The olap graph space can't be null or empty");
            E.checkArgument(this.storageNamespace != null &&
                            !StringUtils.isEmpty(this.storageNamespace),
                            "The storage graph space can't be null or empty");
        }

        public GraphSpace toGraphSpace() {
            GraphSpace graphSpace = new GraphSpace(this.name, this.description,
                                                   this.cpuLimit,
                                                   this.memoryLimit,
                                                   this.storageLimit,
                                                   this.maxGraphNumber,
                                                   this.maxRoleNumber,
                                                   this.auth,
                                                   this.configs);
            graphSpace.oltpNamespace(this.oltpNamespace);
            graphSpace.olapNamespace(this.olapNamespace);
            graphSpace.storageNamespace(this.storageNamespace);

            graphSpace.configs(this.configs);

            return graphSpace;
        }

        public String toString() {
            return String.format("JsonGraphSpace{name=%s, description=%s, " +
                                 "cpuLimit=%s, memoryLimit=%s, " +
                                 "storageLimit=%s, oltpNamespace=%s" +
                                 "olapNamespace=%s, storageNamespace=%s" +
                                 "maxGraphNumber=%s, maxRoleNumber=%s, " +
                                 "configs=%s}", this.name, this.description,
                                 this.cpuLimit, this.memoryLimit,
                                 this.storageLimit, this.oltpNamespace,
                                 this.olapNamespace, this.storageLimit,
                                 this.maxGraphNumber, this.maxRoleNumber,
                                 this.configs);
        }
    }
}
