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

import com.baidu.hugegraph.api.filter.StatusFilter;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeGroup;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.space.GraphSpace;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.StringEncoding;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("auth/managers")
@Singleton
public class SpaceManagerAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    public static final String ALL_GRAPH_SPACES = "*";
    public static final String DEFAULT_SPACE_GROUP_KEY = "DEFAULT_GROUP";
    public static final String DEFAULT_SPACE_TARGET_KEY = "DEFAULT_TARGET";

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String createSpaceManager(@Context GraphManager manager,
                                     JsonManager jsonManager) {
        LOG.debug("Create graph space manager: {}", jsonManager);
        E.checkArgument(jsonManager.type == HugePermission.SPACE ||
                        jsonManager.type == HugePermission.OP,
                        "The type could be 'SPACE' or 'OP'");

        GraphSpace graphSpace = manager.graphSpace(jsonManager.graphSpace);
        E.checkArgument(graphSpace != null,
                        "The graph space is not exist");

        AuthManager authManager = manager.authManager();
        HugeUser hugeUser = authManager.findUser(jsonManager.user, false);
        E.checkArgument(hugeUser != null,
                        "The user is not exist");

        if (jsonManager.type == HugePermission.SPACE) {
            authManager.createSpaceManager(jsonManager.graphSpace,
                                           hugeUser.name());
        } else {
            authManager.createSpaceOpManager(jsonManager.graphSpace,
                                             hugeUser.name());

        }

        return null;
    }

    private static class JsonManager implements Checkable {

        @JsonProperty("user")
        private String user;
        @JsonProperty("type")
        private HugePermission type;
        @JsonProperty("graphspace")
        private String graphSpace;

        @Override
        public void checkCreate(boolean isBatch) {}

        @Override
        public void checkUpdate() {}
    }
}
