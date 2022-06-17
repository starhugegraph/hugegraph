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

package com.baidu.hugegraph.api.gremlin;

import java.util.Map;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.exception.HugeGremlinException;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Path("gremlin")
@Singleton
public class GremlinAPI extends GremlinQueryAPI {

    private static final Histogram gremlinInputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");
    private static final Histogram gremlinOutputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-output");
    private static final Set<String> FORBIDDEN_REQUEST_EXCEPTIONS =
            ImmutableSet.of("java.lang.SecurityException",
                            "javax.ws.rs.ForbiddenException");
    private static final Set<String> BAD_REQUEST_EXCEPTIONS = ImmutableSet.of(
            "java.lang.IllegalArgumentException",
            "java.util.concurrent.TimeoutException",
            "groovy.lang.",
            "org.codehaus.",
            "com.baidu.hugegraph."
    );

    @Context
    private javax.inject.Provider<HugeConfig> configProvider;

    private GremlinClient client;

    public GremlinClient client() {
        if (this.client != null) {
            return this.client;
        }
        HugeConfig config = this.configProvider.get();
        String url = config.get(ServerOptions.GREMLIN_SERVER_URL);
        int timeout = config.get(ServerOptions.GREMLIN_SERVER_TIMEOUT) * 1000;
        int maxRoutes = config.get(ServerOptions.GREMLIN_SERVER_MAX_ROUTE);
        this.client = new GremlinClient(url, timeout, maxRoutes, maxRoutes);
        return this.client;
    }

    @POST
    @Timed
    @Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response post(@Context HugeConfig conf,
                         @Context HttpHeaders headers,
                         String request) {
        /* The following code is reserved for forwarding request */
        // context.getRequestDispatcher(location).forward(request, response);
        // return Response.seeOther(UriBuilder.fromUri(location).build())
        // .build();
        // Response.temporaryRedirect(UriBuilder.fromUri(location).build())
        // .build();
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        Response response = this.client().doPostRequest(auth, request);
        gremlinInputHistogram.update(request.length());
        gremlinOutputHistogram.update(response.getLength());
        return transformResponseIfNeeded(response);
    }
}
