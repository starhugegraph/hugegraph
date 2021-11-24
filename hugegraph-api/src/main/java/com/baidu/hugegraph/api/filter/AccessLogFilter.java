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


package com.baidu.hugegraph.api.filter;


import com.baidu.hugegraph.backend.store.raft.rpc.ListPeersProcessor;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import javax.ws.rs.NameBinding;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * AccessLogFilter performs as a middleware that log all the http accesses.
 * @since 2021-11-24
 */
@Provider
public class AccessLogFilter {

    private static final Logger LOG = Log.logger(ListPeersProcessor.class);

    @NameBinding
    @Target({ ElementType.TYPE, ElementType.METHOD })
    @Retention(value = RetentionPolicy.RUNTIME)
    public @interface AccessLog { }

    /**
     * use inner logger class to implement response filter
     */
    @AccessLog
    public static class AccessLogger implements ContainerResponseFilter {

        /**
         * Use filter to log request info
         * @param containerRequestContext requestContext
         * @param containerResponseContext responseContext
         */
        @Override
        public void filter(ContainerRequestContext containerRequestContext, ContainerResponseContext containerResponseContext) throws IOException {
            // grab corresponding request / response info from context;
            String method = containerRequestContext.getRequest().getMethod();
            String path = containerRequestContext.getUriInfo().getPath();

            int code = containerResponseContext.getStatus();

            // build log string
            // TODO by Scorpiour: Use Formatted log template to replace hard-written string when logging
            LOG.info("{} {} - {}", method, path, code);
        }
    }
}
