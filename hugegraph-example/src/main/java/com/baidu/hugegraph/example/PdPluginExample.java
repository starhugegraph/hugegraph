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

package com.baidu.hugegraph.example;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.IServiceRegister;
import com.baidu.hugegraph.RegisterPlugin;
import com.baidu.hugegraph.registerimpl.SampleRegister;
import com.baidu.hugegraph.util.Log;

public class PdPluginExample {

    private static final Logger LOG = Log.logger(Example1.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Example1 start!");

        try {
            IServiceRegister register = new SampleRegister();
            String serviceId = RegisterPlugin.getInstance().loadPlugin(register, "PdExample");
            System.out.println("====> Service id is " + serviceId);
        } catch (Exception e) {
            System.out.println(e);
        }


        Thread.sleep(60);
    }
}
