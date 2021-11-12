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
package com.baidu.hugegraph.virtual_graph;

import org.apache.commons.pool2.ObjectPool;

import java.util.ArrayList;
import java.util.HashMap;

import com.baidu.hugegraph.backend.id.Id;

public class VirtualGraph {
    private static int InitVertexCap = 10000;
    private HashMap<Id, VirtualVertex> vertexMap;
    private HashMap<Id, VirtualEdge> edgeMap;
    private ObjectPool<VirtualVertex> vertexPool;
    private ObjectPool<VirtualEdge> edgePool;

    public VirtualGraph() {

    }

    public void init(){
        this.vertexMap = new HashMap<Id, VirtualVertex>(InitVertexCap);
        this.edgeMap = new HashMap<Id, VirtualEdge>(InitVertexCap);
    }

    public VirtualVertex queryVertexById(Id vId) {
        VirtualVertex vertex = this.vertexMap.get(vId);
        return vertex;
    }

    public VirtualEdge queryEdgeById(Id eId) {
        VirtualEdge edge = this.edgeMap.get(eId);
        return edge;
    }

    public ArrayList<VirtualEdge> queryEdgesByVertex(Id vId) {
        return this.vertexMap.get(vId).edges;
    }
}