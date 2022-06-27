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

package com.baidu.hugegraph.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import com.alipay.remoting.util.ConcurrentHashSet;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.job.EphemeralJob;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.meta.lock.LockResult;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask.P;
import com.baidu.hugegraph.task.TaskCallable.SysTaskCallable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * EtcdTaskScheduler handle the distributed task by etcd
 * @author Scorpiour
 * @since 2022-01-01
 */
public class EtcdTaskScheduler extends TaskScheduler {

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private static final String TASK_NAME_PREFIX = "etcd-task-worker";

    private final ExecutorService producer = ExecutorUtil.newFixedThreadPool(1, EtcdTaskScheduler.class.getName() + "-task-producer");

    private final Map<TaskPriority, ExecutorService> taskExecutorMap;

    private final ExecutorService backupForLoadTaskExecutor;
    private final ExecutorService taskDBExecutor;

    // Mark if a task has been visited, filter duplicate events
    private final Set<String> visitedTasks = new ConcurrentHashSet<>();

    // Count the task processed by current scheduler
    private final Set<Id> processedTasks = new ConcurrentHashSet<>();

    // Mark if a task is processed by current node
    private final Map<Id, HugeTask<?>> taskMap = new ConcurrentHashMap<>();

    // Mark if Scheduler is closing
    private volatile boolean closing = false;

    /**
     * State table to switch task status under etcd
     */
    private static final Map<TaskStatus, ImmutableSet<TaskStatus>> TASK_STATUS_MAP = 
        new ImmutableMap.Builder<TaskStatus, ImmutableSet<TaskStatus>>() 
            .put(TaskStatus.UNKNOWN,
                ImmutableSet.of())
            .put(TaskStatus.NEW,
                ImmutableSet.of(
                    TaskStatus.NEW, TaskStatus.SCHEDULING))
            .put(TaskStatus.SCHEDULING,
                ImmutableSet.of(
                    TaskStatus.SCHEDULING, TaskStatus.SCHEDULED, TaskStatus.CANCELLING, TaskStatus.FAILED))
            .put(TaskStatus.SCHEDULED,
                ImmutableSet.of(
                    TaskStatus.SCHEDULED, TaskStatus.QUEUED, TaskStatus.CANCELLING, TaskStatus.RESTORING))
            .put(TaskStatus.QUEUED,
                ImmutableSet.of(
                    TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.HANGING, TaskStatus.CANCELLING, TaskStatus.RESTORING))
            .put(TaskStatus.RUNNING,
                ImmutableSet.of(
                    TaskStatus.RUNNING, TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.RESTORING, TaskStatus.CANCELLING))
            .put(TaskStatus.CANCELLING,
                ImmutableSet.of(
                    TaskStatus.CANCELLING, TaskStatus.CANCELLED))
            .put(TaskStatus.CANCELLED,
                ImmutableSet.of(
                    TaskStatus.CANCELLED))
            .put(TaskStatus.SUCCESS,
                ImmutableSet.of(
                    TaskStatus.SUCCESS))
            .put(TaskStatus.FAILED,
                ImmutableSet.of(
                    TaskStatus.FAILED))
            .put(TaskStatus.HANGING,
                ImmutableSet.of(
                    TaskStatus.HANGING, TaskStatus.RESTORING
                ))
            .put(TaskStatus.RESTORING,
                ImmutableSet.of(
                    TaskStatus.RESTORING, TaskStatus.QUEUED))
            .build();


    public EtcdTaskScheduler(
        HugeGraphParams graph,
        ExecutorService backupForLoadTaskExecutor,
        ExecutorService taskDBExecutor,
        ExecutorService serverInfoDbExecutor,
        TaskPriority maxDepth
    ) {
        super(graph, serverInfoDbExecutor);
        this.taskExecutorMap = new ConcurrentHashMap<>();

        for (int i = 0; i <= maxDepth.getValue(); i++) {
            TaskPriority priority = TaskPriority.fromValue(i);
            int poolSize = Math.max(1, CPU_COUNT - i);
            String poolName = TASK_NAME_PREFIX + "-" + priority.name();
            taskExecutorMap.putIfAbsent(priority, ExecutorUtil.newFixedThreadPool(poolSize, poolName));
        }

        this.backupForLoadTaskExecutor = backupForLoadTaskExecutor;
        this.taskDBExecutor = taskDBExecutor;

        this.eventListener =  this.listenChanges();
        MetaManager manager = MetaManager.instance();
        for (int i = 0; i <= maxDepth.getValue(); i++) {
            TaskPriority priority = TaskPriority.fromValue(i);
            manager.listenTaskAdded(graph.graph().graphSpace(), graph.name(),
                                    priority, this::taskEventHandler);
        }
    }

    @Override
    public int pendingTasks() {
        int count = 0;
        MetaManager manager = MetaManager.instance();
        for(TaskStatus status : TaskStatus.PENDING_STATUSES) {
            count +=  manager.countTaskByStatus(this.graphSpace(),
                                                this.graphName, status);
        }
        return count;
    }

    private static <V> LockResult lockTask(String graphSpace,
                                           String graphName,
                                           HugeTask<V> task) {
        if (task.lockResult() != null) {
            return task.lockResult();
        }
        return MetaManager.instance().lockTask(graphSpace, graphName, task);
    }

    private static <V> void unlockTask(String graphSpace,
                                       String graphName,
                                       HugeTask<V> task) {
        if (task.lockResult() == null) {
            return;
        }
        MetaManager manager = MetaManager.instance();
        manager.unlockTask(graphSpace, graphName, task);
    }

    private <V> void restoreTaskFromStatus(TaskStatus status) {
        MetaManager manager = MetaManager.instance();
        List<HugeTask<V>> taskList = manager.listTasksByStatus(this.graphSpace(),
                                                               this.graphName,
                                                               status);
        for(HugeTask<V> task : taskList) {
            if (null == task) {
                continue;
            }
            
            LockResult result = EtcdTaskScheduler.lockTask(this.graphSpace(),
                                                           this.graphName, task);
            if (result.lockSuccess()) {
                task.lockResult(result);
                this.visitedTasks.add(task.id().asString());
                ExecutorService executor = null;

                try {
                    TaskStatus current =
                            manager.getTaskStatus(this.graphSpace(),
                                                  this.graphName, task);
                    if (current != status) {
                        EtcdTaskScheduler.unlockTask(this.graphSpace(),
                                                     this.graphName, task);
                        continue;
                    }
                    executor = this.pickExecutor(task.priority());
                    if (null == executor) {
                        EtcdTaskScheduler.unlockTask(this.graphSpace(),
                                                     this.graphName, task);
                        continue;
                    }

                    // process cancelling first
                    if (current == TaskStatus.CANCELLING) {
                        EtcdTaskScheduler.updateTaskStatus(this.graphSpace(),
                                                           this.graphName, task,
                                                           TaskStatus.CANCELLED);
                        EtcdTaskScheduler.unlockTask(this.graphSpace(),
                                                     this.graphName, task);
                        continue;
                    }

                    // pickup others
                    EtcdTaskScheduler.updateTaskStatus(this.graphSpace(),
                                                       this.graphName, task,
                                                       TaskStatus.RESTORING);
                    // Attach callable info
                    TaskCallable<?> callable = task.callable();
                    // Attach graph info
                    callable.graph(this.graph());
                    if (callable instanceof SysTaskCallable) {
                        ((SysTaskCallable<?>) callable).params(this.graph);
                    }
                    // Update status to queued
                    EtcdTaskScheduler.updateTaskStatus(this.graphSpace(),
                                                       this.graphName, task,
                                                       TaskStatus.QUEUED);
                    // Update local cache
                    this.taskMap.put(task.id(), task);
                    this.visitedTasks.add(task.id().asString());
                    task.scheduler(this);
                    // Use fake context if missing for compatible
                    if (Strings.isNullOrEmpty(task.context())) {
                        task.overwriteContext(TaskManager.getContext(true));
                    }
                    // Update retry
                    EtcdTaskScheduler.updateTaskRetry(this.graphSpace(),
                                                      this.graphName, task);
                } catch (Exception e) {
                    EtcdTaskScheduler.unlockTask(this.graphSpace(),
                                                 this.graphName, task);
                    throw e;
                }
                executor.submit(new TaskRunner<>(task, this.graph,
                                                 this.processedTasks));
            }
        }
    }

    private <V> void restorePendingTasks() {
        this.restoreTaskFromStatus(TaskStatus.HANGING);
    }

    /**
     * headless tasks are those in status of queuing / running / cancelling but not locked.
     * When node is down, the lease of the task will be lost, that means we should pick them up
     * and schedule again
     * 
     * @param <V>
     */
    private <V> void restoreHeadlessTasks() {
            for (TaskStatus status: TaskStatus.RESTORABLE_STATUSES) {
               this.restoreTaskFromStatus(status);
            }
    }

    @Override
    public <V> void restoreTasks() {
        this.restorePendingTasks();
        this.restoreHeadlessTasks();
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        if (task.status() == TaskStatus.NEW && Strings.isNullOrEmpty(task.context())) {
            String currentContext = TaskManager.getContext();
            if (!Strings.isNullOrEmpty(currentContext)) {
                task.context(TaskManager.getContext());
            }
        }

        if (task.callable() instanceof EphemeralJob) {
            task.status(TaskStatus.QUEUED);
            return this.submitEphemeralTask(task);
        }
        
        return this.submitTask(task);
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        try {
            EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), this.graphName,
                                               task, TaskStatus.CANCELLING);
            if (this.processedTasks.contains(task.id())) {
                task.cancel(true);
            }
            EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), this.graphName,
                                               task, task.status());
        } catch (Throwable e) {
            LOGGER.logCriticalError(new Exception(e), "cancel task failed!");
        }
    }

    private <V> Id saveWithId(HugeTask<V> task) {
        
        task.scheduler(this);
        E.checkArgumentNotNull(task, "Task can't be null");
        HugeVertex v = this.call(() -> {
            // Construct vertex from task
            HugeVertex vertex = this.tx().constructVertex(task);
            // Delete index of old vertex to avoid stale index
            this.tx().deleteIndex(vertex);
            // Add or update task info to backend store
            return this.tx().addVertex(vertex);
        });
        return v.id();
    }

    @Override
    public <V> void save(HugeTask<V> task) {
        this.saveWithId(task);
    }

    @Override
    public <V> HugeTask<V> delete(Id id, boolean force) {
        MetaManager manager = MetaManager.instance();
        HugeTask<V> task = manager.getTask(this.graphSpace(), this.graphName, id);
        // Be aware of the order: remove persistance first, then remove memory cache
        if (null != task) {
            manager.deleteTask(this.graphSpace(), this.graphName, task);
            try {
                this.call(() -> {
                    Iterator<Vertex> vertices = this.tx().queryVertices(id);
                    HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
                    if (vertex == null) {
                        return null;
                    }
                    HugeTask<V> result = HugeTask.fromVertex(vertex);
                    E.checkState(force || result.completed(),
                                "Can't delete incomplete task '%s' in status %s",
                                id, result.status());
                    this.tx().removeVertex(vertex);
                    return result;
                }).get();
            } catch (InterruptedException | CancellationException | ExecutionException ie) {
                LOGGER.logCriticalError(ie, "Concurrent Exception captured when delete task");
            }
        }
        this.taskMap.remove(id);
        return task;
    }

    @Override 
    public void flushAllTask() {
        MetaManager.instance().flushAllTasks(this.graphSpace(), this.graphName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> HugeTask<V> task(Id id) {
        /** Here we have three scenarios:
         * 1. The task is handled by current node
         * 2. The task is handled by other node, but having snapshot here
         * 3. No any info here
         * As a result, we should distinguish them and processed separately
         * for case 1, we grab the info locally directly
         * for case 2, we grab the basic info and update related info, like status & progress
         * for case 3, we load everything from etcd and cached the snapshot locally for further use 
        */
        HugeTask<V> potential = (HugeTask<V>)this.taskMap.get(id);
        if (potential != null && TaskStatus.COMPLETED_STATUSES.contains(potential.status()) ) {
            return potential;
        } else {
            MetaManager manager = MetaManager.instance();
            potential = manager.getTask(this.graphSpace(), this.graphName, id);
        }

        // attach task stored info
        HugeTask<V> persisted = this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return HugeTask.fromVertex(vertex);
        });

        // combine status
        if (null != persisted) {
            if (null != potential) {
                persisted.status(potential.status());
                persisted.priority(potential.priority());
                persisted.progress(potential.progress());
                potential.overwriteResult(persisted.result());
            }
        } else {
            persisted = potential;
        }

        if (null != persisted) {
            this.taskMap.put(id, persisted);
            persisted.callable().task(persisted);
            persisted.callable().graph(this.graph());
            if (TaskStatus.COMPLETED_STATUSES.contains(persisted.status())) {
                EtcdTaskScheduler.updateTaskStatus(graphSpace, graphName, persisted, persisted.status());
            }
        }

        return persisted;
    }

    /**
     * In etcd scheduler, we need to grab task actual properties from etcd
     */
    @Override
    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
        MetaManager manager = MetaManager.instance();
        List<HugeTask<V>> allTasks = manager.listTasks(this.graphSpace(), this.graphName, ids);

        Iterator<HugeTask<V>> persistedTasks = this.queryTask(ids);
        Map<Id, HugeTask<V>> persistedMap = new HashMap<>();
        while(persistedTasks.hasNext()) {
            HugeTask<V> task = persistedTasks.next();
            persistedMap.put(task.id(), task);
        }

        allTasks.stream().forEach((task) -> {
            // Attach callable info
            TaskCallable<?> callable = task.callable();
            // Attach graph info
            callable.graph(this.graph());
            if (callable instanceof SysTaskCallable) {
                ((SysTaskCallable<?>)callable).params(this.graph);
            }

            HugeTask<V> persisted = persistedMap.get(task.id());
            if (null != persisted) {
                if (TaskStatus.COMPLETED_STATUSES.contains(persisted.status())) {
                    task.overwriteStatus(persisted.status());
                }
                task.overwriteResult(persisted.result());
            }
        });


        Iterator<HugeTask<V>> iterator = allTasks.iterator();
        return iterator;
    }

    private int pendingTaskCount() {
        MetaManager manager = MetaManager.instance();
        int counter = 0;
        for(TaskStatus status: TaskStatus.PENDING_STATUSES) {
            counter += manager.countTaskByStatus(graphSpace, graphName, status);
        }
        return counter;
    }

    private int finalizedTaskCount() {
        MetaManager manager = MetaManager.instance();
        int counter = 0;
        for(TaskStatus status: TaskStatus.COMPLETED_STATUSES) {
            counter += manager.countTaskByStatus(graphSpace, graphName, status);
        }
        return counter;
    }

    /**
     * find task from storage first, then attach related info
     */
    @Override
    public <V> Iterator<HugeTask<V>> tasks(TaskStatus status, long limit, String page) {

        Iterator<HugeTask<V>> persistedTasks = status == null
            ? this.queryTask(ImmutableMap.of(), limit, page)
            : this.queryTask(P.STATUS, status.code(), limit, page);

        // bulid task map to attach status & properties
        Map<Id, HugeTask<V>> persistedMap = new HashMap<>();

        while(persistedTasks.hasNext()) {
            HugeTask<V> task = persistedTasks.next();
            persistedMap.put(task.id(), task);
        }

        List<Id> taskIdList = persistedMap.keySet().stream().collect(Collectors.toList());

        MetaManager manager = MetaManager.instance();

        List<HugeTask<V>> tasks = manager.listTasks(graphSpace, graphName, taskIdList);

        tasks.stream().forEach((task) -> {
            TaskCallable<?> callable = task.callable();
            // Attach graph info
            callable.graph(this.graph());
            if (callable instanceof SysTaskCallable) {
                ((SysTaskCallable<?>)callable).params(this.graph);
            }
            task.scheduler(this);
            task.status(manager.getTaskStatus(this.graphSpace(), this.graphName, task));
            HugeTask<V> persisted = persistedMap.get(task.id());
            if (null != persisted) {
                task.overwriteResult(persisted.result());
            }
        });
        
        return tasks.iterator();
    }

    @Override
    public boolean close() {
        this.closing = true;
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
        // Mark all tasks that has not been run into pending
        this.hangTasks();
        if (!this.taskDBExecutor.isShutdown()) {
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
            });
        }
        return true;
    }

    /**
     * When scheduler is going to close, the tasks that has not been 
     */
    private void hangTasks() {
        for(Map.Entry<Id, HugeTask<?>> entry : this.taskMap.entrySet()) {
            HugeTask<?> task = entry.getValue();
            if (null == task) {
                continue;
            }
            synchronized(task) {
                if (!TaskStatus.UNBREAKABLE_STATUSES.contains(task.status())) {
                    task.status(TaskStatus.HANGING);
                }
            }
        }

    }

    private <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds,
                                                   long intervalMs)
                                                   throws TimeoutException {
        long passes = seconds * 1000 / intervalMs;
        HugeTask<V> task = null;
        for (long pass = 0;; pass++) {
            try {
                task = this.task(id);
            } catch (NotFoundException e) {
                if (task != null && task.completed()) {
                    assert task.id().asLong() < 0L : task.id();
                    sleep(intervalMs);
                    return task;
                }
                throw e;
            }
            if (task.completed()) {
                // Wait for task result being set after status is completed
                sleep(intervalMs);
                return task;
            }
            if (pass >= passes) {
                break;
            }
            sleep(intervalMs);
        }
        throw new TimeoutException(String.format(
                  "Task '%s' was not completed in %s seconds", id, seconds));
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds) throws TimeoutException {
        return this.waitUntilTaskCompleted(id, seconds, QUERY_INTERVAL);
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id) throws TimeoutException {
        long timeout = this.graph.configuration()
                                 .get(CoreOptions.TASK_WAIT_TIMEOUT);
        return this.waitUntilTaskCompleted(id, timeout, 1L);
    }

    private long incompleteTaskCount() {
        long incompleteTaskCount = 0;
        MetaManager manager = MetaManager.instance();
        for(Id taskId : this.processedTasks) {
            TaskStatus status = manager.getTaskStatus(this.graphSpace, this.graphName, taskId);
            if (!TaskStatus.COMPLETED_STATUSES.contains(status)) {
                incompleteTaskCount++;
            }
        }
        return incompleteTaskCount;
    }

    @Override
    public void waitUntilAllTasksCompleted(long seconds) throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        long taskSize = 0;
        for (long pass = 0;; pass++) {
            taskSize = this.incompleteTaskCount();
            if (taskSize == 0) {
                sleep(QUERY_INTERVAL);
                return;
            }
            if (pass >= passes) {
                break;
            }
            sleep(QUERY_INTERVAL);
        }
        throw new TimeoutException(String.format(
                  "There are still %s incomplete tasks after %s seconds",
                  taskSize, seconds));
        
    }

    private <V> Future<?> submitEphemeralTask(HugeTask<V> task) {
        assert !this.taskMap.containsKey(task.id()) : task;

        ExecutorService executorService = this.pickExecutor(task.priority());
        if (null == executorService) {
            return this.backupForLoadTaskExecutor.submit(task);   
        }

        int size = this.pendingTaskCount() + 1;
        E.checkArgument(size < MAX_PENDING_TASKS,
            "Pending tasks size %s has exceeded the max limit %s",
            size + 1, MAX_PENDING_TASKS);
        task.scheduler(this);
        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());
        if (callable instanceof SysTaskCallable) {
            ((SysTaskCallable<V>)callable).params(this.graph);
        }

        this.taskMap.put(task.id(), task);
        if (this.graph().mode().loading()) {
            return this.backupForLoadTaskExecutor.submit(task);   
        }
        return executorService.submit(task);
    }

    private <V> Future<?> submitTask(HugeTask<V> task) {
        task.scheduler(this);
        task.status(TaskStatus.SCHEDULING);

        // Save task first
        this.saveWithId(task);
        // Submit to etcd
        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());
        if (callable instanceof SysTaskCallable) {
            ((SysTaskCallable<V>)callable).params(this.graph);
        }

        return this.producer.submit(new TaskCreator<V>(task, this.graph));
    }

    @Override
    protected <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    
    @Override
    protected <V> V call(Callable<V> callable) {
        return super.call(callable, this.taskDBExecutor);
    }

    @Override
    protected void taskDone(HugeTask<?> task) {
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure task schema create after system info initialized
            if (storeEvents.contains(event.name())) {
                this.call(() -> this.tx().initSchema());
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private static boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ignored) {
            // Ignore InterruptedException
            return false;
        }
    }

    /**
     * Internal Producer is use to create task info to etcd
     */
    private static class TaskCreator<V> implements Runnable {

        private final HugeTask<V> task;
        private final HugeGraphParams graph;
        private final String graphSpace;
        private final String graphName;

        public TaskCreator(HugeTask<V> task, HugeGraphParams graph) {
            this.task = task;
            this.graph = graph;
            this.graphSpace = this.graph.graph().graphSpace();
            this.graphName = this.graph.name();
        }

        @Override
        public void run() {
            MetaManager manager = MetaManager.instance();
            TaskStatus status = manager.getTaskStatus(this.graphSpace, this.graphName, task);
            // Only unknown status indicates that the task has not been located
            if (status != TaskStatus.UNKNOWN) {
                return;
            }
            EtcdTaskScheduler.updateTaskStatus(graphSpace, this.graphName, task, TaskStatus.SCHEDULED);
            EtcdTaskScheduler.updateTaskContext(graphSpace, this.graphName, task);
            manager.createTask(graphSpace, this.graphName, task);
        }
    }

    /**
     * Internal Producer is use to process task
     */
    private static class TaskRunner<V> implements Runnable {

        private final HugeTask<V> task;
        private final HugeGraphParams graph;
        private final String graphSpace;
        private final String graphName;

        public TaskRunner(HugeTask<V> task, HugeGraphParams graph, Set<Id> processedTasks) {
            this.task = task;
            task.callable().graph(graph.graph());
            this.graph = graph;
            this.graphSpace = this.graph.graph().graphSpace();
            this.graphName = this.graph.graph().name();

            processedTasks.add(task.id());
        }

        @Override
        public void run() {
            try {
                TaskStatus etcdStatus = MetaManager.instance().getTaskStatus(this.graphSpace, this.graphName, task);
                if (TaskStatus.COMPLETED_STATUSES.contains(etcdStatus)) {
                    return;
                } else if(task.status() == TaskStatus.CANCELLING || etcdStatus == TaskStatus.CANCELLING) {
                    EtcdTaskScheduler.updateTaskStatus(graphSpace, this.graphName, task, TaskStatus.CANCELLED);
                    return;
                }

                if (task.status() == TaskStatus.HANGING) {
                    EtcdTaskScheduler.updateTaskStatus(graphSpace, this.graphName, task, TaskStatus.HANGING);
                    return;
                }

                EtcdTaskScheduler.updateTaskStatus(graphSpace, this.graphName, task, TaskStatus.RUNNING);
                Thread t = new Thread(this.task);
                t.start();
                // block until task is finished
                task.get();
                String result = task.result(); 
                if (!Strings.isNullOrEmpty(result) ) {
                    task.scheduler().save(task);
                }
                TaskStatus nextStatus = TaskStatus.COMPLETED_STATUSES.contains(task.status()) ? task.status() : TaskStatus.SUCCESS;
                EtcdTaskScheduler.updateTaskProgress(graphSpace, this.graphName, task, task.progress());
                EtcdTaskScheduler.updateTaskStatus(graphSpace, this.graphName, task, nextStatus);
                
            } catch (Exception e) {
                LOGGER.logCriticalError(e, String.format("task %d %s failed due to fatal error", task.id().asString(), task.name()));
                EtcdTaskScheduler.updateTaskStatus(graphSpace, this.graphName, task, TaskStatus.FAILED);
            } catch (Throwable t) {
                EtcdTaskScheduler.updateTaskStatus(graphSpace, this.graphName, task, TaskStatus.FAILED);
            } finally {
                MetaManager.instance().unlockTask(this.graphSpace, this.graphName, task);
            }
        }
    }

    private static boolean isTaskNextStatus(TaskStatus prevStatus, TaskStatus nextStatus) {
        return EtcdTaskScheduler.TASK_STATUS_MAP.get(prevStatus).contains(nextStatus);
    }


    private static void updateTaskRetry(String graphSpace, String graphName, HugeTask<?> task) {
        MetaManager manager = MetaManager.instance();
        task.retry();
        manager.updateTaskRetry(graphSpace, graphName, task);
    }

    /**
     * Internal TaskUpdater is used to update task status
     */
    private static void updateTaskStatus(String graphSpace, String graphName, HugeTask<?> task, TaskStatus nextStatus) {
        MetaManager manager = MetaManager.instance();
        // Synchronize local status & remote status
        TaskStatus etcdStatus = manager.getTaskStatus(graphSpace, graphName, task);
        /**
         * local status different to etcd status, and delayed
         */
        if (!EtcdTaskScheduler.isTaskNextStatus(etcdStatus, task.status()) && etcdStatus != TaskStatus.UNKNOWN) {
            task.status(etcdStatus);
        }
        // Ensure that next status is available
        TaskStatus prevStatus = task.status();
        if (EtcdTaskScheduler.isTaskNextStatus(prevStatus, nextStatus)) {
            task.status(nextStatus);
            manager.migrateTaskStatus(graphSpace, graphName, task, etcdStatus);
        }
    }

    /**
     * Persist taskc context to etcd for authentication
     */
    private static void updateTaskContext(String graphSpace, String graphName, HugeTask<?> task) {
        if (null == task || Strings.isNullOrEmpty(task.context())) {
            return;
        }
        MetaManager manager = MetaManager.instance();
        manager.updateTaskContext(graphSpace, graphName, task);

    }

    /**
     * Update task progress, make it always lead
     */
    private static void updateTaskProgress(String graphSpace, String graphName, HugeTask<?> task, int nextProgress) {
        MetaManager manager = MetaManager.instance();
        int etcdProgress = manager.getTaskProgress(graphSpace, graphName, task);
        // push task current progress forward
        if (task.progress() < nextProgress) {
            task.progress(nextProgress);
        }
        if (etcdProgress > task.progress()) {
            task.progress(etcdProgress);
            return;
        }
        manager.updateTaskProgress(graphSpace, graphName, task);
    }

    /**
     * Pick a proper executor to ensure that task with higher priority will be run earlier
     */
    private ExecutorService pickExecutor(TaskPriority priority) {
        ExecutorService result = this.taskExecutorMap.get(priority);
        if (null != result) {
            return result;
        }
        for (int i = priority.getValue() + 1; i <= TaskPriority.LOW.getValue(); i++) {
            TaskPriority nextPriority = TaskPriority.fromValue(i);
            result = this.taskExecutorMap.get(nextPriority);
            if (null != result) {
                return result;
            }
        }
        return null;
    }

    /**
     * General handler of tasks
     * @param <T>
     * @param response
     */
    private <T> void taskEventHandler(T response) {

        if (this.closing) {
            return;
        }
        
        // Prepare events
        MetaManager manager = MetaManager.instance();
        Map<String, String> events = manager.extractKVFromResponse(response);

        // Since the etcd event is not a single task, we should cope with them one by one
        for(Map.Entry<String, String> entry : events.entrySet()) {

            String currentContext = TaskManager.getContext();
            try {
                // Deserialize task
                HugeTask<?> task = TaskSerializer.fromJson(entry.getValue());
                Id parentId = task.parent();
                if (parentId != null) {
                    TaskStatus parentStatus = manager.getTaskStatus(graphSpace, graphName, parentId);    
                    // two cases: not finished or not run
                    if (parentStatus != TaskStatus.SUCCESS) {
                        // prepare to reschedule
                        if (parentStatus == TaskStatus.CANCELLED) {
                            // fast cancel
                            EtcdTaskScheduler.updateTaskStatus(graphSpace, graphName, task, TaskStatus.CANCELLING);
                            return;
                        } else {
                            // TODO: Currently, task.parent is not used in any scenario. However, it should be implemented
                        }
                        
                    }
                }

                // Try to lock the task
                LockResult result = EtcdTaskScheduler.lockTask(this.graphSpace(), this.graphName, task);
                if (result.lockSuccess()) {
                    // Persist the lockResult instance to task for keepAlive and unlock
                    task.lockResult(result);
                    // The task has been visited once
                    if (this.visitedTasks.contains(task.id().asString())) {
                        EtcdTaskScheduler.unlockTask(this.graphSpace(), this.graphName, task);
                        continue;
                    }
                    // Mark the task is visited already
                    this.visitedTasks.add(task.id().asString());
                    // Pick executor
                    TaskPriority priority = task.priority();
                    ExecutorService executor = this.pickExecutor(priority);
                    if (null == executor) {
                        EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), this.graphName, task, TaskStatus.SCHEDULED);
                        return;
                    }
                    // Grab status info from task
                    TaskStatus currentStatus = manager.getTaskStatus(this.graphSpace(), this.graphName, task);
                    task.status(currentStatus);
                    // If task has been occupied, skip also
                    if (TaskStatus.OCCUPIED_STATUS.contains(currentStatus)) {
                        this.visitedTasks.add(task.id().asString());
                        EtcdTaskScheduler.unlockTask(this.graphSpace(), this.graphName, task);
                        continue;
                    }
                    // Attach callable info
                    TaskCallable<?> callable = task.callable();
                    // Attach priority info
                    MetaManager.instance().attachTaskInfo(task, entry.getKey());
                    // Attach graph info
                    callable.graph(this.graph());
                    if (callable instanceof SysTaskCallable) {
                        ((SysTaskCallable<?>)callable).params(this.graph);
                    }
                    // Update status to queued
                    this.taskMap.put(task.id(), task);
                    task.scheduler(this);
                    EtcdTaskScheduler.updateTaskStatus(this.graphSpace(), this.graphName, task, TaskStatus.QUEUED);
                    // Update Threadlocal Context for authentication
                    String taskContext = manager.getTaskContext(this.graphSpace(), this.graphName, task);
                    task.overwriteContext(taskContext);
                    TaskManager.setContext(task.context());
                    // run it
                    executor.submit(new TaskRunner<>(task, this.graph, this.processedTasks));
                }
            } catch (Exception e) {
                LOGGER.logCriticalError(e, "Handle task failed");
            } finally {
                TaskManager.setContext(currentContext);
            }
        }
    }
}