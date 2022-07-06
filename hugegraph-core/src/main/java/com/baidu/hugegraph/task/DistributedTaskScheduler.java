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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.concurrent.PausableScheduledThreadPool;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.meta.lock.LockResult;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public class DistributedTaskScheduler extends TaskAndResultScheduler{
    private static final Logger LOG = Log.logger(DistributedTaskScheduler.class);

    protected static final int SCHEDULE_PERIOD = 3;

    private final ExecutorService taskDbExecutor;
    private final ExecutorService schemaTaskExecutor;
    private final ExecutorService olapTaskExecutor;
    private final ExecutorService ephemeralTaskExecutor;
    private final ExecutorService gremlinTaskExecutor;
    private final PausableScheduledThreadPool schedulerExecutor;

    private ConcurrentHashMap<Id, HugeTask> runningTasks = new ConcurrentHashMap();

    public DistributedTaskScheduler(HugeGraphParams graph,
                                    ExecutorService taskDbExecutor,
                                    ExecutorService schemaTaskExecutor,
                                    ExecutorService olapTaskExecutor,
                                    ExecutorService gremlinTaskExecutor,
                                    ExecutorService ephemeralTaskExecutor) {
        super(graph, null);

        this.taskDbExecutor = taskDbExecutor;
        this.schemaTaskExecutor = schemaTaskExecutor;
        this.olapTaskExecutor = olapTaskExecutor;
        this.gremlinTaskExecutor = gremlinTaskExecutor;
        this.ephemeralTaskExecutor = ephemeralTaskExecutor;

        this.eventListener =  this.listenChanges();

        // For schedule task to run, just one thread is ok
        this.schedulerExecutor = ExecutorUtil.newPausableScheduledThreadPool(
                1, "distributed-" + graphSpace() + "-" + graph.name() + "-%d");
        // Start after 10s waiting for HugeGraphServer startup

        this.schedulerExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        // 使用超级管理员权限，查询任务
                        TaskManager.useFakeContext();
                        this.cronSchedule();
                    } catch (Throwable t) {
                        LOG.warn("cronScheduler exception ", t);
                    }
                },
                10L, SCHEDULE_PERIOD,
                TimeUnit.SECONDS);
    }

    public void cronSchedule() {
        // 执行周期调度任务

        // 处理NEW状态的任务
        Iterator<HugeTask<Object>> news = queryTaskWithoutResultByStatus(
                TaskStatus.NEW);

        while (news.hasNext()) {
            // Print Scheudler status
            logCurrentState();

            HugeTask newTask = news.next();
            initTaskParams(newTask);
            LOG.info("Try to start task({})@({}/{})", newTask.id(),
                     this.graphSpace, this.graphName);
            tryStartHugeTask(newTask);
        }

        // 处理runnning状态的任务
        Iterator<HugeTask<Object>> runnings =
                queryTaskWithoutResultByStatus(TaskStatus.RUNNING);

        while (runnings.hasNext()) {
            HugeTask running = runnings.next();
            initTaskParams(running);
            if (!MetaManager.instance().isLockedTask(graphSpace, graphName,
                                                     running.id().toString())) {
                LOG.info("Try to update task({})@({}/{}) status" +
                         "(RUNNING->FAILED)", running.id(), this.graphSpace,
                         this.graphName);
                updateStatusWithLock(running.id(), TaskStatus.RUNNING,
                                     TaskStatus.FAILED);
                runningTasks.remove(running.id());
            }
        }

        // 处理FAIELD/HANGING状态的任务
        Iterator<HugeTask<Object>> faileds =
                queryTaskWithoutResultByStatus(TaskStatus.FAILED);

        while (faileds.hasNext()) {
            HugeTask<Object> failed = faileds.next();
            initTaskParams(failed);
            if (failed.retries() < 3) {
                LOG.info("Try to update task({})@({}/{}) status(FAILED->NEW)",
                         failed.id(), this.graphSpace, this.graphName);
                updateStatusWithLock(failed.id(), TaskStatus.FAILED,
                                     TaskStatus.NEW);
            }
        }

        // 处理CANCELLING状态的任务
        Iterator<HugeTask<Object>> cancellings = queryTaskWithoutResultByStatus(
                TaskStatus.CANCELLING);

        while (cancellings.hasNext()) {
            Id cancellingId = cancellings.next().id();
            if (runningTasks.containsKey(cancellingId)) {
                HugeTask cancelling  = runningTasks.get(cancellingId);
                initTaskParams(cancelling);
                cancelling.cancel(true);

                runningTasks.remove(cancellingId);
            } else {
                // 本地没有执行任务，但是当前任务已经无节点在执行
                if (!MetaManager.instance().isLockedTask(graphSpace, graphName,
                                                         cancellingId.toString())) {
                    updateStatusWithLock(cancellingId, TaskStatus.CANCELLING,
                                         TaskStatus.CANCELLED);
                }
            }
        }

        // 处理DELETING状态的任务
        Iterator<HugeTask<Object>> deletings = queryTaskWithoutResultByStatus(
                TaskStatus.DELETING);

        while (deletings.hasNext()) {
            Id deletingId = deletings.next().id();
            if (runningTasks.containsKey(deletingId)) {
                HugeTask deleting = runningTasks.get(deletingId);
                initTaskParams(deleting);
                deleting.cancel(true);

                // 删除存储信息
                deleteFromDB(deletingId);

                runningTasks.remove(deletingId);
            } else {
                // 本地没有执行任务，但是当前任务已经无节点在执行
                if (!MetaManager.instance().isLockedTask(graphSpace, graphName,
                                                         deletingId.toString())) {
                    deleteFromDB(deletingId);
                }
            }
        }
    }

    protected <V> Iterator<HugeTask<V>> queryTaskByStatus(TaskStatus status) {
        return queryTask(HugeTask.P.STATUS, status.code(), NO_LIMIT, null);
    }

    protected <V> Iterator<HugeTask<V>> queryTaskWithoutResultByStatus(TaskStatus status) {
        return queryTaskWithoutResult(HugeTask.P.STATUS, status.code(), NO_LIMIT, null);
    }

    @Override
    public int pendingTasks() {
        int count = 0;
        for(TaskStatus status : TaskStatus.PENDING_STATUSES) {

            count += Iterators.size(queryTaskWithoutResultByStatus(status));
        }

        return count;
    }


    @Override
    public <V> void restoreTasks() {
        // DO Nothing!
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        initTaskParams(task);

        if (task.ephemeralTask()) {
            // 处理ephemeral任务，不需要调度，直接执行
            return this.ephemeralTaskExecutor.submit(task);
        }

        // 处理schema任务
        // 处理gremlin任务
        // 处理OLAP计算任务
        // 添加任务到DB， 当前任务状态为NEW
        this.save(task);

        return null;
    }

    protected void initTaskParams(HugeTask task) {
        // 绑定当前任务执行所需的环境变量。
        // 在任务反序列化和执行之前，均需要调用该方法.
        task.scheduler(this);
        TaskCallable callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());

        if (callable instanceof TaskCallable.SysTaskCallable) {
            ((TaskCallable.SysTaskCallable) callable).params(this.graph);
        }
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        // 更新状态为CANCELLING
        this.updateStatus(task.id(), null, TaskStatus.CANCELLING);
    }

    protected <V> HugeTask<V> deleteFromDB(Id id) {
        // 从DB中删除Task，不检查任务状态
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            HugeTask<V> result = HugeTask.fromVertex(vertex);
            this.tx().removeVertex(vertex);
            return result;
        });
    }

    @Override
    public <V> HugeTask<V> delete(Id id, boolean force) {
        if (!force) {
            // 更改状态为DELETING，通过自动调度实现删除操作。
            this.updateStatus(id, null, TaskStatus.DELETING);
            return null;
        } else {
            return this.deleteFromDB(id);
        }
    }
    @Override
    protected void taskDone(HugeTask<?> task) {
        runningTasks.remove(task.id());
    }

    @Deprecated
    @Override
    public void flushAllTask() {
        // Do Nothing!
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

    @Override
    public boolean close() {
        // 关闭周期调度线程
        schedulerExecutor.shutdownNow();

        this.graph.loadSystemStore().provider().unlisten(this.eventListener);

        if (!this.taskDbExecutor.isShutdown()) {
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

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
            throws TimeoutException {
        return this.waitUntilTaskCompleted(id, seconds, QUERY_INTERVAL);
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id)
            throws TimeoutException {
        // This method is just used by tests
        long timeout = this.graph.configuration()
                                 .get(CoreOptions.TASK_WAIT_TIMEOUT);
        return this.waitUntilTaskCompleted(id, timeout, 1L);
    }

    private <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds,
                                                   long intervalMs)
            throws TimeoutException {
        long passes = seconds * 1000 / intervalMs;
        HugeTask<V> task = null;
        for (long pass = 0;; pass++) {
            try {
                task = this.taskWithoutResult(id);
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
    public void waitUntilAllTasksCompleted(long seconds)
            throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        int taskSize = 0;
        for (long pass = 0;; pass++) {
            taskSize = this.pendingTasks();
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

    private static boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ignored) {
            // Ignore InterruptedException
            return false;
        }
    }

    @Override
    protected <V> V call(Callable<V> callable) {
        return super.call(callable, this.taskDbExecutor);
    }

    @Override
    protected <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    protected boolean updateStatus(Id id, TaskStatus prestatus,
                                   TaskStatus status) {
        HugeTask<Object> task = this.taskWithoutResult(id);
        initTaskParams(task);
        if (prestatus == null || task.status() == prestatus) {
            task.status(status);
            // 如果状态更新为FAILED->NEW，则增加重试次数。
            if (prestatus == TaskStatus.FAILED && status == TaskStatus.NEW) {
                task.retry();
            }
            this.save(task);
            LOG.info("Update task({}) success: pre({}), status({})",
                     id, prestatus, status);

            return true;
        } else {
            LOG.info("Update task({}) status conflict: current({}), " +
                             "pre({}), status({})", id, task.status(),
                     prestatus, status);
            return false;
        }
    }

    protected boolean updateStatusWithLock(Id id, TaskStatus prestatus,
                                           TaskStatus status) {

        LockResult lockResult = MetaManager.instance()
                                           .tryLockTask(this.graphSpace,
                                                        this.graphName,
                                                        id.asString());

        if (lockResult.lockSuccess()) {
            try {
                return updateStatus(id, prestatus, status);
            } finally {
                MetaManager.instance().unlockTask(this.graphSpace,
                                                  this.graphName,
                                                  id.asString(), lockResult);
            }
        }

        return false;
    }

    protected void tryStartHugeTask(HugeTask task) {
        TaskCallable callable = task.callable();
        ExecutorService chosenExecutor = gremlinTaskExecutor;

        if (task.computer()) {
           chosenExecutor = this.olapTaskExecutor;
        }

        if (task.gremlinTask()) {
            chosenExecutor = this.gremlinTaskExecutor;
        }

        if (task.schemaTask()) {
            chosenExecutor = schemaTaskExecutor;
        }

        if (((ThreadPoolExecutor) chosenExecutor).getQueue().size() <= 0) {
            TaskRunner runner = new TaskRunner<>(task);
            chosenExecutor.submit(runner);
        }
    }

    protected void logCurrentState() {
        int gremlinActive =
                ((ThreadPoolExecutor) gremlinTaskExecutor).getActiveCount();
        int schemaActive =
                ((ThreadPoolExecutor) schemaTaskExecutor).getActiveCount();
        int ephemeralActive =
                ((ThreadPoolExecutor) ephemeralTaskExecutor).getActiveCount();
        int olapActive =
                ((ThreadPoolExecutor) olapTaskExecutor).getActiveCount();

        LOG.info("Current State: gremlinTaskExecutor({}), schemaTaskExecutor" +
                 "({}), ephemeralTaskExecutor({}), olapTaskExecutor({})",
                 gremlinActive, schemaActive, ephemeralActive, olapActive);
    }

    private class TaskRunner<V> implements Runnable {

        private final HugeTask<V> task;

        public TaskRunner(HugeTask<V> task) {
            this.task = task;
        }

        @Override
        public void run() {
            LockResult lockResult = MetaManager.instance()
                                               .tryLockTask(graphSpace,
                                                            graphName,
                                                            task.id().asString());

            initTaskParams(task);
            if (lockResult.lockSuccess()) {
                try {
                    // 更新任务的状态为SCHEDULED，并及时保存到DB中
                    runningTasks.put(task.id(), task);

                    // 任务执行不会抛出异常， hugetask在执行过程中，会捕获异常，并存储到DB中
                    task.run();
                } finally {
                    runningTasks.remove(task.id());
                    MetaManager.instance().unlockTask(graphSpace,
                                                      graphName,
                                                      task.id().asString(),
                                                      lockResult);
                }
            }
        }
    }
}
