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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.baidu.hugegraph.backend.id.IdGenerator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageInfo;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.job.EphemeralJob;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask.P;
import com.baidu.hugegraph.task.TaskCallable.SysTaskCallable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class StandardTaskScheduler extends TaskScheduler {

    private static final Logger LOG = Log.logger(TaskScheduler.class);

    public static final Id OWN = IdGenerator.of("own");

    private final ExecutorService taskExecutor;
    private final ExecutorService backupForLoadTaskExecutor;
    private final ExecutorService taskDbExecutor;

    private final Map<Id, HugeTask<?>> tasks;


    public StandardTaskScheduler(HugeGraphParams graph,
                                 ExecutorService taskExecutor,
                                 ExecutorService backupForLoadTaskExecutor,
                                 ExecutorService taskDbExecutor,
                                 ExecutorService serverInfoDbExecutor) {
        
        super(graph, serverInfoDbExecutor);
        E.checkNotNull(graph, "graph");
        E.checkNotNull(taskExecutor, "taskExecutor");
        E.checkNotNull(taskDbExecutor, "dbExecutor");
       

        this.taskExecutor = taskExecutor;
        this.backupForLoadTaskExecutor = backupForLoadTaskExecutor;
        this.taskDbExecutor = taskDbExecutor;

        this.tasks = new ConcurrentHashMap<>();

        this.taskTx = null;

        this.eventListener = this.listenChanges();
    }

    public boolean started() {
        return this.graph.started();
    }

    public String graphName() {
        return this.graph.name();
    }

    @Override
    public int pendingTasks() {
        return this.tasks.size();
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    @Override
    public <V> void restoreTasks() {
        // Restore 'RESTORING', 'RUNNING' and 'QUEUED' tasks in order.
        for (TaskStatus status : TaskStatus.PENDING_STATUSES) {
            String page = this.supportsPaging() ? PageInfo.PAGE_NONE : null;
            do {
                Iterator<HugeTask<V>> iter;
                for (iter = this.findTask(status, PAGE_SIZE, page);
                     iter.hasNext();) {
                    HugeTask<V> task = iter.next();
                    this.restore(task);
                }
                if (page != null) {
                    page = PageInfo.pageInfo(iter);
                }
            } while (page != null);
        }
    }

    private <V> Future<?> restore(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        E.checkArgument(!this.tasks.containsKey(task.id()),
                        "Task '%s' is already in the queue", task.id());
        E.checkArgument(!task.isDone() && !task.completed(),
                        "No need to restore completed task '%s' with status %s",
                        task.id(), task.status());
        task.status(TaskStatus.RESTORING);
        task.retry();
        return this.submitTask(task);
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");

        if (task.status() == TaskStatus.NEW && Strings.isNullOrEmpty(task.context())) {
            LOGGER.logCustomDebug("attach context to task {} ", "Scorpiour", task.id().asString());
            String currentContext = TaskManager.getContext();
            if (!Strings.isNullOrEmpty(currentContext)) {
                task.context(TaskManager.getContext());
            }
        } else {
            LOGGER.logCustomDebug("task {} has context already", "Scorpiour", task.id().asString());
        }

        if (task.status() == TaskStatus.QUEUED) {
            /*
             * Just submit to queue if status=QUEUED (means re-schedule task)
             * NOTE: schedule() method may be called multi times by
             * HugeTask.checkDependenciesSuccess() method
             */
            return this.resubmitTask(task);
        }

        if (task.callable() instanceof EphemeralJob) {
            /*
             * Due to EphemeralJob won't be serialized and deserialized through
             * shared storage, submit EphemeralJob immediately on master
             */
            task.status(TaskStatus.QUEUED);
            return this.submitTask(task);
        }

        // Only check if not EphemeralJob
        this.checkOnMasterNode("schedule");

        if (!task.computer()) {
            /*
             * Speed up for single node, submit task immediately
             * this can be removed without affecting logic
             */
            task.status(TaskStatus.QUEUED);
            task.server(OWN);
            this.save(task);
            return this.submitTask(task);
        } else {
            /*
             * Just set SCHEDULING status and save task
             * it will be scheduled by periodic scheduler worker
             */
            task.status(TaskStatus.SCHEDULING);
            this.save(task);

            // Notify master server to schedule and execute immediately
            TaskManager.instance().notifyNewTask(task);

            return task;
        }
    }

    private <V> Future<?> submitTask(HugeTask<V> task) {
        int size = this.tasks.size() + 1;
        E.checkArgument(size <= MAX_PENDING_TASKS,
                        "Pending tasks size %s has exceeded the max limit %s",
                        size, MAX_PENDING_TASKS);
        this.initTaskCallable(task);
        assert !this.tasks.containsKey(task.id()) : task;
        this.tasks.put(task.id(), task);
        if (this.graph.mode().loading()) {
            return this.backupForLoadTaskExecutor.submit(() -> {
                String currentContext = TaskManager.getContext();
                try {
                    TaskManager.setContext(task.context());
                    task.run();
                } catch (Throwable t) {
                    LOG.error("Meet error when execute task", t);
                    throw t;
                } finally {
                    TaskManager.setContext(currentContext);
                }
            });
        }

        return this.taskExecutor.submit(() -> {
            String currentContext = TaskManager.getContext();
            try {
                TaskManager.setContext(task.context());
                task.run();
            } catch (Throwable t) {
                LOG.error("Meet error when execute task", t);
                throw t;
            } finally {
                TaskManager.setContext(currentContext);
            }
        });
    }

    private <V> Future<?> resubmitTask(HugeTask<V> task) {
        E.checkArgument(task.status() == TaskStatus.QUEUED,
                        "Can't resubmit task '%s' with status %s",
                        task.id(), TaskStatus.QUEUED);
        E.checkArgument(this.tasks.containsKey(task.id()),
                        "Can't resubmit task '%s' not been submitted before",
                        task.id());
        if (this.graph.mode().loading()) {
            return this.backupForLoadTaskExecutor.submit(() -> {
                String currentContext = TaskManager.getContext();
                try {
                    TaskManager.setContext(task.context());
                    task.run();
                } catch (Throwable t) {
                    LOG.error("Meet error when execute task", t);
                    throw t;
                } finally {
                    TaskManager.setContext(currentContext);
                }
            });
        }

        return this.taskExecutor.submit(() -> {
            String currentContext = TaskManager.getContext();
            try {
                TaskManager.setContext(task.context());
                task.run();
            } catch (Throwable t) {
                throw t;
            } finally {
                TaskManager.setContext(currentContext);
            }
        });
    }

    public <V> void initTaskCallable(HugeTask<V> task) {
        task.scheduler(this);

        TaskCallable<V> callable = task.callable();
        callable.task(task);
        callable.graph(this.graph());
        if (callable instanceof SysTaskCallable) {
            // Only authorized to the necessary tasks
            ((SysTaskCallable<V>) callable).params(this.graph);
        }
    }

    @Override
    public synchronized <V> void cancel(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        this.checkOnMasterNode("cancel");

        if (task.completed() || task.cancelling()) {
            return;
        }

        LOG.info("Cancel task '{}' in status {}", task.id(), task.status());

        if (task.server() == null) {
            // The task not scheduled to workers, set canceled immediately
            assert task.status().code() < TaskStatus.QUEUED.code();
            if (task.status(TaskStatus.CANCELLED)) {
                this.save(task);
                return;
            }
        } else if (task.status(TaskStatus.CANCELLING)) {
            // The task scheduled to workers, let the worker node to cancel
            this.save(task);
            assert task.server() != null : task;
            this.remove(task);
            // Notify master server to schedule and execute immediately
            TaskManager.instance().notifyNewTask(task);
            return;
        }

        throw new HugeException("Can't cancel task '%s' in status %s",
                                task.id(), task.status());
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


    protected synchronized void scheduleTasks() {
        // Master server schedule all scheduling tasks to suitable worker nodes
        String page = this.supportsPaging() ? PageInfo.PAGE_NONE : null;
        do {
            Iterator<HugeTask<Object>> tasks = this.tasks(TaskStatus.SCHEDULING,
                                                          PAGE_SIZE, page);
            while (tasks.hasNext()) {
                HugeTask<?> task = tasks.next();
                if (task.server() != null) {
                    // Skip if already scheduled
                    continue;
                }

                task.server(OWN);
                task.status(TaskStatus.SCHEDULED);
                this.save(task);

                LOG.info("Scheduled task '{}' to server '{}'", task.id(), OWN);
            }
            if (page != null) {
                page = PageInfo.pageInfo(tasks);
            }
        } while (page != null);
    }

    protected void executeTasksOnWorker(Id server) {
        String page = this.supportsPaging() ? PageInfo.PAGE_NONE : null;
        do {
            Iterator<HugeTask<Object>> tasks = this.tasks(TaskStatus.SCHEDULED,
                                                          PAGE_SIZE, page);
            while (tasks.hasNext()) {
                HugeTask<?> task = tasks.next();
                this.initTaskCallable(task);
                Id taskServer = task.server();
                if (taskServer == null) {
                    LOG.warn("Task '{}' may not be scheduled", task.id());
                    continue;
                }
                HugeTask<?> memTask = this.tasks.get(task.id());
                if (memTask != null) {
                    assert memTask.status().code() > task.status().code();
                    continue;
                }
                if (taskServer.equals(server)) {
                    task.status(TaskStatus.QUEUED);
                    this.submitTask(task);
                }
            }
            if (page != null) {
                page = PageInfo.pageInfo(tasks);
            }
        } while (page != null);
    }

    protected void cancelTasksOnWorker(Id server) {
        String page = this.supportsPaging() ? PageInfo.PAGE_NONE : null;
        do {
            Iterator<HugeTask<Object>> tasks = this.tasks(TaskStatus.CANCELLING,
                                                          PAGE_SIZE, page);
            while (tasks.hasNext()) {
                HugeTask<?> task = tasks.next();
                Id taskServer = task.server();
                if (taskServer == null) {
                    LOG.warn("Task '{}' may not be scheduled", task.id());
                    continue;
                }
                if (!taskServer.equals(server)) {
                    continue;
                }
                /*
                 * Task may be loaded from backend store and not initialized.
                 * like: A task is completed but failed to save in the last
                 * step, resulting in the status of the task not being
                 * updated to storage, the task is not in memory, so it's not
                 * initialized when canceled.
                 */
                HugeTask<?> memTask = this.tasks.get(task.id());
                if (memTask != null) {
                    task = memTask;
                } else {
                    this.initTaskCallable(task);
                }
                boolean cancelled = task.cancel(true);
                LOG.info("Server '{}' cancel task '{}' with cancelled={}",
                         server, task.id(), cancelled);
            }
            if (page != null) {
                page = PageInfo.pageInfo(tasks);
            }
        } while (page != null);
    }

    
    @Override
    protected void taskDone(HugeTask<?> task) {
        this.remove(task);
    }

    protected void remove(HugeTask<?> task) {
        this.remove(task, false);
    }

    protected void remove(HugeTask<?> task, boolean force) {
        E.checkNotNull(task, "remove task");
        HugeTask<?> delTask = this.tasks.remove(task.id());
        if (delTask != null && delTask != task) {
            LOG.warn("Task '{}' may be inconsistent status {}(expect {})",
                      task.id(), task.status(), delTask.status());
        }
        assert force || delTask == null || delTask.completed() ||
               delTask.cancelling() || delTask.isCancelled() : delTask;
    }

    @Override
    public <V> void save(HugeTask<V> task) {
        task.scheduler(this);
        E.checkArgumentNotNull(task, "Task can't be null");
        this.call(() -> {
            // Construct vertex from task
            HugeVertex vertex = this.tx().constructVertex(task);
            // Delete index of old vertex to avoid stale index
            this.tx().deleteIndex(vertex);
            // Add or update task info to backend store
            return this.tx().addVertex(vertex);
        });
    }

    @Override
    public boolean close() {
        this.unlistenChanges();
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
    public <V> HugeTask<V> task(Id id) {
        E.checkArgumentNotNull(id, "Parameter task id can't be null");
        @SuppressWarnings("unchecked")
        HugeTask<V> task = (HugeTask<V>) this.tasks.get(id);
        if (task != null) {
            return task;
        }
        return this.findTask(id);
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
        List<Id> taskIdsNotInMem = new ArrayList<>();
        List<HugeTask<V>> taskInMem = new ArrayList<>();
        for (Id id : ids) {
            @SuppressWarnings("unchecked")
            HugeTask<V> task = (HugeTask<V>) this.tasks.get(id);
            if (task != null) {
                taskInMem.add(task);
            } else {
                taskIdsNotInMem.add(id);
            }
        }
        ExtendableIterator<HugeTask<V>> iterator;
        if (taskInMem.isEmpty()) {
            iterator = new ExtendableIterator<>();
        } else {
            iterator = new ExtendableIterator<>(taskInMem.iterator());
        }
        iterator.extend(this.findTasks(taskIdsNotInMem));
        return iterator;
    }

    @Override
    public <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                           long limit, String page) {
        if (status == null) {
            return this.findAllTask(limit, page);
        }
        return this.findTask(status, limit, page);
    }

    public <V> HugeTask<V> findTask(Id id) {
        HugeTask<V> result =  this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return HugeTask.fromVertex(vertex);
        });
        if (result == null) {
            throw new NotFoundException("Can't find task with id '%s'", id);
        }
        return result;
    }

    public <V> Iterator<HugeTask<V>> findTasks(List<Id> ids) {
        return this.queryTask(ids);
    }

    public <V> Iterator<HugeTask<V>> findAllTask(long limit, String page) {
        return this.queryTask(ImmutableMap.of(), limit, page);
    }

    public <V> Iterator<HugeTask<V>> findTask(TaskStatus status,
                                              long limit, String page) {
        return this.queryTask(P.STATUS, status.code(), limit, page);
    }

    @Override
    public <V> HugeTask<V> delete(Id id, boolean force) {
        this.checkOnMasterNode("delete");

        HugeTask<?> task = this.task(id);
        /*
         * The following is out of date when task running on worker node:
         * HugeTask<?> task = this.tasks.get(id);
         * Tasks are removed from memory after completed at most time,
         * but there is a tiny gap between tasks are completed and
         * removed from memory.
         * We assume tasks only in memory may be incomplete status,
         * in fact, it is also possible to appear on the backend tasks
         * when the database status is inconsistent.
         */
        if (task != null) {
            E.checkArgument(force || task.completed(),
                            "Can't delete incomplete task '%s' in status %s" +
                            ", Please try to cancel the task first",
                            id, task.status());
            this.remove(task, force);
        }

        return this.call(() -> {
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
        });
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

    @Override
    protected <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    private void checkOnMasterNode(String op) {
    }

    private boolean supportsPaging() {
        return this.graph.backendStoreFeatures().supportsQueryByPage();
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

    protected <V> V call(Callable<V> callable) {
        assert !Thread.currentThread().getName().startsWith(
               "task-db-worker") : "can't call by itself";

        return super.call(callable, taskDbExecutor);
    }

    @Override
    public void flushAllTask() {
        // TODO Auto-generated method stub
        
    }
}
