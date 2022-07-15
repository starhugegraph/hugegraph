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

import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.baidu.hugegraph.config.CoreOptions;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.concurrent.PausableScheduledThreadPool;
import com.baidu.hugegraph.util.Consumers;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.Log;

import static com.baidu.hugegraph.task.StandardTaskScheduler.OWN;

public final class TaskManager {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public static final String TASK_WORKER_PREFIX = "task-worker";
    public static final String BACKUP_TASK_WORKER_PREFIX =
                               "backup-" + TASK_WORKER_PREFIX;
    public static final String TASK_WORKER = TASK_WORKER_PREFIX + "-%d";
    public static final String BACKUP_TASK_WORKER =
                               BACKUP_TASK_WORKER_PREFIX + "-%d";
    public static final String TASK_DB_WORKER = "task-db-worker-%d";
    public static final String SERVER_INFO_DB_WORKER =
                               "server-info-db-worker-%d";
    public static final String TASK_SCHEDULER = "task-scheduler-%d";

    public static final String OLAP_TASK_WORKER_PREFIX = "olap-task-worker";
    public static final String OLAP_TASK_WORKER =
            OLAP_TASK_WORKER_PREFIX + "-%d";
    public static final String SCHEMA_TASK_WORKER_PREFIX = "schema-task-worker";
    public static final String SCHEMA_TASK_WORKER =
            SCHEMA_TASK_WORKER_PREFIX + "-%d";
    public static final String EPHEMERAL_TASK_WORKER_PREFIX =
            "ephemeral-task-worker";
    public static final String EPHEMERAL_TASK_WORKER =
            EPHEMERAL_TASK_WORKER_PREFIX + "-%d";
    public static final String DISTRIBUTED_TASK_SCHEDULER_PREFIX =
            "distributed-scheduler";
    public static final String DISTRIBUTED_TASK_SCHEDULER =
            DISTRIBUTED_TASK_SCHEDULER_PREFIX + "-%d";

    protected static final int SCHEDULE_PERIOD = 3; // Unit second
    private static int THREADS;
    private static TaskManager MANAGER;
    private static String fakeContext;

    private final Map<HugeGraphParams, TaskScheduler> schedulers;

    private final ExecutorService taskExecutor;
    private final ExecutorService backupForLoadTaskExecutor;
    private final ExecutorService taskDbExecutor;
    private final ExecutorService serverInfoDbExecutor;
    private final PausableScheduledThreadPool schedulerExecutor;

    private final ExecutorService schemaTaskExecutor;
    private final ExecutorService olapTaskExecutor;
    private final ExecutorService ephemeralTaskExecutor;

    protected static final int DistributedSchedulerThread = 2;
    private final PausableScheduledThreadPool distributedSchedulerExecutor;


    public static TaskManager instance(int threads) {
        THREADS = threads;
        if (MANAGER == null) {
            MANAGER = new TaskManager(THREADS);
        }
        return MANAGER;
    }

    public static TaskManager instance() {
        if (MANAGER == null) {
            THREADS = Math.max(4, CoreOptions.CPUS / 2);
            MANAGER = new TaskManager(THREADS);
        }
        return MANAGER;
    }

    private TaskManager(int pool) {
        this.schedulers = new ConcurrentHashMap<>();

        // For execute tasks
        this.taskExecutor = ExecutorUtil.newFixedThreadPool(pool, TASK_WORKER);
        this.backupForLoadTaskExecutor =
                ExecutorUtil.newFixedThreadPool(pool, BACKUP_TASK_WORKER);
        // For save/query task state, just one thread is ok
        this.taskDbExecutor = ExecutorUtil.newFixedThreadPool(
                              1, TASK_DB_WORKER);
        this.serverInfoDbExecutor = ExecutorUtil.newFixedThreadPool(
                                    1, SERVER_INFO_DB_WORKER);

        this.schemaTaskExecutor = ExecutorUtil.newFixedThreadPool(pool,
                                                                  SCHEMA_TASK_WORKER);
        this.olapTaskExecutor = ExecutorUtil.newFixedThreadPool(pool,
                                                                OLAP_TASK_WORKER);
        this.ephemeralTaskExecutor = ExecutorUtil.newFixedThreadPool(pool,
                                                                     EPHEMERAL_TASK_WORKER);
        this.distributedSchedulerExecutor =
                ExecutorUtil.newPausableScheduledThreadPool(DistributedSchedulerThread,
                                                            DISTRIBUTED_TASK_SCHEDULER);

        // For schedule task to run, just one thread is ok
        this.schedulerExecutor = ExecutorUtil.newPausableScheduledThreadPool(
                                 1, TASK_SCHEDULER);
        // Start after 10s waiting for HugeGraphServer startup

        this.schedulerExecutor.scheduleWithFixedDelay(() -> {
                                                        this.scheduleOrExecuteJob(true);
                                                      },
                                                      10L, SCHEDULE_PERIOD,
                                                      TimeUnit.SECONDS);
    }

    public static void setFakeContext(String fakeContext) {
        TaskManager.fakeContext = fakeContext;
    }

    public void addScheduler(HugeGraphParams graph) {
        E.checkArgumentNotNull(graph, "The graph can't be null");
        LOG.info("Use {} as the scheudler of graph ({})",
                 graph.schedulerType(), graph.name());
        switch (graph.schedulerType()) {
            case "etcd": {
                    TaskScheduler scheduler = 
                        new EtcdTaskScheduler(
                            graph,
                            this.backupForLoadTaskExecutor,
                            this.taskDbExecutor,
                            this.serverInfoDbExecutor,
                            TaskPriority.LOW);
                    this.schedulers.put(graph, scheduler);
                }
                break;
            case "distributed": {
                TaskScheduler scheduler =
                        new DistributedTaskScheduler(
                                graph,
                                distributedSchedulerExecutor,
                                taskDbExecutor,
                                schemaTaskExecutor,
                                olapTaskExecutor,
                                taskExecutor,
                                ephemeralTaskExecutor
                        );
                this.schedulers.put(graph, scheduler);
                }
                break;
            case "local":
            default:
            {
                TaskScheduler scheduler = new StandardTaskScheduler(graph,
                                              this.taskExecutor,
                                              this.backupForLoadTaskExecutor,
                                              this.taskDbExecutor,
                                              this.serverInfoDbExecutor);
                this.schedulers.put(graph, scheduler);
            }
        }
    }

    public void closeScheduler(HugeGraphParams graph) {
        TaskScheduler scheduler = this.schedulers.get(graph);
        long tid = Thread.currentThread().getId();
        if (scheduler != null) {
            /*
             * Synch close+remove scheduler and iterate scheduler, details:
             * 'closeScheduler' should sync with 'scheduleOrExecuteJob'.
             * Because 'closeScheduler' will be called by 'graph.close()' in
             * main thread and there is gap between 'scheduler.close()'
             * (will close graph tx) and 'this.schedulers.remove(graph)'.
             * In this gap 'scheduleOrExecuteJob' may be run in
             * scheduler-db-thread and 'scheduleOrExecuteJob' will reopen
             * graph tx. As a result, graph tx will mistakenly not be closed
             * after 'graph.close()'.
             */
            Date currentTime = DateUtil.now();
            LOG.debug("Entering scheduler lock of closeScheduler(), trying to remove graphs, with tid {}, time {} ...", tid, currentTime);
            int poolActivateCount = this.schedulerExecutor.getActiveCount();
            int poolQueueSize = this.schedulerExecutor.getQueue().size();
            long taskCount = this.schedulerExecutor.getTaskCount();
            LOG.debug("current pool thread usage: activateCount {} , queueSize {} , taskCount {}", poolActivateCount, poolQueueSize, taskCount);

            synchronized (scheduler) {
                if (scheduler.close()) {
                    this.schedulers.remove(graph);
                }
            }
            LOG.debug("exit scheduler lock with tid {} , consumed {} ms", tid, DateUtil.now().getTime() - currentTime.getTime());
        }

        if (!this.taskExecutor.isTerminated()) {
            this.closeTaskTx(graph);
        }

        if (!this.backupForLoadTaskExecutor.isTerminated()) {
            this.closeBackupForLoadTaskTx(graph);
        }

        if (!this.schedulerExecutor.isTerminated()) {
            this.closeSchedulerTx(graph);
        }

        if (!this.distributedSchedulerExecutor.isTerminated()) {
            this.closeDistributedSchedulerTx(graph);
        }

        if (!this.schemaTaskExecutor.isTerminated()) {
            this.closeSchemaTaskTx(graph);
        }

        if (!this.olapTaskExecutor.isTerminated()) {
            this.closeOlapTaskTx(graph);
        }

        if (!this.ephemeralTaskExecutor.isTerminated()) {
            this.closeEphemeralTaskTx(graph);
        }

    }

    public void forceRemoveScheduler(HugeGraphParams params) {
        this.schedulers.remove(params);
    }

    private void closeTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.taskExecutor, totalThreads,
                                               graph::closeTx);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing task tx", e);
        }
    }

    private void closeBackupForLoadTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                         .startsWith(BACKUP_TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.backupForLoadTaskExecutor,
                                               totalThreads, graph::closeTx);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing backup task tx", e);
        }
    }

    private void closeSchemaTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(SCHEMA_TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.schemaTaskExecutor,
                                               totalThreads, graph::closeTx);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing schema task tx", e);
        }
    }

    private void closeOlapTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(OLAP_TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.olapTaskExecutor,
                                               totalThreads, graph::closeTx);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing olap task tx", e);
        }
    }

    private void closeEphemeralTaskTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(EPHEMERAL_TASK_WORKER_PREFIX);
        final int totalThreads = selfIsTaskWorker ? THREADS - 1 : THREADS;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.ephemeralTaskExecutor,
                                               totalThreads, graph::closeTx);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing ephemeral task tx",
                                    e);
        }
    }

    private void closeDistributedSchedulerTx(HugeGraphParams graph) {
        final boolean selfIsTaskWorker = Thread.currentThread().getName()
                                               .startsWith(DISTRIBUTED_TASK_SCHEDULER_PREFIX);
        final int totalThreads = selfIsTaskWorker ?
                DistributedSchedulerThread - 1 : DistributedSchedulerThread;
        try {
            if (selfIsTaskWorker) {
                // Call closeTx directly if myself is task thread(ignore others)
                graph.closeTx();
            } else {
                Consumers.executeOncePerThread(this.distributedSchedulerExecutor,
                                               totalThreads, graph::closeTx);
            }
        } catch (Exception e) {
            throw new HugeException("Exception when closing distributed " +
                                    "scheduler tx", e);
        }
    }

    private void closeSchedulerTx(HugeGraphParams graph) {
        final Callable<Void> closeTx = () -> {
            // Do close-tx for current thread
            graph.closeTx();
            // Let other threads run
            Thread.yield();
            return null;
        };
        try {
            this.schedulerExecutor.submit(closeTx).get();
        } catch (Exception e) {
            throw new HugeException("Exception when closing scheduler tx", e);
        }
    }

    public void pauseScheduledThreadPool() {
        this.schedulerExecutor.pauseSchedule();
    }

    public void resumeScheduledThreadPool() {
        this.schedulerExecutor.resumeSchedule();
    }

    public TaskScheduler getScheduler(HugeGraphParams graph) {
        return this.schedulers.get(graph);
    }

    public void shutdown(long timeout) {
        assert this.schedulers.isEmpty() : this.schedulers.size();

        Throwable ex = null;
        boolean terminated = this.schedulerExecutor.isTerminated();
        final TimeUnit unit = TimeUnit.SECONDS;

        if (!this.schedulerExecutor.isShutdown()) {
            this.schedulerExecutor.shutdown();
            try {
                terminated = this.schedulerExecutor.awaitTermination(timeout,
                                                                     unit);
                LOG.info("Shutdown schedulerExecutor result: {}", terminated);
            } catch (Throwable e) {
                ex = new HugeException("Shutdown SchedulerExecutor error", e);
            }
        }

        if (!this.distributedSchedulerExecutor.isShutdown()) {
            this.distributedSchedulerExecutor.shutdown();
            try {
                terminated = this.distributedSchedulerExecutor
                                 .awaitTermination(timeout, unit);
                LOG.info("Shutdown distributedSchedulerExecutor result: {}",
                         terminated);
            } catch (Throwable e) {
                ex = new HugeException("Shutdown SchedulerExecutor error", e);
            }
        }

        if (terminated && !this.taskExecutor.isShutdown()) {
            this.taskExecutor.shutdown();
            try {
                terminated = this.taskExecutor.awaitTermination(timeout, unit);
                LOG.info("Shutdown taskExecutor result: {}", terminated);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.backupForLoadTaskExecutor.isShutdown()) {
            this.backupForLoadTaskExecutor.shutdown();
            try {
                terminated = this.backupForLoadTaskExecutor
                                 .awaitTermination(timeout, unit);
                LOG.info("Shutdown backupForLoadTaskExecutor result: {}",
                         terminated);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.serverInfoDbExecutor.isShutdown()) {
            this.serverInfoDbExecutor.shutdown();
            try {
                terminated = this.serverInfoDbExecutor.awaitTermination(timeout,
                                                                        unit);
                LOG.info("Shutdown serverInfoDbExecutor result: {}",
                         terminated);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.taskDbExecutor.isShutdown()) {
            this.taskDbExecutor.shutdown();
            try {
                terminated = this.taskDbExecutor.awaitTermination(timeout, unit);
                LOG.info("Shutdown taskDbExecutor result: {}",
                         terminated);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.ephemeralTaskExecutor.isShutdown()) {
            this.ephemeralTaskExecutor.shutdown();
            try {
                terminated = this.ephemeralTaskExecutor.awaitTermination(timeout, unit);
                LOG.info("Shutdown ephemeralTaskExecutor result: {}",
                         terminated);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.schemaTaskExecutor.isShutdown()) {
            this.schemaTaskExecutor.shutdown();
            LOG.info("schemaTaskexecutor running count({})",
                     ((ThreadPoolExecutor) this.schemaTaskExecutor).getActiveCount());
            try {
                terminated = this.schemaTaskExecutor.awaitTermination(timeout, unit);
                LOG.info("Shutdown schemaTaskExecutor result: {}", terminated);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (terminated && !this.olapTaskExecutor.isShutdown()) {
            this.olapTaskExecutor.shutdown();
            try {
                terminated = this.olapTaskExecutor.awaitTermination(timeout, unit);
                LOG.info("Shutdown olapTaskExecutor result: {}", terminated);
            } catch (Throwable e) {
                ex = e;
            }
        }

        if (!terminated) {
            ex = new TimeoutException(timeout + "s");
        }
        if (ex != null) {
            throw new HugeException("Failed to wait for TaskScheduler", ex);
        }
    }

    public int workerPoolSize() {
        return ((ThreadPoolExecutor) this.taskExecutor).getCorePoolSize();
    }

    public int pendingTasks() {
        int size = 0;
        for (TaskScheduler scheduler : this.schedulers.values()) {
            size += scheduler.pendingTasks();
        }
        return size;
    }

    protected void notifyNewTask(HugeTask<?> task) {
        Queue<Runnable> queue = this.schedulerExecutor.getQueue();
        if (queue.size() <= 1) {
            /*
             * Notify to schedule tasks initiatively when have new task
             * It's OK to not notify again if there are more than one task in
             * queue(like two, one is timer task, one is immediate task),
             * we don't want too many immediate tasks to be inserted into queue,
             * one notify will cause all the tasks to be processed.
             */
            this.schedulerExecutor.submit((Callable<?>) () -> {
                String prevContext = TaskManager.getContext();
                try {
                    String context = task.context();
                    if (Strings.isNotBlank(context)) {
                        TaskManager.setContext(task.context());
                    } 
                    this.scheduleOrExecuteJob(false);
                } catch (Throwable e) {
                    LOG.error("Meet error when scheduleOrExecuteJob", e);
                } finally {
                    TaskManager.setContext(prevContext);
                }
                return null;
            });
        }
    }

    private void scheduleOrExecuteJob(boolean skipAuth) {
        // Called by scheduler timer
        try {
            // Long tid = Thread.currentThread().getId();
            for (TaskScheduler entry : this.schedulers.values()) {

                TaskScheduler scheduler = entry;
                // Maybe other thread close&remove scheduler at the same time
                // Date currentTime = DateUtil.now();
                // LOG.debug("Entering scheduler lock of scheduleOrExecuteJob(), with tid {}, time {}", tid, currentTime);
                // int poolActivateCount = this.schedulerExecutor.getActiveCount();
                // int poolQueueSize = this.schedulerExecutor.getQueue().size();
                // long taskCount = this.schedulerExecutor.getTaskCount();
                // LOG.debug("current pool thread usage: activateCount {} , queueSize {} , taskCount {}", poolActivateCount, poolQueueSize, taskCount);
                synchronized (scheduler) {
                    this.scheduleOrExecuteJobForGraph(scheduler, skipAuth);
                }
                // LOG.debug("exit scheduler lock with tid {} , time {}", tid, DateUtil.now().getTime() - currentTime.getTime());
            }
        } catch (Throwable e) {
            LOG.error("Exception occurred when schedule job", e);
        }
    }

    private void scheduleOrExecuteJobForGraph(TaskScheduler scheduler, boolean skipAuth) {
        E.checkNotNull(scheduler, "scheduler");
        if (scheduler instanceof StandardTaskScheduler) {

            StandardTaskScheduler standardTaskScheduler = (StandardTaskScheduler)(scheduler);

            if (!skipAuth) {
                if (!scheduler.graph().started()) {
                    return;
                }
            }

            // Ensuring the lock contains both graphSpace & graphName
            String graph = scheduler.graph().spaceGraphName();
            LockUtil.lock(graph, LockUtil.GRAPH_LOCK);
            try {
                /*
                 * Master schedule tasks to suitable servers.
                 * There is no suitable server when these tasks are created
                 */
                standardTaskScheduler.scheduleTasks();
                // Schedule queued tasks scheduled to current server
                standardTaskScheduler.executeTasksOnWorker(OWN);
                // Cancel tasks scheduled to current server
                standardTaskScheduler.cancelTasksOnWorker(OWN);
            } catch(Throwable e) {
                LOG.error("Raise throwable when scheduleOrExecuteJob", e);
                throw e;
            } finally {
                LockUtil.unlock(graph, LockUtil.GRAPH_LOCK);
            }
        }
    }

    private static final ThreadLocal<String> contexts = new ThreadLocal<>();

    protected static final void setContext(String context) {
        contexts.set(context);
    }

    protected static final void resetContext() {
        contexts.remove();
    }

    public static final String getContext() {
        return TaskManager.getContext(false);
    }

    public static final String getContext(boolean useFake) {
        String context = contexts.get();
        if (Strings.isEmpty(context) && useFake) {
            context = TaskManager.fakeContext;
        }
        return context;
    }

    public static boolean fakeContext(String context) {
        if (Strings.isEmpty(context)) {
            return false;
        }
        return context.equals(TaskManager.fakeContext);
    }

    public static final void useFakeContext() {
        setContext(fakeContext);
    }

    public static class ContextCallable<V> implements Callable<V> {

        private final Callable<V> callable;
        private final String context;

        public ContextCallable(Callable<V> callable) {
            E.checkNotNull(callable, "callable");
            this.context = getContext();
            this.callable = callable;
        }

        @Override
        public V call() throws Exception {
            setContext(this.context);
            try {
                return this.callable.call();
            } finally {
                resetContext();
            }
        }
    }
}
