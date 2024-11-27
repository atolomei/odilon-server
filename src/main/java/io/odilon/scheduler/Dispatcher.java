/*
 * Odilon Object Storage
 * (C) Novamens 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.odilon.scheduler;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import io.odilon.log.Logger;
import io.odilon.model.BaseObject;
import io.odilon.model.SharedConstant;

/**
 * <p>
 * A Dispatcher contains a ThreadPool and assigns incoming
 * {@link ServiceRequest} to its Threads.
 * </p>
 * <p>
 * There is one Dispatcher for every {@link SchedulerWorkder} in the system
 * </p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class Dispatcher extends BaseObject {

    static private Logger startupLogger = Logger.getLogger("StartupLogger");
    static private Logger logger = Logger.getLogger(Dispatcher.class.getName());

    private int priority;
    private int maxPoolSize;
    private int minPoolSize;
    private int initialSize;
    private int keeepAliveMS; /* Time in ms */
    private String id;
    private int threadsPriority = Thread.currentThread().getPriority();
    private int threadNumber = 1;

    @JsonIgnore
    private ThreadFactory threadFactory = new ThreadFactory();

    @JsonIgnore
    private PooledExecutor threadPool;

    @JsonIgnore
    Map<String, Thread> i_threads = new HashMap<String, Thread>();

    private class ThreadFactory implements EDU.oswego.cs.dl.util.concurrent.ThreadFactory {
        public Thread newThread(Runnable command) {
            Thread newThread = new Thread(command);
            newThread.setDaemon(true);
            newThread.setName(getId() + "_Worker" + "-" + String.valueOf(threadNumber++));
            newThread.setPriority(threadsPriority);
            return newThread;
        }
    }

    /**
     * @param priority
     * @param poolSize
     */

    public Dispatcher(String id, int priority, int poolSize) {
        this(priority, poolSize, id);
    }

    /**
     * <p>
     * </p>
     */
    public Dispatcher(int priority, int poolSize, String id) {
        this.priority = priority;
        this.maxPoolSize = poolSize; // max size
        this.minPoolSize = poolSize;
        this.initialSize = poolSize > 4 ? 4 : poolSize;
        this.keeepAliveMS = -1;
        this.id = id;
        startupLogger.debug("Starting Dispatcher " + getId());
        makePool();
        startupLogger.debug(getInfo());
    }

    /**
     * <p>
     * it doesn't make sense to have maxPoolSize != minPoolSize, because queue is
     * unbounded so maxPoolSize will never be used
     * </p>
     * 
     * @param priority
     * @param maxPoolSize
     * @param minPoolSize
     * @param initialSize
     * @param keeepAliveMS
     * @param id
     */
    public Dispatcher(int priority, int maxPoolSize, int minPoolSize, int initialSize, int keeepAliveMS, String id) {

        assert (maxPoolSize == minPoolSize);

        this.priority = priority;
        this.maxPoolSize = maxPoolSize; // max size
        this.minPoolSize = maxPoolSize;
        this.initialSize = initialSize;
        this.keeepAliveMS = keeepAliveMS;
        this.minPoolSize = minPoolSize;
        this.id = id;
        makePool();
        startupLogger.info(getInfo());
    }

    public String getId() {
        return id;
    }

    public void restart() {
        restart(false);
    }

    public void restart(boolean force) {
        if (force) {
            logger.info("shuting down now.");
            this.threadPool.shutdownNow();
        } else {
            logger.info("Waiting for all current processes to terminate and shutdown.");
            this.threadPool.shutdownAfterProcessingCurrentlyQueuedTasks();
        }
        logger.info("restarting threads.");
        makePool();
    }

    public String getInfo() {
        return "Priority: " + String.valueOf(this.priority) + ". Workers: " + String.valueOf(this.maxPoolSize);
    }

    public String getStatus() {
        StringBuilder str = new StringBuilder();
        int poolsize = this.threadPool.getPoolSize();
        str.append("Priority: " + String.valueOf(this.priority) + " | ");
        str.append("PoolSize: " + String.valueOf(poolsize) + " | ");
        str.append("MaxPoolSize: " + String.valueOf(this.threadPool.getMaximumPoolSize()));
        return str.toString();
    }

    public void shutDownNow() {
        this.threadPool.shutdownNow();
    }

    public int getPoolSize() {
        return this.threadPool.getPoolSize();
    }

    public int getMaximumPoolSize() {
        return this.threadPool.getMaximumPoolSize();
    }

    public int getPriority() {
        return this.priority;
    }

    public void setPriority(int value) {
        this.priority = value;
    }

    public void dispatch(ServiceRequestExecutor serviceExecutor) {
        try {

            this.threadPool.execute(serviceExecutor);
        } catch (InterruptedException e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    public void execute(Runnable runnable) {
        try {
            this.threadPool.execute(runnable);
        } catch (InterruptedException e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    public void setMaxPoolSize(int value) {
        this.maxPoolSize = value;
        this.threadPool.setMaximumPoolSize(value);
    }

    private void makePool() {
        this.threadPool = new PooledExecutor(new LinkedQueue(), maxPoolSize);
        this.threadPool.setThreadFactory(threadFactory);
        this.threadPool.setMinimumPoolSize(minPoolSize);
        this.threadPool.createThreads(initialSize); // Threads are created at startup.
        this.threadPool.setKeepAliveTime(keeepAliveMS);
        this.threadPool.setMaximumPoolSize(maxPoolSize);
        this.threadPool.waitWhenBlocked(); // Wait for a free thread when blocked.
    }
}
