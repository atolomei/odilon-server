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
package io.odilon.replication;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * IMPORTANT. Initial Sync only syncs head version It will not sync previous
 * versions.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public class StandByInitialSync implements Runnable {

    static private Logger logger = Logger.getLogger(StandByInitialSync.class.getName());
    static private Logger startuplogger = Logger.getLogger("StartupLogger");
    static private Logger intialSynclogger = Logger.getLogger("IntialSyncLogger");

    @JsonIgnore
    private long start_ms;

    @JsonIgnore
    private int maxProcessingThread = 1;

    @JsonIgnore
    private AtomicLong errors = new AtomicLong(0);

    @JsonIgnore
    private AtomicBoolean done = new AtomicBoolean(false);

    @JsonIgnore
    private AtomicLong checkOk = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong counter = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong copied = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong totalBytes = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong cleaned = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong notAvailable = new AtomicLong(0);

    @JsonIgnore
    private LockService vfsLockService;

    @JsonIgnore
    VirtualFileSystemService vfs;

    @JsonIgnore
    ReplicationService replicationService;

    @JsonIgnore
    private Thread thread;

    @JsonIgnore
    private IODriver driver;

    /**
     * 
     */
    public StandByInitialSync(IODriver driver) {
        this.driver = driver;
        this.vfsLockService = this.driver.getLockService();
        this.vfs = this.driver.getVirtualFileSystemService();
        this.replicationService = this.vfs.getReplicationService();
    }

    public void start() {
        this.thread = new Thread(this);
        this.thread.setDaemon(true);
        this.thread.setName(this.getClass().getSimpleName());
        this.thread.start();
    }

    @Override
    public void run() {
        try {
            logger.debug("Starting -> " + getClass().getSimpleName());
            sync();
        } finally {
            this.done = new AtomicBoolean(true);
            getReplicationService().setInitialSync(new AtomicBoolean(false));
        }
    }

    public AtomicBoolean isDone() {
        return this.done;
    }

    protected IODriver getDriver() {
        return driver;
    }

    protected LockService getLockService() {
        return this.vfsLockService;
    }

    protected VirtualFileSystemService getVirtualFileSystemService() {
        return this.vfs;
    }

    protected ReplicationService getReplicationService() {
        return this.replicationService;
    }

    private void sync() {

        this.start_ms = System.currentTimeMillis();

        OdilonServerInfo info = getDriver().getVirtualFileSystemService().getOdilonServerInfo();

        if (info == null)
            return;

        if (info.getStandByStartDate() == null)
            return;

        if (info.getCreationDate() == null)
            return;

        getReplicationService().setInitialSync(new AtomicBoolean(true));

        this.maxProcessingThread = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0).intValue()
                + 1 - 2;

        if (this.maxProcessingThread < 1)
            this.maxProcessingThread = 1;

        if (this.vfs.getServerSettings().getStandbySyncThreads() > 0)
            this.maxProcessingThread = this.vfs.getServerSettings().getStandbySyncThreads();

        ExecutorService executor = null;

        try {

            this.errors = new AtomicLong(0);

            executor = Executors.newFixedThreadPool(maxProcessingThread);

            List<ServerBucket> buckets = this.driver.getVirtualFileSystemService().listAllBuckets();
            boolean completed = buckets.isEmpty();

            for (ServerBucket bucket : buckets) {

                Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
                Long offset = Long.valueOf(0);
                String agentId = null;

                boolean done = false;

                while ((!done) && (this.errors.get() <= 10)) {

                    DataList<Item<ObjectMetadata>> data = this.driver.getVirtualFileSystemService().listObjects(bucket.getName(),
                            Optional.of(offset), Optional.ofNullable(pageSize), Optional.empty(), Optional.ofNullable(agentId));

                    if (agentId == null)
                        agentId = data.getAgentId();

                    List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());

                    for (Item<ObjectMetadata> item : data.getList()) {
                        tasks.add(() -> {
                            try {

                                if (this.errors.get() > 10)
                                    return null;

                                this.counter.getAndIncrement();

                                if (((this.counter.get() + 1) % 10) == 1)
                                    logger.debug("synced so far -> " + String.valueOf(this.counter.get()));

                                if (item.isOk()) {

                                    boolean objectSynced = false;

                                    getLockService().getObjectLock(item.getObject().getBucketId(), item.getObject().getObjectName())
                                            .readLock().lock();

                                    try {

                                        getLockService().getBucketLock(bucket).readLock().lock();

                                        try {

                                            if ((item.getObject().dateSynced == null)
                                                    || (item.getObject().dateSynced.isBefore(info.getStandByStartDate()))) {

                                                logger.debug(item.getObject().getBucketId().toString() + "-"
                                                        + item.getObject().getObjectName());

                                                VirtualFileSystemOperation op = new OdilonVirtualFileSystemOperation(
                                                        getDriver().getVirtualFileSystemService().getJournalService()
                                                                .newOperationId(),
                                                        OperationCode.CREATE_OBJECT, Optional.of(item.getObject().getBucketId()),
                                                        Optional.of(item.getObject().getBucketName()), // TODO AT VER
                                                        Optional.of(item.getObject().getObjectName()),
                                                        Optional.of(Integer.valueOf(item.getObject().getVersion())),
                                                        getDriver().getVirtualFileSystemService().getRedundancyLevel(),
                                                        getDriver().getVirtualFileSystemService().getJournalService());

                                                getReplicationService().replicate(op);
                                                objectSynced = true;
                                                this.totalBytes.addAndGet(item.getObject().length());
                                                this.copied.getAndIncrement();
                                            }

                                        } catch (Exception e) {
                                            logger.error(e, "can not sync -> " + item.getObject().bucketId.toString() + "-"
                                                    + item.getObject().objectName, SharedConstant.NOT_THROWN);
                                            intialSynclogger.error(e, "can not sync -> " + item.getObject().bucketId.toString()
                                                    + "-" + item.getObject().objectName);
                                            this.errors.getAndIncrement();
                                        } finally {
                                            getLockService().getBucketLock(item.getObject().getBucketId()).readLock().unlock();

                                        }
                                    } finally {
                                        getLockService()
                                                .getObjectLock(item.getObject().getBucketId(), item.getObject().getObjectName())
                                                .readLock().unlock();
                                    }

                                    if (objectSynced) {
                                        try {
                                            ObjectMetadata meta = item.getObject();
                                            meta.dateSynced = OffsetDateTime.now();
                                            meta.lastModified = OffsetDateTime.now();
                                            getDriver().putObjectMetadata(meta);

                                        } catch (Exception e) {
                                            logger.error("can not sync ObjectMetadata -> " + item.getObject().bucketId.toString()
                                                    + "-" + item.getObject().objectName, SharedConstant.NOT_THROWN);
                                            intialSynclogger.error("can not sync ObjectMetadata -> "
                                                    + item.getObject().bucketId.toString() + "-" + item.getObject().objectName);
                                            intialSynclogger.error(e, SharedConstant.NOT_THROWN);
                                            this.errors.getAndIncrement();
                                        }
                                    }

                                } else {
                                    this.notAvailable.getAndIncrement();
                                }
                            } catch (Exception e) {
                                logger.error(e, SharedConstant.NOT_THROWN);
                                intialSynclogger.error(e, SharedConstant.NOT_THROWN);
                                this.errors.getAndIncrement();
                            }
                            return null;
                        });
                    }

                    try {
                        executor.invokeAll(tasks, 10, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        logger.error(e, SharedConstant.NOT_THROWN);
                    }

                    offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
                    done = (data.isEOD() || (this.errors.get() > 0) || (this.notAvailable.get() > 0));

                    completed = done && (this.errors.get() == 0) && (this.notAvailable.get() == 0);

                }
            }
            try {
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.MINUTES);

                if (completed) {
                    info.setStandBySyncedDate(OffsetDateTime.now());
                    this.vfs.setOdilonServerInfo(info);

                    logger.info(ServerConstant.SEPARATOR);
                    logger.info("Intial Sync completed");
                    intialSynclogger.info("Intial Sync completed");
                    startuplogger.info("Intial Sync completed");
                } else {

                    logger.info(ServerConstant.SEPARATOR);
                    logger.error(
                            "The intial Sync process can not be completed. Please correct the issues and restart the Odilon Server in order for the Sync process to execute again");
                    intialSynclogger.error("Intial Sync can not be completed");
                    startuplogger.error("Intial Sync can not be completed");
                }

            } catch (InterruptedException e) {
            }

        } finally {
            logResults(startuplogger);
            logResults(intialSynclogger);
        }
    }

    /**
     * @param logger
     */
    private void logResults(Logger logger) {

        logger.info(ServerConstant.SEPARATOR);
        logger.debug("Threads: " + String.valueOf(maxProcessingThread));
        logger.info("Total files scanned: " + String.valueOf(this.counter.get()));
        logger.info("Total files synced: " + String.valueOf(this.copied.get()));
        logger.info("Total size synced: "
                + String.format("%14.4f", Double.valueOf(totalBytes.get()).doubleValue() / SharedConstant.d_gigabyte).trim()
                + " GB");
        if (this.errors.get() > 0)
            logger.info("Error files: " + String.valueOf(this.errors.get()));
        if (this.notAvailable.get() > 0)
            logger.info("Not Available files: " + String.valueOf(this.notAvailable.get()));
        logger.info("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000))
                + " secs");
        logger.info(ServerConstant.SEPARATOR);

    }

}
