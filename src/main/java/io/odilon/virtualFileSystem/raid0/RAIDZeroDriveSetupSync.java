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
package io.odilon.virtualFileSystem.raid0;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Files;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.virtualFileSystem.Context;
import io.odilon.virtualFileSystem.DriveInfo;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.IODriveSetup;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Set up a new <b>Drive</b> added to the <b>odilon.properties</b> config file.
 * For RAID 0 this process is <b>Sync</b> when the server starts up (for RAID 1
 * and RAID 6 the process is Async and runs in background).<br/>
 * Unlike {@link RAIDSixDriver}, this setup does not need the
 * {@link VirtualFileSystemService} to be in state {@link ServiceStatus.RUNNING}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public class RAIDZeroDriveSetupSync implements IODriveSetup {

    static private Logger logger = Logger.getLogger(RAIDZeroDriveSetupSync.class.getName());
    static private Logger startuplogger = Logger.getLogger("StartupLogger");

    @JsonIgnore
    private RAIDZeroDriver driver;

    @JsonIgnore
    private AtomicLong checkOk = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong counter = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong moved = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong totalBytesMoved = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong totalBytesCleaned = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong errors = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong cleaned = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong notAvailable = new AtomicLong(0);

    @JsonIgnore
    int maxProcessingThread;

    @JsonIgnore
    private long start_ms;

    @JsonIgnore
    private long start_move;

    @JsonIgnore
    private long start_cleanup;

    @JsonIgnore
    private List<Drive> listEnabledBefore = new ArrayList<Drive>();

    @JsonIgnore
    private List<Drive> listAllBefore = new ArrayList<Drive>();

    /**
     * @param driver
     */
    public RAIDZeroDriveSetupSync(RAIDZeroDriver driver) {
        this.driver = driver;
        this.maxProcessingThread = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0).intValue()
                + 1;
    }

    /**
     * <p>
     * this method does not need the {@link VirtualFileSystemService} to be in state
     * Running
     * </p>
     */
    @Override
    public boolean setup() {

        this.start_ms = System.currentTimeMillis();

        getDriver().getDrivesEnabled().forEach(item -> this.listEnabledBefore.add(item));
        getDriver().getDrivesAll().forEach(item -> this.listAllBefore.add(item));

        startuplogger.info("This process is blocking for RAID 0");
        startuplogger.info("It may take some time to complete. It has tow steps:");
        startuplogger.info("1. Copy files to the new Drives");
        startuplogger.info("2. After completing the copy process, it will clean up duplicates");

        final OdilonServerInfo serverInfo = getDriver().getServerInfo();
        final File keyFile = getDriver().getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
        final String jsonString;

        try {
            jsonString = getDriver().getObjectMapper().writeValueAsString(serverInfo);
        } catch (JsonProcessingException e) {
            throw new InternalCriticalException(e);
        }

        startuplogger.info("1. Copying -> " + VirtualFileSystemService.SERVER_METADATA_FILE);

        getDriver().getDrivesAll().forEach(item -> {
            File file = item.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
            if ((item.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) && ((file == null) || (!file.exists()))) {
                try {
                    item.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
                } catch (Exception e) {
                    throw new InternalCriticalException(e, "Drive -> " + item.getName());
                }
            }
        });

        if ((keyFile != null) && keyFile.exists()) {
            startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE);
            getDriver().getDrivesAll().forEach(item -> {
                File file = item.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
                if ((item.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) && ((file == null) || (!file.exists()))) {
                    try {
                        Files.copy(keyFile, file);
                    } catch (Exception e) {
                        throw new InternalCriticalException(e, "Drive -> " + item.getName());
                    }
                }
            });
        } else {
            startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE + " | file not exist. skipping");
        }

        createBuckets();

        if (this.errors.get() > 0 || this.notAvailable.get() > 0) {
            startuplogger.error("The process can not be completed due to errors");
            startuplogger.error(ServerConstant.SEPARATOR);
            return false;
        }

        copy();

        if (this.errors.get() > 0 || this.notAvailable.get() > 0) {
            startuplogger.error("The process can not be completed due to errors");
            startuplogger.error(ServerConstant.SEPARATOR);
            return false;
        }

        updateDrives();

        try {
            cleanUp();
        } catch (Exception e) {
            startuplogger.debug(e);
            startuplogger.info("Although the Cleanup process did not complete normally, the server can operate normally.");
            startuplogger.info("Cleanup will be executed again automatically in the future to release unused storage");
        }

        startuplogger.info(ServerConstant.SEPARATOR);
        startuplogger.info(this.getClass().getSimpleName() + " Process completed");
        startuplogger.info("Drive setup completed successfully.");

        startuplogger.debug("Threads: " + String.valueOf(maxProcessingThread));

        startuplogger.info("Total objects processed: " + String.valueOf(this.counter.get()));
        startuplogger.info("Total objects required move to another disk: " + String.valueOf(this.moved.get()));
        startuplogger.info("Total storage moved: "
                + String.format("%16.6f", Double.valueOf(totalBytesMoved.get()).doubleValue() / SharedConstant.d_gigabyte).trim()
                + " GB");

        if (this.errors.get() > 0)
            startuplogger.info("Errors: " + String.valueOf(this.errors.get()));

        if (this.notAvailable.get() > 0)
            startuplogger.debug("Not Available: " + String.valueOf(this.notAvailable.get()));

        startuplogger.debug("Duration copy: "
                + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_move) / Double.valueOf(1000)) + " secs");
        startuplogger.debug("Duration clean up: "
                + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_cleanup) / Double.valueOf(1000)) + " secs");

        startuplogger.info("Duration Total: "
                + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
        startuplogger.info(ServerConstant.SEPARATOR);

        return true;
    }

    protected RAIDZeroDriver getDriver() {
        return driver;
    }

    protected List<ServerBucket> listAllBuckets() {
        return getDriver().getVirtualFileSystemService().listAllBuckets();
    }

    private void updateDrives() {
        for (Drive drive : getDriver().getDrivesAll()) {
            if (drive.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) {
                DriveInfo info = drive.getDriveInfo();
                info.setStatus(DriveStatus.ENABLED);
                info.setOrder(drive.getConfigOrder());
                drive.setDriveInfo(info);
                getDriver().getVirtualFileSystemService().updateDriveStatus(drive);
                startuplogger.info("drive added -> " + drive.getRootDirPath());
            }
        }
    }

    private void cleanUp() {

        ExecutorService executor = null;

        startuplogger.info("5. Starting clean up step");
        startuplogger.info("The new Drives are already operational");
        startuplogger.info("This process eliminates duplicates");

        try {

            this.start_cleanup = System.currentTimeMillis();
            this.counter = new AtomicLong(0);
            this.cleaned = new AtomicLong(0);
            this.totalBytesCleaned = new AtomicLong(0);

            executor = Executors.newFixedThreadPool(this.maxProcessingThread);

            for (ServerBucket bucket : listAllBuckets()) {

                Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
                Long offset = Long.valueOf(0);
                String agentId = null;

                boolean done = false;

                while (!done) {
                    DataList<Item<ObjectMetadata>> bucketItems = getDriver().getVirtualFileSystemService().listObjects(
                            bucket.getName(), Optional.of(offset), Optional.ofNullable(pageSize), Optional.empty(),
                            Optional.ofNullable(agentId));

                    if (agentId == null)
                        agentId = bucketItems.getAgentId();

                    List<Callable<Object>> tasks = new ArrayList<>(bucketItems.getList().size());

                    for (Item<ObjectMetadata> item : bucketItems.getList()) {
                        tasks.add(() -> {
                            try {

                                this.counter.getAndIncrement();

                                if (((this.counter.get() + 1) % 50) == 0)
                                    logger.debug("scanned (clean up) so far -> " + String.valueOf(this.counter.get()));

                                if (item.isOk()) {

                                    Drive currentDrive = getCurrentDrive(item.getObject().getBucketId(),
                                            item.getObject().getObjectName());

                                    Drive newDrive = getNewDrive(item.getObject().getBucketId(), item.getObject().getObjectName());

                                    if (!newDrive.equals(currentDrive)) {
                                        try {

                                            /**
                                             * HEAD VERSION ---------------------------------------------------------
                                             */
                                            currentDrive.deleteObjectMetadata(bucket, item.getObject().getObjectName());

                                            ObjectPath path = new ObjectPath(currentDrive, item.getObject());
                                            FileUtils.deleteQuietly(path.dataFilePath().toFile());

                                            /**
                                             * PREVIOUS VERSIONS -----------------------------------------------------
                                             */
                                            if (getDriver().getVirtualFileSystemService().getServerSettings().isVersionControl()) {
                                                for (int version = 0; version < item.getObject().getVersion(); version++) {

                                                    File m = currentDrive.getObjectMetadataVersionFile(bucket,
                                                            item.getObject().getObjectName(), version);

                                                    FileUtils.deleteQuietly(m);
                                                    FileUtils.deleteQuietly(path.dataFileVersionPath(version).toFile());

                                                }
                                            }
                                            this.cleaned.getAndIncrement();

                                        } catch (Exception e) {
                                            logger.error(e, SharedConstant.NOT_THROWN);
                                            this.errors.getAndIncrement();
                                        }
                                    }
                                } else {
                                    this.notAvailable.getAndIncrement();
                                }
                            } catch (Exception e) {
                                logger.error(e, SharedConstant.NOT_THROWN);
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

                    offset += Long.valueOf(Integer.valueOf(bucketItems.getList().size()).longValue());
                    done = (bucketItems.isEOD() || (this.errors.get() > 0));
                }
            }
            try {
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
            }
        } finally {
            logger.debug("scanned (clean up) so far -> " + String.valueOf(this.counter.get()));
            startuplogger.info("cleanup completed -> "
                    + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_cleanup) / Double.valueOf(1000)) + " secs");
        }
    }

    private void copy() {

        ExecutorService executor = null;
        try {
            startuplogger.info("4. Starting to copy data");

            this.start_move = System.currentTimeMillis();
            this.errors = new AtomicLong(0);
            this.totalBytesMoved = new AtomicLong(0);
            this.counter = new AtomicLong(0);

            executor = Executors.newFixedThreadPool(maxProcessingThread);

            for (final ServerBucket bucket : getDriver().getVirtualFileSystemService().listAllBuckets()) {

                Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);

                Long offset = Long.valueOf(0);
                String agentId = null;
                boolean done = false;

                while (!done) {
                    DataList<Item<ObjectMetadata>> data = getDriver().getVirtualFileSystemService().listObjects(bucket.getName(),
                            Optional.of(offset), Optional.ofNullable(pageSize), Optional.empty(), Optional.ofNullable(agentId));

                    if (agentId == null)
                        agentId = data.getAgentId();

                    List<Callable<Object>> tasks = new ArrayList<>(data.getList().size());

                    for (Item<ObjectMetadata> item : data.getList()) {
                        tasks.add(() -> {
                            try {
                                this.counter.getAndIncrement();

                                if (((this.counter.get() + 1) % 50) == 0)
                                    logger.debug("scanned (copy) so far -> " + String.valueOf(this.counter.get()));

                                if (item.isOk()) {

                                    Drive currentDrive = getCurrentDrive(item.getObject().getBucketId(),
                                            item.getObject().getObjectName());
                                    Drive newDrive = getNewDrive(item.getObject().getBucketId(), item.getObject().getObjectName());

                                    if (!newDrive.equals(currentDrive)) {
                                        try {

                                            File newMetadata = newDrive.getObjectMetadataFile(bucket,
                                                    item.getObject().getObjectName());

                                            /**
                                             * if newMetadata is not null, it means the file was already copied
                                             */

                                            if (!newMetadata.exists()) {

                                                /**
                                                 * HEAD VERSION ---------------------------------------------------------
                                                 */

                                                /** Data */
                                                //File data_head = ((SimpleDrive) currentDrive).getObjectDataFile(
                                                 //       item.getObject().getBucketId(), item.getObject().getObjectName());
                                                
                                                ObjectPath path = new ObjectPath(currentDrive, item.getObject().getBucketId(), item.getObject().getObjectName());
                                                File data_head = path.dataFilePath().toFile();
                                                
                                                if (data_head.exists()) {
                                                    ((SimpleDrive) newDrive).putObjectDataFile(item.getObject().getBucketId(),
                                                            item.getObject().getObjectName(), data_head);
                                                    totalBytesMoved.getAndAdd(data_head.length());
                                                }

                                                /** Metadata */
                                                ObjectMetadata meta = item.getObject();
                                                meta.drive = newDrive.getName();
                                                newDrive.saveObjectMetadata(meta);
                                                this.moved.getAndIncrement();
                                                File f = currentDrive.getObjectMetadataFile(bucket, meta.getObjectName());
                                                if (f.exists())
                                                    totalBytesMoved.getAndAdd(f.length());

                                                /**
                                                 * PREVIOUS VERSIONS ---------------------------------------------------------
                                                 */

                                                if (getDriver().getVirtualFileSystemService().getServerSettings()
                                                        .isVersionControl()) {
                                                    for (int n = 0; n < item.getObject().getVersion(); n++) {
                                                        // move Meta Version
                                                        File meta_version_n = currentDrive.getObjectMetadataVersionFile(bucket,
                                                                item.getObject().getObjectName(), n);
                                                        if (meta_version_n.exists()) {
                                                            newDrive.putObjectMetadataVersionFile(bucket,
                                                                    item.getObject().getObjectName(), n, meta_version_n);
                                                            totalBytesMoved.getAndAdd(meta_version_n.length());
                                                        }
                                                        
                                                        // move Data Version
                                                        File version_n = path.dataFileVersionPath(n).toFile();
                                                        
                                                        //File version_n = ((SimpleDrive) currentDrive).getObjectDataVersionFile(
                                                        //        item.getObject().getBucketId(), item.getObject().getObjectName(),
                                                        //       n);
                                                        
                                                        
                                                        
                                                        if (version_n.exists()) {
                                                            ((SimpleDrive) newDrive).putObjectDataVersionFile(
                                                                    item.getObject().getBucketId(),
                                                                    item.getObject().getObjectName(), n, version_n);
                                                            totalBytesMoved.getAndAdd(version_n.length());
                                                        }
                                                    }
                                                }

                                            }

                                        } catch (Exception e) {
                                            logger.error(e, SharedConstant.NOT_THROWN);
                                            this.errors.getAndIncrement();
                                        }
                                    }
                                } else {
                                    this.notAvailable.getAndIncrement();
                                }
                            } catch (Exception e) {
                                logger.error(e, SharedConstant.NOT_THROWN);
                                this.errors.getAndIncrement();
                            }
                            return null;
                        });
                    }

                    try {
                        executor.invokeAll(tasks, 15, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        logger.error(e, SharedConstant.NOT_THROWN);
                    }

                    offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
                    done = (data.isEOD() || (this.errors.get() > 0) || (this.notAvailable.get() > 0));
                }
            }

            try {
                executor.shutdown();
                executor.awaitTermination(15, TimeUnit.MINUTES);

            } catch (InterruptedException e) {
            }

        } finally {
            startuplogger.info("move completed -> "
                    + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_move) / Double.valueOf(1000)) + " secs");
        }
    }

    private Drive getCurrentDrive(Long bucketId, String objectName) {
        return this.listEnabledBefore.get(Math.abs(objectName.hashCode() % listEnabledBefore.size()));
    }

    private Drive getNewDrive(Long bucketId, String objectName) {
        return this.listAllBefore.get(Math.abs(objectName.hashCode() % listAllBefore.size()));
    }

    private void createBuckets() {
        List<ServerBucket> list = getDriver().getVirtualFileSystemService().listAllBuckets();
        startuplogger.info("3. Creating " + String.valueOf(list.size()) + " Bucket" + (list.size() > 1 ? "s" : ""));
        for (ServerBucket bucket : list) {
            for (Drive drive : getDriver().getDrivesAll()) {
                if (drive.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) {
                    try {
                        if (!drive.existsBucketById(bucket.getId())) {
                            BucketMetadata meta = bucket.getBucketMetadata();
                            drive.createBucket(meta);
                        }
                    } catch (Exception e) {
                        this.errors.getAndIncrement();
                        logger.error(e, SharedConstant.NOT_THROWN);
                        return;
                    }
                }
            }
        }
    }

}
