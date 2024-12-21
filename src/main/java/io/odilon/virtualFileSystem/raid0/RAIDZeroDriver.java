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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.query.BucketIteratorService;
import io.odilon.scheduler.AbstractServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.BaseIODriver;
import io.odilon.virtualFileSystem.Context;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.OdilonBucket;
import io.odilon.virtualFileSystem.OdilonObject;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.BucketIterator;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveBucket;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * <b>RAID 0. Stripped Disks.</b>
 * </p>
 * <p>
 * Two or more disks are combined to form a volume, which appears as a single
 * virtual drive. It is not aconfiguration with data replication, its function
 * is to provide greater storage and performance by allowing access to the disks
 * in parallel.
 * </p>
 * <p>
 * All buckets <b>must</b> exist on all drives. If a bucket is not present on a
 * drive -> the Bucket is considered <i>"non existent"</i>.<br/>
 * Each file is stored only on 1 Drive in RAID 0. If a file does not have the
 * file's Metadata Directory -> the file is considered <i>"non existent"</i>.
 * </p>
 * <p>
 * NOTE:- There are no {@link Drive} in mode {@link DriveStatus.NOTSYNC} in RAID
 * 0. All new drives are synchronized before the
 * {@link VirtualFileSystemService} completes its initialization.
 * </p>
 * <p>
 * This Class is works as a
 * <a href="https://en.wikipedia.org/wiki/Facade_pattern">Facade pattern</a>
 * that uses {@link RAIDZeroCreateObjectHandler},
 * {@link RAIDZeroDeleteObjectHandler}, {@link RAIDZeroUpdateObjectHandler} and
 * other
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
@Component
@Scope("prototype")
public class RAIDZeroDriver extends BaseIODriver implements ApplicationContextAware {

    static private Logger logger = Logger.getLogger(RAIDZeroDriver.class.getName());
    static private Logger std_logger = Logger.getLogger("StartupLogger");

    @JsonIgnore
    private ApplicationContext applicationContext;

    public RAIDZeroDriver(VirtualFileSystemService virtualFileSystemService, LockService vfsLockService) {
        super(virtualFileSystemService, vfsLockService);
    }

    @Override
    public boolean hasVersions(ServerBucket bucket, String objectName) {
        return !getObjectMetadataVersionAll(bucket, objectName).isEmpty();
    }
    /**
     * <p>
     * Delete all versions older than the current <b>head version</b>. <br/>
     * <b>IMPORTANT</b>. It does not delete the current head version. <br/>
     * <br/>
     * If the current <b>head version</b> does not have previous versions it does
     * nothing.
     * </p>
     * 
     * @see {@link RAIDZeroUpdateObjectHandler}
     */
    @Override
    public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        Check.requireNonNullArgument(meta.bucketId, "bucketId is null");
        Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketId.toString());
        RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
        agent.deleteObjectAllPreviousVersions(meta);
    }
    /**
     * <p>
     * Restores the version that is previous to the current <b>head
     * version</b>.<br/>
     * The previous version becomes the new head version, and the current head
     * version is deleted.If the current <b>head version</b> does not have previous
     * version it does nothing.
     * </p>
     * 
     * @see {@link RAIDZeroUpdateObjectHandler}
     */
    @Override
    public ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucket.getName());
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        RAIDZeroUpdateObjectHandler agent = new RAIDZeroUpdateObjectHandler(this);
        return agent.restorePreviousVersion(bucket, objectName);
    }
    /**
     * <p>
     * Creates a ServiceRequest to walk through all objects and delete versions.
     * This process is Async and handler returns immediately.
     * </p>
     * <p>
     * The ServiceRequest {@link DeleteBucketObjectPreviousVersionServiceRequest}
     * creates N Threads to scan all Objects and remove previous versions. In case
     * of failure (for example. the server is shutdown before completion), it is
     * retried up to 5 times.
     * </p>
     * <p>
     * Although the removal of all versions for every Object is Transactional, the
     * ServiceRequest itself is not Transactional, and it can not be Rollback
     * </p>
     */
    @Override
    public void wipeAllPreviousVersions() {
        RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
        agent.wipeAllPreviousVersions();
    }
    /**
     * <p>
     * Creates a ServiceRequest to walk through all objects and delete versions.
     * This process is Async and handler returns immediately.
     * </p>
     * <p>
     * The ServiceRequest {@link DeleteBucketObjectPreviousVersionServiceRequest}
     * creates N Threads to scan all Objects and remove previous versions. In case
     * of failure (for example. the server is shutdown before completion), it is
     * retried up to 5 times.
     * </p>
     * <p>
     * Although the removal of all versions for every Object is Transactional, the
     * ServiceRequest itself is not Transactional, and it can not be Rollback
     * </p>
     */
    @Override
    public void deleteBucketAllPreviousVersions(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(bucket, "bucket does not exist ->" + objectInfo(bucket));
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
        agent.deleteBucketAllPreviousVersions(bucket);
    }

    @Override
    public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType,
            Optional<List<String>> customTags) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullStringArgument(objectName, "objectName can not be null " + objectInfo(bucket));
        Check.requireNonNullStringArgument(fileName, "fileName is null " + objectInfo(bucket, objectName));
        Check.requireNonNullArgument(stream, "InpuStream can not null " + objectInfo(bucket, objectName));
        if (exists(bucket, objectName)) {
            RAIDZeroUpdateObjectHandler updateAgent = new RAIDZeroUpdateObjectHandler(this);
            updateAgent.update(bucket, objectName, stream, fileName, contentType, customTags);
            getSystemMonitorService().getUpdateObjectCounter().inc();
        } else {
            RAIDZeroCreateObjectHandler createAgent = new RAIDZeroCreateObjectHandler(this);
            createAgent.create(bucket, objectName, stream, fileName, contentType, customTags);
            getSystemMonitorService().getCreateObjectCounter().inc();
        }
    }
    /**
     * <p>
     * This method is called only for Objects that already exist
     * </p>
     */
    @Override
    public void putObjectMetadata(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDZeroUpdateObjectHandler updateAgent = new RAIDZeroUpdateObjectHandler(this);
        updateAgent.updateObjectMetadata(meta);
    }

    @Override
    public void delete(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
        RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
        agent.delete(bucket, objectName);
    }
    /**
     * <p>
     * This method is executed Async by the {@link SchedulerService} to cleanup work
     * files after a Object is deleted
     * </p>
     */
    @Override
    public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDZeroDeleteObjectHandler createAgent = new RAIDZeroDeleteObjectHandler(this);
        createAgent.postObjectDelete(meta, headVersion);
    }
    /**
     * <p>
     * This method is executed Async by the {@link SchedulerService}
     * </p>
     */
    @Override
    public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta, int headVersion) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDZeroDeleteObjectHandler createAgent = new RAIDZeroDeleteObjectHandler(this);
        createAgent.postObjectPreviousVersionDeleteAll(meta, headVersion);
    }
    /**
     * <p>
     * Set up a new drive
     * </p>
     * 
     * @see {@link RAIDZeroDriveSetupSync}
     */
    @Override
    public boolean setUpDrives() {
        return getApplicationContext().getBean(RAIDZeroDriveSetupSync.class, this).setup();
    }
    /**
     * <p>
     * RAID 0 -> Bucket must be empty on all Disks VFSBucket bucket must exist and
     * be ENABLED
     * </p>
     */
    @Override
    public boolean isEmpty(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        bucketReadLock(bucket);
        try {
            /**
             * This check was executed by the VirtualFilySystemService, but it must be
             * executed also inside the critical zone.
             */
            if (!existsCacheBucket(bucket.getName()))
                throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

            for (Drive drive : getDrivesEnabled()) {
                if (!drive.isEmpty(bucket))
                    return false;
            }
            return true;

        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket));
        } finally {
            bucketReadUnLock(bucket);
        }
    }
    /**
     * <p>
     * The object must be in status {@code BucketStatus.ENABLED} or
     * {@code BucketStatus.ARCHIVED}. If the object is {@code BucketStatus.DELETED}
     * -> it will be purged from the system at some point. The normal use case is to
     * check {@code exists} before calling this method.
     * </p>
     */
    @Override
    public VirtualFileSystemObject getObject(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty " + objectInfo(bucket));

        objectReadLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try {
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(bucket.getName()))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

                ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));
                
                return new OdilonObject(bucket, objectName, getVirtualFileSystemService());

            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }
    /**
     * <p>
     * RAID 0 -> object is stored only on 1 Drive
     * </p>
     */
    @Override
    public boolean exists(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty " + objectInfo(bucket));

        objectReadLock(bucket, objectName);
        try {
            bucketReadLock(bucket);
            try {
                /** must be executed also inside the critical zone. */
                if (!existsCacheBucket(bucket))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));
                
                /** must be executed also inside the critical zone. */
                if (getObjectMetadataCacheService().containsKey(bucket, objectName))
                    return true;
                    
                /** TBA chequear que no este "deleted" en el drive */
                return getDrive(bucket, objectName).existsObjectMetadata(bucket, objectName);
                
                
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }
    
    /**
     * <p>
     * Returns a {@link DataList} of {@link Items} <br/>
     * The list contained by the {@link DataList} has <code>pageSize</code> items
     * starting from <code>offset</code> (first element is 0), or less if there are
     * not enough items.
     * 
     * {@link Item} is a wrapper of an {@link ObjectMetadata} or an error. The items
     * in the DataList are not ordered.<br/>
     * 
     * @param serverAgentId is an optional Id that works as a cache of the object
     *                      that is generating the pages for this query.
     *                      {@link BucketIteratorService} contains a cache of
     *                      {@link BucketIterator} for ongoing queries.
     * 
     * @param prefix        of the objectname
     *                      </p>
     */
    @Override
    public DataList<Item<ObjectMetadata>> listObjects(ServerBucket bucket, Optional<Long> offset, Optional<Integer> pageSize,
            Optional<String> prefix, Optional<String> serverAgentId) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));

        BucketIterator iterator = null;
        try {
            /**
             * serverAgentId works as a cache of the object that is generating 
             * the pages of the query
             */
            if (serverAgentId.isPresent())
                iterator = getBucketIteratorService().get(serverAgentId.get());

            if (iterator == null) {
                iterator = new RAIDZeroBucketIterator(this, bucket, offset, prefix);
                getBucketIteratorService().register(iterator);
            }
            List<Item<ObjectMetadata>> list = new ArrayList<Item<ObjectMetadata>>();
            int size = pageSize.orElseGet(() -> ServerConstant.DEFAULT_PAGE_SIZE);
            int counter = 0;
            while (iterator.hasNext() && (counter++ < size)) {
                Item<ObjectMetadata> item;
                try {
                    ObjectMetadata meta = getOM(bucket, iterator.next().toFile().getName(), Optional.empty(), false);
                    item = new Item<ObjectMetadata>(meta);
                } catch (Exception e) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                    item = new Item<ObjectMetadata>(e);
                }
                list.add(item);
            }
            DataList<Item<ObjectMetadata>> result = new DataList<Item<ObjectMetadata>>(list);
            /**
             * EOD (End of Data) is used to prevent the client to send a new Request after
             * returning the last page of the result
             */
            if (!iterator.hasNext())
                result.setEOD(true);

            result.setOffset(iterator.getOffset());
            result.setPageSize(size);
            result.setAgentId(iterator.getAgentId());
            return result;
        } finally {
            if (iterator != null && (!iterator.hasNext())) {
                /** removing from IteratorService closes the stream */
                getBucketIteratorService().remove(iterator.getAgentId());
            }
        }
    }
    /**
     * <p>
     * It returns ObjectMetadata of all previous versions it does not include head
     * version
     * </p>
     */
    @Override
    public List<ObjectMetadata> getObjectMetadataVersionAll(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty " + objectInfo(bucket));
        
        List<ObjectMetadata> list = null;
        Drive readDrive = null;

        objectReadLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try {
                
                list = new ArrayList<ObjectMetadata>();
                /** must be executed also inside the critical zone */
                if (!existsCacheBucket(bucket.getName()))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

                /** read is from only 1 drive */
                readDrive = getReadDrive(bucket, objectName);

                ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(ObjectMetadata.class.getSimpleName());

                if (meta.getVersion() == 0)
                    return list;

                for (int version = 0; version < meta.getVersion(); version++) {
                    ObjectMetadata meta_version = readDrive.getObjectMetadataVersion(bucket, objectName, version);
                    if (meta_version != null) {
                        // bucketName is not stored on disk, only bucketId, we must set it explicitly
                        meta_version.setBucketName(bucket.getName());
                        list.add(meta_version);
                    }
                }
                return list;
                
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (OdilonObjectNotFoundException e) {
                e.setErrorMessage((e.getMessage() != null ? (e.getMessage() + " | ") : "") + objectInfo(bucket, objectName));
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    @Override
    public ObjectMetadata getObjectMetadata(ServerBucket bucket, String objectName) {
        return getOM(bucket, objectName, Optional.empty(), true);
    }

    @Override
    public ObjectMetadata getObjectMetadataVersion(ServerBucket bucket, String objectName, int version) {
        return getOM(bucket, objectName, Optional.of(Integer.valueOf(version)), true);
    }
    /**
     * <p>
     * If version does not exist -> throws OdilonObjectNotFoundException
     * </p>
     */
    @Override
    public InputStream getObjectVersionInputStream(ServerBucket bucket, String objectName, int version) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty " + objectInfo(bucket));

        objectReadLock(bucket, objectName);
        try {
            bucketReadLock(bucket);
            try {
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(bucket))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

                /** RAID 0: read is from only 1 drive */
                Drive readDrive = getReadDrive(bucket, objectName);

                ObjectMetadata meta = getObjectMetadataVersion(bucket, objectName, version);

                if ((meta != null) && meta.isAccesible()) {
                    ObjectPath path = new ObjectPath(readDrive, bucket, objectName);
                    InputStream stream = Files.newInputStream(path.dataFileVersionPath(version));
                    if (meta.isEncrypt())
                        return getEncryptionService().decryptStream(stream);
                    else
                        return stream;
                } else
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName, version));

            } catch (IllegalArgumentException e) {
                throw e;
            } catch (OdilonObjectNotFoundException e) {
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }
    /**
     * <p>
     * <b>IMPORTANT</b> -> caller must close the {@link InputStream} returned
     * </p>
     * 
     * @param bucketName
     * @param objectName
     * @return
     * 
     */
    @Override
    public InputStream getInputStream(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty " + objectInfo(bucket));

        objectReadLock(bucket, objectName);
        try {
            bucketReadLock(bucket);
            try {
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(bucket))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

                /** RAID 0: read is from only 1 drive */
                Drive readDrive = getReadDrive(bucket, objectName);

                ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException( objectInfo(bucket, objectName));
                
                ObjectPath path = new ObjectPath(readDrive, bucket, objectName);
                InputStream stream = Files.newInputStream(path.dataFilePath(Context.STORAGE));

                return (meta.isEncrypt()) ? getEncryptionService().decryptStream(stream) : stream;
                

            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    /**
     * <p>
     * RAID 0. Journal Files go to drive 0
     * </p>
     */
    @Override
    public List<VirtualFileSystemOperation> getJournalPending(JournalService journalService) {

        List<VirtualFileSystemOperation> list = new ArrayList<VirtualFileSystemOperation>();
        Drive drive = getDrivesEnabled().get(0);

        File dir = new File(drive.getJournalDirPath());

        if (!dir.exists())
            return list;

        if (!dir.isDirectory())
            return list;

        File[] files = dir.listFiles();

        if (files.length == 0)
            return list;

        for (File file : files) {

            if (!file.isDirectory()) {
                Path pa = Paths.get(file.getAbsolutePath());
                try {
                    String str = Files.readString(pa);
                    OdilonVirtualFileSystemOperation operation = getObjectMapper().readValue(str, OdilonVirtualFileSystemOperation.class);
                    operation.setJournalService(getJournalService());
                    list.add(operation);
                } catch (IOException e) {
                    logger.debug(e, fileInfo(file));
                    try {
                        Files.delete(file.toPath());
                    } catch (IOException e1) {
                        logger.error(e, SharedConstant.NOT_THROWN);
                    }
                }
            }
        }
        std_logger.info("Rollback -> " + String.valueOf(list.size()) + " transactions");
        return list;
    }
    /**
     * <p>
     * before starting operations load Requests that are stored on disk
     * </p>
     */
    @Override
    public List<ServiceRequest> getSchedulerPendingRequests(String queueId) {

        List<ServiceRequest> list = new ArrayList<ServiceRequest>();

        Drive drive = getDrivesEnabled().get(0);

        for (File file : drive.getSchedulerRequests(queueId)) {
            try {
                list.add((ServiceRequest) getObjectMapper().readValue(file, AbstractServiceRequest.class));
            } catch (IOException e) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e1) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                }
            }
        }
        return list;
    }
    /**
     * <p>
     * RAID 0. Journal Files go to drive 0
     * </p>
     */
    @Override
    public void saveJournal(VirtualFileSystemOperation operation) {
        getDrivesEnabled().get(0).saveJournal(operation);
    }
    /**
     * <p>
     * RAID 0. Journal Files go to drive 0
     * </p>
     */
    @Override
    public void removeJournal(String id) {
        getDrivesEnabled().get(0).removeJournal(id);
    }

    public void rollbackJournal(VirtualFileSystemOperation operation) {
        rollbackJournal(operation, false);
    }

    /**
     * <p>
     * Rollback from Journal
     * 
     * Required locks must be applied before calling this method
     * 
     * </p>
     */
    @Override
    public void rollbackJournal(VirtualFileSystemOperation operation, boolean recoveryMode) {

        Check.requireNonNullArgument(operation, "operation is null");

        switch (operation.getOperationCode()) {
        case CREATE_OBJECT: {
            RAIDZeroCreateObjectHandler handler = new RAIDZeroCreateObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case UPDATE_OBJECT: {
            RAIDZeroUpdateObjectHandler handler = new RAIDZeroUpdateObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case DELETE_OBJECT: {
            RAIDZeroDeleteObjectHandler handler = new RAIDZeroDeleteObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case DELETE_OBJECT_PREVIOUS_VERSIONS: {
            RAIDZeroDeleteObjectHandler handler = new RAIDZeroDeleteObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case RESTORE_OBJECT_PREVIOUS_VERSION: {
            RAIDZeroUpdateObjectHandler handler = new RAIDZeroUpdateObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case UPDATE_OBJECT_METADATA: {
            RAIDZeroUpdateObjectHandler handler = new RAIDZeroUpdateObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        default:
            break;
        }

        boolean done = false;

        try {

            if (getServerSettings().isStandByEnabled())
                        getReplicationService().cancel(operation);

            if (operation.getOperationCode() == OperationCode.CREATE_BUCKET) {

                done = generalRollbackJournal(operation);

            } else if (operation.getOperationCode() == OperationCode.DELETE_BUCKET) {

                done = generalRollbackJournal(operation);

            } else if (operation.getOperationCode() == OperationCode.UPDATE_BUCKET) {

                done = generalRollbackJournal(operation);
            }
            if (operation.getOperationCode() == OperationCode.CREATE_SERVER_MASTERKEY) {

                done = generalRollbackJournal(operation);

            } else if (operation.getOperationCode() == OperationCode.CREATE_SERVER_METADATA) {

                done = generalRollbackJournal(operation);

            } else if (operation.getOperationCode() == OperationCode.UPDATE_SERVER_METADATA) {

                done = generalRollbackJournal(operation);
            }

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(opInfo(operation), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(operation));
            else
                logger.error(opInfo(operation), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                operation.cancel();
            } else {
                if (getServerSettings().isRecoverMode()) {
                    logger.error("---------------------------------------------------------------");
                    logger.error("Cancelling failed operation -> " + opInfo(operation));
                    logger.error("---------------------------------------------------------------");
                    operation.cancel();
                }
            }
        }
    }

    @Override
    public void saveServerMasterKey(byte[] key, byte[] hmac, byte[] iv, byte[] salt) {

        Check.requireNonNullArgument(key, "key is null");
        Check.requireNonNullArgument(salt, "salt is null");

        boolean done = false;
        boolean reqRestoreBackup = false;

        VirtualFileSystemOperation op = null;

        getLockService().getServerLock().writeLock().lock();

        try {
            /** backup */
            for (Drive drive : getDrivesAll()) {
                try {
                    // drive.putSysFile(ServerConstant.ODILON_SERVER_METADATA_FILE, jsonString);
                    // backup
                } catch (Exception e) {
                    // isError = true;
                    reqRestoreBackup = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }

            op = getJournalService().saveServerKey();

            reqRestoreBackup = true;

            Exception eThrow = null;

            byte[] data = new byte[hmac.length + iv.length + key.length + salt.length];

            /** HMAC(32) + Master Key (16) + IV(12) + Salt (64) */
            System.arraycopy(hmac, 0, data, 0, hmac.length);
            System.arraycopy(key, 0, data, hmac.length, key.length);
            System.arraycopy(iv, 0, data, (hmac.length + key.length), iv.length);
            System.arraycopy(salt, 0, data, (hmac.length + iv.length + key.length), salt.length);

            byte[] dataEnc = getVirtualFileSystemService().getMasterKeyEncryptorService().encryptKey(data, iv);

            /** save */
            for (Drive drive : getDrivesAll()) {
                try {
                    File file = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
                    FileUtils.writeByteArrayToFile(file, dataEnc);
                } catch (Exception e) {
                    eThrow = new InternalCriticalException(e, "Drive -> " + drive.getName());
                    break;
                }
            }

            if (eThrow != null)
                throw eThrow;

            done = op.commit();

        } catch (InternalCriticalException e) {
            throw e;

        } catch (Exception e) {
            if (logger.isDebugEnabled())
                logger.error(e, SharedConstant.NOT_THROWN);
            throw new InternalCriticalException(e, "saveServerMasterKey");

        } finally {
            try {
                if (!done) {
                    if (!reqRestoreBackup)
                        op.cancel();
                    else
                        rollbackJournal(op);
                }

            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                getLockService().getServerLock().writeLock().unlock();
            }
        }
    }

    @Override
    public boolean checkIntegrity(ServerBucket bucket, String objectName, boolean forceCheck) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null for b:" + bucket.getName());

        OffsetDateTime thresholdDate = OffsetDateTime.now()
                .minusDays(getServerSettings().getIntegrityCheckDays());

        Drive readDrive = null;
        ObjectMetadata metadata = null;

        boolean objectLock = false;
        boolean bucketLock = false;

        try {
            try {
                objectLock = getLockService().getObjectLock(bucket, objectName).readLock().tryLock(30, TimeUnit.SECONDS);
                if (!objectLock) {
                    logger.warn("Can not acquire read Lock for o: " + objectName + ". Assumes -> check is ok");
                    return true;
                }
            } catch (InterruptedException e) {
                return true;
            }

            try {
                bucketLock = getLockService().getBucketLock(bucket).readLock().tryLock(30, TimeUnit.SECONDS);
                if (!bucketLock) {
                    logger.warn("Can not acquire read Lock for b: " + bucket.getName() + ". Assumes -> check is ok");
                    return true;
                }
            } catch (InterruptedException e) {
                logger.warn(e);
                return true;
            }

            /**
             * For RAID 0 the check is in the head version there is no way to fix a damaged
             * file the goal of this process is to warn that a Object is damaged
             **/
            readDrive = getReadDrive(bucket, objectName);
            metadata = readDrive.getObjectMetadata(bucket, objectName);

            if ((forceCheck) || (metadata.getIntegrityCheck() != null) && (metadata.getIntegrityCheck().isAfter(thresholdDate))) {
                return true;
            }

            OffsetDateTime now = OffsetDateTime.now();

            String originalSha256 = metadata.getSha256();

            if (originalSha256 == null) {
                metadata.setIntegrityCheck(now);
                getVirtualFileSystemService().getObjectMetadataCacheService().remove(metadata.getBucketId(),
                        metadata.getObjectName());
                readDrive.saveObjectMetadata(metadata);
                return true;
            }

            ObjectPath path = new ObjectPath(readDrive, bucket.getId(), objectName);
            File file = path.dataFilePath().toFile();
            String sha256 = null;

            try {
                sha256 = OdilonFileUtils.calculateSHA256String(file);

            } catch (NoSuchAlgorithmException | IOException e) {
                logger.error(e, SharedConstant.NOT_THROWN);
                return false;
            }

            if (originalSha256.equals(sha256)) {
                metadata.integrityCheck = now;
                readDrive.saveObjectMetadata(metadata);
                return true;
            } else {
                logger.error("Integrity Check failed -> " + objectInfo(bucket, objectName, readDrive), SharedConstant.NOT_THROWN);
            }
            /**
             * it is not possible to fix the file if the integrity fails because there is no
             * redundancy in RAID 0
             **/
            return false;

        } finally {

            try {
                if (objectLock)
                    getLockService().getObjectLock(bucket, objectName).readLock().unlock();
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }

            try {
                if (bucketLock)
                    getLockService().getBucketLock(bucket).readLock().unlock();
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }

        }
    }

    @Override
    public RedundancyLevel getRedundancyLevel() {
        return RedundancyLevel.RAID_0;
    }

    /**
     * Scheduler goes to drive 0
     */
    @Override
    public void saveScheduler(ServiceRequest request, String queueId) {
        getDrivesEnabled().get(0).saveScheduler(request, queueId);
    }

    @Override
    public void removeScheduler(ServiceRequest request, String queueId) {
        getDrivesEnabled().get(0).removeScheduler(request, queueId);
    }
    
    @Override
    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    @Override
    public OdilonServerInfo getServerInfo() {
        try {
            getLockService().getServerLock().readLock().lock();
            File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
            if (file == null || !file.exists())
                return null;
            return getObjectMapper().readValue(file, OdilonServerInfo.class);
        } catch (IOException e) {
            logger.error(e, SharedConstant.NOT_THROWN);
            throw new InternalCriticalException(e);

        } finally {
            getLockService().getServerLock().readLock().unlock();
        }
    }

    @Override
    public void setServerInfo(OdilonServerInfo serverInfo) {

        Check.requireNonNullArgument(serverInfo, "serverInfo is null");

        if (getServerInfo() == null) {
            saveNewServerInfo(serverInfo);
            return;
        }

        boolean done = false;
        boolean mayReqRestoreBackup = false;
        VirtualFileSystemOperation op = null;

        getLockService().getServerLock().writeLock().lock();
        try {
            op = getJournalService().updateServerMetadata();
            String jsonString = getObjectMapper().writeValueAsString(serverInfo);

            for (Drive drive : getDrivesAll()) {
                try {
                    // drive.putSysFile(ServerConstant.ODILON_SERVER_METADATA_FILE, jsonString);
                    // backup
                } catch (Exception e) {
                    done = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }

            mayReqRestoreBackup = true;

            for (Drive drive : getDrivesAll()) {
                try {
                    drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
                } catch (Exception e) {
                    done = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }
            done = op.commit();

        } catch (Exception e) {
            logger.error(e, serverInfo.toString());
            throw new InternalCriticalException(e, serverInfo.toString());

        } finally {
            try {
                if (!mayReqRestoreBackup) {
                    op.cancel();
                } else if (!done) {
                    rollbackJournal(op);
                }
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                getLockService().getServerLock().writeLock().unlock();
            }
        }
    }

    @Override
    public void syncObject(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        logger.error("not done", SharedConstant.NOT_THROWN);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
        return getReadDrive(bucket, objectName);
    }

    /**
     * RAID 0 -> read drive and write drive are the same
     */
    protected Drive getWriteDrive(ServerBucket bucket, String objectName) {
        return getDrive(bucket, objectName);
    }

    /**
     * <p>
     * RAID 0 -> all enabled Drives have all buckets
     * </p>
     */
    @Override
    protected Map<String, ServerBucket> getBucketsMap() {

        Map<String, ServerBucket> map = new HashMap<String, ServerBucket>();
        Map<String, Integer> control = new HashMap<String, Integer>();

        int totalDrives = getDrivesEnabled().size();

        for (Drive drive : getDrivesEnabled()) {
            for (DriveBucket bucket : drive.getBuckets()) {
                if (bucket.getStatus().isAccesible()) {
                    String name = bucket.getName();
                    Integer count;
                    if (control.containsKey(name))
                        count = control.get(name) + 1;
                    else
                        count = Integer.valueOf(1);
                    control.put(name, count);
                }
            }
        }

        // any drive is ok because all have all the buckets
        Drive drive = getDrivesEnabled().get(0);
        for (DriveBucket bucket : drive.getBuckets()) {
            String name = bucket.getName();
            if (control.containsKey(name)) {
                Integer count = control.get(name);
                if (count == totalDrives) {
                    ServerBucket vfsbucket = new OdilonBucket(bucket);
                    map.put(vfsbucket.getName(), vfsbucket);
                }
            }
        }
        return map;
    }

    /**
     * <p>
     * RAID 0. there is only 1 Drive for each object
     * </p>
     * 
     * @param bucketName
     * @param objectName
     * @return
     */
    protected Drive getReadDrive(ServerBucket bucket, String objectName) {
        return getDrive(bucket, objectName);
    }

    protected Drive getReadDrive(ServerBucket bucket) {
        return getDrive(bucket, null);
    }

    protected Drive getDrive(ServerBucket bucket, String objectName) {
        return getDrivesEnabled().get(Math.abs(objectName.hashCode() % getDrivesEnabled().size()));
    }

    protected String fileInfo(File file) {
        if (file == null)
            return "f:null";
        return "f:" + file.getName();
    }

    private void saveNewServerInfo(OdilonServerInfo serverInfo) {
        boolean done = false;
        VirtualFileSystemOperation op = null;
        getLockService().getServerLock().writeLock().lock();
        try {
            op = getJournalService().createServerMetadata();
            String jsonString = getObjectMapper().writeValueAsString(serverInfo);

            for (Drive drive : getDrivesAll()) {
                try {
                    drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
                } catch (Exception e) {
                    done = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }
            done = op.commit();

        } catch (InternalCriticalException e) {
            throw (e);

        } catch (Exception e) {
            throw new InternalCriticalException(e, serverInfo.toString());

        } finally {
            try {
                if (!done) {
                    rollbackJournal(op);
                }
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                getLockService().getServerLock().writeLock().unlock();
            }
        }
    }

    /**
     * <p>
     * If the bucket does not exist on the selected Drive -> the system is in an
     * illegal state
     * </p>
     */
    private ObjectMetadata getOM(ServerBucket bucket, String objectName, Optional<Integer> o_version, boolean addToCacheifMiss) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName can not be null " + objectInfo(bucket));

        Drive readDrive = null;

        objectReadLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try {
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(bucket.getName()))
                    throw new IllegalArgumentException("bucket does not exist -> " + bucket.getName());

                /** read is from only 1 drive */
                readDrive = getReadDrive(bucket, objectName);

                ObjectMetadata meta = null;

                if (o_version.isPresent()) {
                    meta = readDrive.getObjectMetadataVersion(bucket, objectName, o_version.get());
                    meta.setBucketName(bucket.getName());
                } else {
                    meta = getDriverObjectMetadataInternal(bucket, objectName, addToCacheifMiss);
                }

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

                return meta;


            } catch (IllegalArgumentException e) {
                throw e;
            } catch (OdilonObjectNotFoundException e) {
                e.setErrorMessage(objectInfo(bucket, objectName));
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }
}
