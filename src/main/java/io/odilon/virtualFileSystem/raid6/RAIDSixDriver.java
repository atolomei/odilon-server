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
package io.odilon.virtualFileSystem.raid6;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.query.BucketIteratorService;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.BaseIODriver;
import io.odilon.virtualFileSystem.OdilonObject;
import io.odilon.virtualFileSystem.model.BucketIterator;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * RAID 6. Driver
 * </p>
 * <p>
 * The coding convention for RS blocks is:
 * <ul>
 * <li><b>objectName.[chunk#].[block#]</b></li>
 * <li><b>objectName.[chunk#].[block#].v[version#]</b></li>
 * </ul>
 * where: <br/>
 * <ul>
 * <li><b>chunk#</b><br/>
 * 0..total_chunks, depending of the size of the file to encode
 * ({@link ServerConstant.MAX_CHUNK_SIZE} is 32 MB) this means that for files
 * smaller or equal to 32 MB there will be only one chunk (chunk=0), for files
 * up to 64 MB there will be 2 chunks and so on. <br/>
 * <br/>
 * </li>
 * <li><b>block#</b><br/>
 * is the disk order [0..(data+parity-1)] <br/>
 * <br/>
 * </li>
 * <li><b>version#</b><br/>
 * is omitted for head version. <br/>
 * <br/>
 * </li>
 * </ul>
 * <p>
 * The total number of files once the src file is encoded are: <br/>
 * <br/>
 * (data+parity) * (file_size / MAX_CHUNK_SIZE ) rounded to the following
 * integer. Examples:
 * </p>
 * <p>
 * objectname.block#.disk# <br/>
 * <br/>
 * D:\odilon-data-raid6\drive0\bucket1\TOLOMEI.0.0 <br/>
 * _______________________________________________________________ <br/>
 * <br/>
 * D:\odilon-data-raid6\drive0\bucket1\TOLOMEI.0.0 <br/>
 * D:\odilon-data-raid6\drive1\bucket1\TOLOMEI.1.0 <br/>
 * D:\odilon-data-raid6\drive1\bucket1\TOLOMEI.2.0 <br/>
 * </p>
 * <p>
 * RAID 6. The only configurations supported in v1.x is -><br/>
 * <br/>
 * data shards = 2 + parity shards=1 -> 3 disks <br/>
 * data shards = 4 + parity shards=2 -> 6 disks <br/>
 * data shards = 8 + parity shards=4 -> 12 disks <br/>
 * data shards = 16 + parity shards=8 -> 24 disks <br/>
 * data shards = 32 + parity shards=16 -> 48 disks <br/>
 * </p>
 * <p>
 * All buckets <b>must</b> exist on all drives. If a bucket is not present on a
 * drive -> the bucket is considered "non existent".<br/>
 * Each file is stored only on 6 Drives. If a file does not have the file's
 * Metadata Directory -> the file is considered "non existent"
 * </p>
 * 
 * 
 * <p>
 * This Class is works as a
 * <a href="https://en.wikipedia.org/wiki/Facade_pattern">Facade pattern</a>
 * that uses {@link RAIDSixCreateObjectHandler},
 * {@link RAIDSixDeleteObjectHandler}, {@link RAIDSixUpdateObjectHandler},
 * {@link RAIDSixSyncObjectHandler} and other
 * </p>
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
@Component
@Scope("prototype")
public class RAIDSixDriver extends BaseIODriver implements ApplicationContextAware {

    static private Logger logger = Logger.getLogger(RAIDSixDriver.class.getName());

    @JsonIgnore
    private ApplicationContext applicationContext;

    public RAIDSixDriver(VirtualFileSystemService vfs, LockService vfsLockService) {
        super(vfs, vfsLockService);
    }

    @Override
    public void syncObject(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDSixSyncObjectHandler handler = new RAIDSixSyncObjectHandler(this);
        handler.sync(meta);
    }

    @Override
    public InputStream getInputStream(ServerBucket bucket, String objectName) throws IOException {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
        objectReadLock(bucket, objectName);
        try {
            bucketReadLock(bucket);
            try {
                checkIsAccesible(bucket);
                ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);
                
                if ((meta != null) && meta.isAccesible()) {
                    RAIDSixDecoder decoder = new RAIDSixDecoder(this);
                    return (meta.isEncrypt())
                            ? getVirtualFileSystemService().getEncryptionService()
                                    .decryptStream(Files.newInputStream(decoder.decodeHead(meta, bucket).toPath()))
                            : Files.newInputStream(decoder.decodeHead(meta, bucket).toPath());
                }
                throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));
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
     * 
     * 
     */
    @Override
    public InputStream getObjectVersionInputStream(ServerBucket bucket, String objectName, int version) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
        objectReadLock(bucket, objectName);
        try {
            bucketReadLock(bucket);
            try {
                checkIsAccesible(bucket);
                /** RAID 6: read is from any of the drives */
                Drive readDrive = getObjectMetadataReadDrive(bucket, objectName);
                ObjectMetadata meta = readDrive.getObjectMetadataVersion(bucket, objectName, version);
                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException("object version does not exists for -> b:"
                            + objectInfo(bucket, objectName) + " | v:" + String.valueOf(version));
                RAIDSixDecoder decoder = new RAIDSixDecoder(this);
                File file = decoder.decodeVersion(meta, bucket);

                if (meta.isEncrypt())
                    return getVirtualFileSystemService().getEncryptionService().decryptStream(Files.newInputStream(file.toPath()));
                else
                    return Files.newInputStream(file.toPath());
            } catch (OdilonObjectNotFoundException e) {
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName) + " | v:" + String.valueOf(version));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    /**
     * <p>
     * falta completar
     * </p>
     */
    @Override
    public boolean checkIntegrity(ServerBucket bucket, String objectName, boolean forceCheck) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null");

        
        logger.error("not completed");
        
        OffsetDateTime thresholdDate = OffsetDateTime.now()
                .minusDays(getVirtualFileSystemService().getServerSettings().getIntegrityCheckDays());

        Drive readDrive = null;
        ObjectMetadata metadata = null;

        boolean objectLock = false;
        boolean bucketLock = false;

        try {

            try {
                objectLock = getLockService().getObjectLock(bucket, objectName).readLock().tryLock(20, TimeUnit.SECONDS);
                if (!objectLock) {
                    logger.error("Can not acquire read Lock for o: " + objectName + ". Assumes -> check is ok");
                    return true;
                }
            } catch (InterruptedException e) {
                return true;
            }

            try {
                bucketLock = getLockService().getBucketLock(bucket).readLock().tryLock(20, TimeUnit.SECONDS);
                if (!bucketLock) {
                    logger.error("Can not acquire read Lock for b: " + bucket.getName() + ". Assumes -> check is ok");
                    return true;
                }
            } catch (InterruptedException e) {
                return true;
            }

            /**
             * For RAID 0 the check is in the head version there is no way to fix a damaged
             * file the goal of this process is to warn that a Object is damaged
             */
            readDrive = getObjectMetadataReadDrive(bucket, objectName);
            metadata = readDrive.getObjectMetadata(bucket, objectName);

            if ((forceCheck) || (metadata.integrityCheck != null) && (metadata.integrityCheck.isAfter(thresholdDate)))
                return true;

            return true;

            // OffsetDateTime now = OffsetDateTime.now();

            // String originalSha256 = metadata.sha256;

            // if (originalSha256 == null) {
            // metadata.integrityCheck = now;
            // getVirtualFileSystemService().getObjec tCacheService().rem
            // ove(metadata.bucketName,
            // metadata.objectName);
            // readDrive.saveObjectMetadata(metadata);
            // return true;
            // }

            // File file = ((SimpleDrive) readDrive).getObjectDataFile(bucketName,
            // objectName);
            // String sha256 = null;
            //
            // try {
            // sha256 = ODFileUtils.calculateSHA256String(file);
            //
            // } catch (NoSuchAlgorithmException | IOException e) {
            // logger.error(e);
            // return false;
            // }
            //
            // if (originalSha256.equals(sha256)) {
            // metadata.integrityCheck = now;
            // readDrive.saveObjectMetadata(metadata);
            // return true;
            // } else {
            // logger.error("Integrity Check failed for -> d: " + readDrive.getName() + " |
            // b:" + bucketName + " | o:" + objectName + " | " + SharedConstant.NOT_THROWN);
            // }
            /**
             * it is not possible to fix the file if the integrity fails because there is no
             * redundancy in RAID 0
             **/
            // return false;

        } finally {

            try {
                if (bucketLock)
                    getLockService().getBucketLock(bucket).readLock().unlock();
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }

            try {
                if (objectLock)
                    getLockService().getObjectLock(bucket, objectName).readLock().unlock();
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }
        }

    }

    /**
     * 
     * <p>
     * This method is not ThreadSafe. The calling object must ensure concurrency
     * control.
     * 
     * from VFS -> there is only one Thread active from Handler -> objects are
     * locked before calling this
     * 
     */
    @Override
    public void rollbackJournal(VirtualFileSystemOperation operation, boolean recoveryMode) {

        Check.requireNonNullArgument(operation, "peration is null");

        switch (operation.getOperationCode()) {
        case CREATE_OBJECT: {
            RAIDSixCreateObjectHandler handler = new RAIDSixCreateObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case UPDATE_OBJECT: {
            RAIDSixUpdateObjectHandler handler = new RAIDSixUpdateObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case DELETE_OBJECT: {
            RAIDSixDeleteObjectHandler handler = new RAIDSixDeleteObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case DELETE_OBJECT_PREVIOUS_VERSIONS: {
            RAIDSixDeleteObjectHandler handler = new RAIDSixDeleteObjectHandler(this);
            handler.rollbackJournal(operation, recoveryMode);
            return;
        }
        case UPDATE_OBJECT_METADATA: {
            RAIDSixUpdateObjectHandler handler = new RAIDSixUpdateObjectHandler(this);
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
                logger.error(opInfo(operation));
            throw (e);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(operation));
        } finally {
            if (done || recoveryMode) {
                operation.cancel();
            } else {
                if (getVirtualFileSystemService().getServerSettings().isRecoverMode()) {
                    logger.error("---------------------------------------------------------------");
                    logger.error("Cancelling failed operation -> " + operation.toString());
                    logger.error("---------------------------------------------------------------");
                    operation.cancel();
                }
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    @Override
    public ObjectMetadata getObjectMetadata(ServerBucket bucket, String objectName) {
        return getOM(bucket, objectName, Optional.empty(), true);
    }

    /**
     * <p>
     * Invariant: all drives contain the same bucket structure
     * </p>
     */
    @Override
    public boolean exists(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

        getLockService().getObjectLock(bucket, objectName).readLock().lock();
        try {
            getLockService().getBucketLock(bucket).readLock().lock();
            try {
                checkIsAccesible(bucket);
                return getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucket, objectName);
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                getLockService().getBucketLock(bucket).readLock().unlock();
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).readLock().unlock();
        }
    }

    /**
     * 
     */
    @Override
    public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType,
            Optional<List<String>> customTags) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucket.getName());
        Check.requireNonNullStringArgument(fileName, "fileName is null | b: " + bucket.getName() + " o:" + objectName);
        Check.requireNonNullArgument(stream, "InpuStream can not null -> b:" + bucket.getName() + " | o:" + objectName);
        if (exists(bucket, objectName)) {
            RAIDSixUpdateObjectHandler updateAgent = new RAIDSixUpdateObjectHandler(this);
            updateAgent.update(bucket, objectName, stream, fileName, contentType, customTags);
            getVirtualFileSystemService().getSystemMonitorService().getUpdateObjectCounter().inc();
        } else {
            RAIDSixCreateObjectHandler createAgent = new RAIDSixCreateObjectHandler(this);
            createAgent.create(bucket, objectName, stream, fileName, contentType, customTags);
            getVirtualFileSystemService().getSystemMonitorService().getCreateObjectCounter().inc();
        }
    }

    @Override
    public void putObjectMetadata(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDSixUpdateObjectHandler updateAgent = new RAIDSixUpdateObjectHandler(this);
        updateAgent.updateObjectMetadataHeadVersion(meta);
        getVirtualFileSystemService().getSystemMonitorService().getUpdateObjectCounter().inc();
    }

    @Override
    public VirtualFileSystemObject getObject(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
        
        String bucketName = bucket.getName();
        getLockService().getObjectLock(bucket, objectName).readLock().lock();

        try {
            bucketReadLock(bucket);
            try {

                checkIsAccesible(bucket);
                
                /** must be executed also inside the critical zone. */
                if (!existsCacheBucket(bucket.getName()))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

                ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);
                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));
                
                return new OdilonObject(bucket, objectName, getVirtualFileSystemService());

            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucketName ,objectName));
            } finally {
                bucketReadUnLock(bucket);

            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).readLock().unlock();
        }
    }

    /**
     * 
     */
    @Override
    public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion) {
        Check.requireNonNullArgument(meta, "meta is null");
        String bucketName = meta.getBucketName();
        String objectName = meta.getObjectName();
        Check.requireNonNullArgument(bucketName, "bucketName is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
        RAIDSixDeleteObjectHandler deleteAgent = new RAIDSixDeleteObjectHandler(this);
        deleteAgent.postObjectDelete(meta, headVersion);
    }

    /**
     * 
     */
    @Override
    public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta, int headVersion) {
        Check.requireNonNullArgument(meta, "meta is null");
        String bucketName = meta.getBucketName();
        String objectName = meta.getObjectName();
        Check.requireNonNullArgument(bucketName, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
        RAIDSixDeleteObjectHandler deleteAgent = new RAIDSixDeleteObjectHandler(this);
        deleteAgent.postObjectPreviousVersionDeleteAll(meta, headVersion);
    }

    @Override
    public boolean hasVersions(ServerBucket bucket, String objectName) {
        return !getObjectMetadataVersionAll(bucket, objectName).isEmpty();
    }

    @Override
    public List<ObjectMetadata> getObjectMetadataVersionAll(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

        List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
        Drive readDrive = null;

        getLockService().getObjectLock(bucket, objectName).readLock().lock();

        try {
            bucketReadLock(bucket);
            try {
                
                checkIsAccesible(bucket);
                
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(bucket.getName()))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));
                
                /** read is from only 1 drive */
                readDrive = getObjectMetadataReadDrive(bucket, objectName);

                ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);
                
                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(ObjectMetadata.class.getName() + " does not exist");

                meta.setBucketName(bucket.getName());
                
                if (meta.getVersion() == 0)
                    return list;

                for (int version = 0; version < meta.getVersion(); version++) {
                    ObjectMetadata meta_version = readDrive.getObjectMetadataVersion(bucket, objectName, version);
                    if (meta_version != null) {

                        /**
                         * bucketName is not stored on disk, only bucketId, we must set it explicitly
                         */
                        meta_version.setBucketName(bucket.getName());
                        list.add(meta_version);
                    }
                }
                return list;
            } catch (OdilonObjectNotFoundException e) {
                e.setErrorMessage((e.getMessage() != null ? (e.getMessage() + " | ") : "") + objectInfo(bucket, objectName) + ", d:"
                        + (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null"));
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName) + ", d:"
                        + (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null"));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).readLock().unlock();
        }
    }

    @Override
    public void wipeAllPreviousVersions() {
        RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
        agent.wipeAllPreviousVersions();
    }

    @Override
    public void delete(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
        RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
        agent.delete(bucket, objectName);
    }

    @Override
    public ObjectMetadata getObjectMetadataVersion(ServerBucket bucket, String objectName, int version) {
        return getOM(bucket, objectName, Optional.of(Integer.valueOf(version)), true);
    }

    @Override
    public ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        RAIDSixUpdateObjectHandler agent = new RAIDSixUpdateObjectHandler(this);
        return agent.restorePreviousVersion(bucket, objectName);
    }

    @Override
    public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        Check.requireNonNullArgument(meta.bucketId, "bucketId is null");
        Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketId.toString());
        RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
        agent.deleteObjectAllPreviousVersions(meta);
    }

    @Override
    public void deleteBucketAllPreviousVersions(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucket.getName());
        RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
        agent.deleteBucketAllPreviousVersions(bucket);
    }

    @Override
    public RedundancyLevel getRedundancyLevel() {
        return RedundancyLevel.RAID_6;
    }

    public void rollbackJournal(VirtualFileSystemOperation operation) {
        rollbackJournal(operation, false);
    }

    @Override
    public boolean setUpDrives() {
        logger.debug("Starting async process to set up drives");
        return getApplicationContext().getBean(RAIDSixDriveSetup.class, this).setup();
    }

    /**
     * <p>
     * Weak Consistency.<br/>
     * If a file gives error while bulding the {@link DataList}, the Item will
     * contain an String with the error {code isOK()} should be used before
     * getObject()
     * </p>
     */
    @Override
    public DataList<Item<ObjectMetadata>> listObjects(ServerBucket bucket, Optional<Long> offset, Optional<Integer> pageSize,
            Optional<String> prefix, Optional<String> serverAgentId) {

        Check.requireNonNullArgument(bucket, "bucket is null");

        BucketIterator walker = null;
        BucketIteratorService walkerService = getVirtualFileSystemService().getBucketIteratorService();

        try {
            if (serverAgentId.isPresent())
                walker = walkerService.get(serverAgentId.get());

            if (walker == null) {
                walker = new RAIDSixBucketIterator(this, bucket, offset, prefix);
                walkerService.register(walker);
            }

            List<Item<ObjectMetadata>> list = new ArrayList<Item<ObjectMetadata>>();

            int size = pageSize.orElseGet(() -> ServerConstant.DEFAULT_PAGE_SIZE);

            int counter = 0;

            while (walker.hasNext() && counter++ < size) {
                Item<ObjectMetadata> item;
                try {
                    Path path = walker.next();
                    String objectName = path.toFile().getName();
                    item = new Item<ObjectMetadata>(getObjectMetadata(bucket, objectName));

                } catch (IllegalMonitorStateException e) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                    item = new Item<ObjectMetadata>(e);
                } catch (Exception e) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                    item = new Item<ObjectMetadata>(e);
                }
                list.add(item);
            }

            DataList<Item<ObjectMetadata>> result = new DataList<Item<ObjectMetadata>>(list);

            if (!walker.hasNext())
                result.setEOD(true);

            result.setOffset(walker.getOffset());
            result.setPageSize(size);
            result.setAgentId(walker.getAgentId());

            return result;

        } finally {
            if (walker != null && (!walker.hasNext()))
                getVirtualFileSystemService().getBucketIteratorService()
                        .remove(walker.getAgentId()); /** closes the stream upon removal */
        }
    }

    @Override
    protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
        return getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % getDrivesEnabled().size());
    }

    /**
     * <p>
     * RAID 6. Metadata read is from only 1 drive, selected randomly from all drives
     * </p>
     */
    private ObjectMetadata getOM(ServerBucket bucket, String objectName, Optional<Integer> o_version, boolean addToCacheifMiss) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
        Drive readDrive = null;

        objectReadLock(bucket, objectName);
        try {
        
            bucketReadLock(bucket);
            try {
            
                checkIsAccesible(bucket);
                
                /** read is from only 1 drive */
                readDrive = getObjectMetadataReadDrive(bucket, objectName);

                ObjectMetadata meta;
                
                if (o_version.isPresent()) {
                    meta = readDrive.getObjectMetadataVersion(bucket, objectName, o_version.get());
                } else {
                    meta = getDriverObjectMetadataInternal(bucket, objectName, addToCacheifMiss);
                }

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

                meta.setBucketName(bucket.getName());
                return meta;
                
            } catch (InternalCriticalException e) {
                throw e;
                
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName) + (o_version.isPresent() ? (", v:" + String.valueOf(o_version.get())) : ""));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    /**
     * @param meta
     * @param version
     * @return
     */
    protected Map<Drive, List<String>> getObjectDataFilesNames(ObjectMetadata meta, Optional<Integer> version) {

        Check.requireNonNullArgument(meta, "meta is null");

        Map<Drive, List<String>> map = new HashMap<Drive, List<String>>();

        for (Drive drive : getDrivesAll())
            map.put(drive, new ArrayList<String>());

        int totalBlocks = meta.getSha256Blocks().size();

        int totalDisks = getVirtualFileSystemService().getServerSettings().getRAID6DataDrives()
                + getVirtualFileSystemService().getServerSettings().getRAID6ParityDrives();
        Check.checkTrue(totalDisks > 0, "total disks must be greater than zero");

        int chunks = totalBlocks / totalDisks;
        Check.checkTrue(chunks > 0, "chunks must be greater than zero");

        for (int chunk = 0; chunk < chunks; chunk++) {
            for (int disk = 0; disk < getDrivesAll().size(); disk++) {
                String suffix = "." + String.valueOf(chunk) + "." + String.valueOf(disk)
                        + (version.isEmpty() ? "" : (".v" + String.valueOf(version.get())));
                Drive drive = getDrivesAll().get(disk);
                map.get(drive).add(meta.getObjectName() + suffix);
            }
        }
        return map;
    }

    protected boolean isConfigurationValid(int dataShards, int parityShards) {
        return getVirtualFileSystemService().getServerSettings().isRAID6ConfigurationValid(dataShards, parityShards);
    }

    /**
     * 
     * @param meta
     * @param version
     * @return
     */
    protected List<File> getObjectDataFiles(ObjectMetadata meta, ServerBucket bucket, Optional<Integer> version) {

        List<File> files = new ArrayList<File>();

        if (meta == null)
            return files;

        int totalBlocks = meta.getSha256Blocks().size();

        int totalDisks = getServerSettings().getRAID6DataDrives() + getServerSettings().getRAID6ParityDrives();
        Check.checkTrue(totalDisks > 0, "total disks must be greater than zero");

        int chunks = totalBlocks / totalDisks;
        Check.checkTrue(chunks > 0, "chunks must be greater than zero");

        for (int chunk = 0; chunk < chunks; chunk++) {
            for (int disk = 0; disk < getDrivesAll().size(); disk++) {
                String suffix = "." + String.valueOf(chunk) + "." + String.valueOf(disk)
                        + (version.isEmpty() ? "" : (".v" + String.valueOf(version.get())));
                Drive drive = getDrivesAll().get(disk);
                if (version.isEmpty())
                    files.add(new File(drive.getBucketObjectDataDirPath(bucket), meta.getObjectName() + suffix));
                else
                    files.add(new File(
                            drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR,
                            meta.getObjectName() + suffix));
            }
        }
        return files;
    }
}
