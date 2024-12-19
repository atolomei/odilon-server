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
package io.odilon.virtualFileSystem.raid1;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketStatus;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.query.BucketIteratorService;

import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.BaseIODriver;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.OdilonBucket;
import io.odilon.virtualFileSystem.OdilonObject;
import io.odilon.virtualFileSystem.model.BucketIterator;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VFSOp;
import io.odilon.virtualFileSystem.model.VFSOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * <b>RAID 1</b>
 * </p>
 * <p>
 * {@link OdilonBucket} structure is the same for all drives <br/>
 * {@link VirtualFileSystemService} checks consistency during startup.
 * </p>
 * <p>
 * For each object, a copy is created on each {@link Drive}.
 * </p>
 * 
 * <p>
 * This Class is works as a
 * <a href="https://en.wikipedia.org/wiki/Facade_pattern">Facade pattern</a>
 * that uses {@link RAIDOneCreateObjectHandler},
 * {@link RAIDOneDeleteObjectHandler}, {@link RAIDOneUpdateObjectHandler} and
 * other
 * </p>
 */
@ThreadSafe
@Component
@Scope("prototype")
public class RAIDOneDriver extends BaseIODriver {

    private static Logger logger = Logger.getLogger(RAIDOneDriver.class.getName());

    /**
     * <p>
     * 
     * @param vfs
     * @param vfsLockService
     *                       </p>
     */
    public RAIDOneDriver(VirtualFileSystemService vfs, LockService vfsLockService) {
        super(vfs, vfsLockService);
    }

    @Override
    public boolean hasVersions(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return !getObjectMetadataVersionAll(bucket, objectName).isEmpty();
    }

    @Override
    public void wipeAllPreviousVersions() {
        RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
        agent.wipeAllPreviousVersions();
    }

    @Override
    public void deleteBucketAllPreviousVersions(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
        agent.deleteBucketAllPreviousVersions(bucket);
    }

    @Override
    public ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        RAIDOneUpdateObjectHandler agent = new RAIDOneUpdateObjectHandler(this);
        return agent.restorePreviousVersion(bucket, objectName);
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
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

        objectReadLock(bucket, objectName);

        try {
            bucketReadLock(bucket);

            try {
                /** RAID 1: read is from any of the drives */
                Drive readDrive = getReadDrive(bucket, objectName);

                if (!readDrive.existsBucketById(bucket.getId()))
                    throw new IllegalStateException("bucket -> b:" + bucket.getId() + " does not exist for -> d:"
                            + readDrive.getName() + " | v:" + String.valueOf(version));

                ObjectMetadata meta = getObjectMetadataVersion(bucket, objectName, version);

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException("object version does not exists for -> b:" + bucket.getId() + " | o:"
                            + objectName + " | v:" + String.valueOf(version));

                if (meta.isEncrypt())
                    return getVirtualFileSystemService().getEncryptionService()
                            .decryptStream(getInputStreamFromSelectedDrive(readDrive, bucket.getId(), objectName, version));
                else
                    return getInputStreamFromSelectedDrive(readDrive, bucket.getId(), objectName, version);
            } catch (OdilonObjectNotFoundException e) {
                logger.error(e, SharedConstant.NOT_THROWN);
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e,
                        "b:" + bucket.getId() + ", o:" + objectName + " | v:" + String.valueOf(version));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    @Override
    public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {

        Check.requireNonNullArgument(meta, "meta is null");
        Check.requireNonNullArgument(meta.bucketId, "bucketId is null");
        Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketId.toString());

        RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
        agent.deleteObjectAllPreviousVersions(meta);

    }

    @Override
    public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType,
            Optional<List<String>> customTags) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucket.getId());
        Check.requireNonNullStringArgument(fileName, "fileName is null | b: " + bucket.getId() + " o:" + objectName);
        Check.requireNonNullArgument(stream, "InpuStream can not null -> b:" + bucket.getId() + " | o:" + objectName);

        // TODO AT ->ideally lock must be before creating the agent
        if (exists(bucket, objectName)) {
            RAIDOneUpdateObjectHandler updateAgent = new RAIDOneUpdateObjectHandler(this);
            updateAgent.update(bucket, objectName, stream, fileName, contentType, customTags);
            getVirtualFileSystemService().getSystemMonitorService().getUpdateObjectCounter().inc();
        } else {
            RAIDOneCreateObjectHandler createAgent = new RAIDOneCreateObjectHandler(this);
            createAgent.create(bucket, objectName, stream, fileName, contentType, customTags);
            getVirtualFileSystemService().getSystemMonitorService().getCreateObjectCounter().inc();
        }
    }

    /**
     * <p>
     * RAID 1: Delete from all Drives
     * </p>
     */
    @Override
    public void delete(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getId());
        RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
        agent.delete(bucket, objectName);
    }

    /**
     * <p>
     * THis method is executed Async by the {@link SchedulerService}
     * </p>
     */
    @Override
    public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDOneDeleteObjectHandler deleteAgent = new RAIDOneDeleteObjectHandler(this);
        deleteAgent.postObjectDelete(meta, headVersion);
    }

    /**
     * 
     */
    @Override
    public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta, int headVersion) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDOneDeleteObjectHandler deleteAgent = new RAIDOneDeleteObjectHandler(this);
        deleteAgent.postObjectPreviousVersionDeleteAll(meta, headVersion);
    }

    /**
     * 
     */
    @Override
    public void putObjectMetadata(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        RAIDOneUpdateObjectHandler updateAgent = new RAIDOneUpdateObjectHandler(this);
        updateAgent.updateObjectMetadata(meta);
        getVirtualFileSystemService().getSystemMonitorService().getUpdateObjectCounter().inc();
    }

    /**
     * <p>
     * The process is Async for RAID 1
     * </p>
     */
    @Override
    public boolean setUpDrives() {
        logger.debug("Starting non blocking process to set up drives");
        return getApplicationContext().getBean(RAIDOneDriveSetup.class, this).setup();
    }

    /**
     * <p>
     * 
     * @param bucket bucket must exist in the system
     *               </p>
     * 
     *               public void deleteBucket(ServerBucket bucket) {
     *               getVirtualFileSystemService().removeBucket(bucket); }
     */

    /**
     * <p>
     * RAID 1 -> Read drive can be any from the pool
     * </p>
     */
    @Override
    public boolean isEmpty(ServerBucket bucket) {
        // Objects.requireNonNull(bucket, "bucket is null");
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(existsBucketInDrives(bucket.getId()), "bucket does not exist -> b: " + bucket.getId());

        bucketReadLock(bucket);

        try {
            return getReadDrive(bucket).isEmpty(bucket);
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket));

        } finally {
            bucketReadUnLock(bucket);
        }
    }

    @Override
    public List<ObjectMetadata> getObjectMetadataVersionAll(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | " + objectInfo(bucket));
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() + " or "
                + BucketStatus.ENABLED.getName() + ") | b:" + bucket.getName());

        List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

        Drive readDrive = null;

        objectReadLock(bucket, objectName);

        try {
            bucketReadLock(bucket);

            try {
                /** read is from only 1 drive */
                readDrive = getReadDrive(bucket, objectName);

                if (!readDrive.existsBucketById(bucket.getId()))
                    throw new IllegalStateException("bucket -> b:" + bucket.getId() + " does not exist for -> d:"
                            + readDrive.getName() + " | raid -> " + this.getClass().getSimpleName());

                ObjectMetadata meta = getObjectMetadataInternal(bucket, objectName, true);
                meta.bucketName = bucket.getName();

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(ObjectMetadata.class.getName() + " does not exist or not accesible");

                if (meta.version == 0)
                    return list;

                for (int version = 0; version < meta.version; version++) {
                    ObjectMetadata meta_version = readDrive.getObjectMetadataVersion(bucket, objectName, version);

                    /**
                     * bucketName is not stored on disk, only bucketId, we must set it explicitly
                     */
                    if (meta_version != null) {
                        meta_version.setBucketName(bucket.getName());
                        list.add(meta_version);
                    }
                }
                return list;
            } catch (OdilonObjectNotFoundException e) {
                e.setErrorMessage((e.getMessage() != null ? (e.getMessage() + " | ") : "") + "b:" + bucket.getName() + ", o:"
                        + objectName + ", d:" + (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null"));
                throw e;
            } catch (Exception e) {
                throw new InternalCriticalException(e, "b:" + bucket.getName() + ", o:" + objectName + ", d:"
                        + (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null"));
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
     * 
     */
    public VirtualFileSystemObject getObject(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getId());
        Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getId());

        objectReadLock(bucket, objectName);

        try {
            bucketReadLock(bucket);

            try {
                /** read is from only one of the drive (randomly selected) drive */
                Drive readDrive = getReadDrive(bucket, objectName);

                if (!readDrive.existsBucketById(bucket.getId()))
                    throw new IllegalArgumentException(
                            "bucket control folder -> b:" + bucket.getId() + " does not exist for drive -> d:" + readDrive.getName()
                                    + " | Raid -> " + this.getClass().getSimpleName());

                if (!exists(bucket, objectName))
                    throw new IllegalArgumentException("object does not exists for ->  b:" + bucket.getId() + " | o:" + objectName
                            + " | " + this.getClass().getSimpleName());

                ObjectMetadata meta = getObjectMetadataInternal(bucket, objectName, true);

                if (meta.status == ObjectStatus.ENABLED || meta.status == ObjectStatus.ARCHIVED) {
                    return new OdilonObject(bucket, objectName, getVirtualFileSystemService());
                }

                /**
                 * if the object is DELETED or DRAFT -> it will be purged from the system at
                 * some point.
                 */
                throw new OdilonObjectNotFoundException(String.format(
                        "object not found | status must be %s or %s -> b: %s | o:%s | o.status: %s", ObjectStatus.ENABLED.getName(),
                        ObjectStatus.ARCHIVED.getName(), Optional.ofNullable(bucket.getId().toString()).orElse("null"),
                        Optional.ofNullable(bucket.getId().toString()).orElse("null"), meta.status.getName()));
            } catch (Exception e) {
                throw new InternalCriticalException(e, "b:" + (Optional.ofNullable(bucket).isPresent() ? (bucket.getId()) : "null")
                        + ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName) : "null"));
            } finally {
                bucketReadUnLock(bucket);
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    /**
     * <p>
     * Invariant: all drives contain the same bucket structure
     * </p>
     */
    @Override
    public boolean exists(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getId());
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getId());

        objectReadLock(bucket, objectName);

        try {
            getLockService().getBucketLock(bucket).readLock().lock();

            try {
                return getReadDrive(bucket, objectName).existsObjectMetadata(bucket, objectName);
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                getLockService().getBucketLock(bucket).readLock().unlock();

            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    /**
     * <p>
     * Weak Consistency.<br/>
     * If a file gives error while building the {@link DataList}, the Item will
     * contain an String with the error Method {code isOK()} should be checked
     * before accessing the ObjectMetadata with {@code getObject()}
     * </p>
     */
    @Override
    public DataList<Item<ObjectMetadata>> listObjects(ServerBucket bucket, Optional<Long> offset, Optional<Integer> pageSize,
            Optional<String> prefix, Optional<String> serverAgentId) {

        Check.requireNonNullArgument(bucket, "bucket is null");

        BucketIterator bucketIterator = null;
        BucketIteratorService walkerService = getVirtualFileSystemService().getBucketIteratorService();

        try {
            if (serverAgentId.isPresent())
                bucketIterator = walkerService.get(serverAgentId.get());

            if (bucketIterator == null) {
                bucketIterator = new RAIDOneBucketIterator(this, bucket, offset, prefix);
                walkerService.register(bucketIterator);
            }

            List<Item<ObjectMetadata>> list = new ArrayList<Item<ObjectMetadata>>();

            int size = pageSize.orElseGet(() -> ServerConstant.DEFAULT_PAGE_SIZE);

            int counter = 0;

            while (bucketIterator.hasNext() && counter++ < size) {
                Item<ObjectMetadata> item;
                try {
                    Path path = bucketIterator.next();
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

            if (!bucketIterator.hasNext())
                result.setEOD(true);

            result.setOffset(bucketIterator.getOffset());
            result.setPageSize(size);
            result.setAgentId(bucketIterator.getAgentId());

            return result;

        } finally {

            if (bucketIterator != null && (!bucketIterator.hasNext()))
                /** closes the stream upon removal */
                getVirtualFileSystemService().getBucketIteratorService().remove(bucketIterator.getAgentId());
        }
    }

    /**
     * <b>IMPORTANT</b> -> caller must close the {@link InputStream} returned
     * 
     * @param bucketName
     * @param objectName
     * @return
     * @throws IOException
     */
    public InputStream getInputStream(ServerBucket bucket, String objectName) throws IOException {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getId());
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getId());

        objectReadLock(bucket, objectName);
        try {
            getLockService().getBucketLock(bucket).readLock().lock();

            try {
                /** read is from only 1 drive, randomly selected */
                Drive readDrive = getReadDrive(bucket, objectName);

                if (!readDrive.existsBucketById(bucket.getId()))
                    throw new IllegalStateException("bucket -> b:" + bucket.getId() + " does not exist for drive -> d:"
                            + readDrive.getName() + " | class -> " + this.getClass().getSimpleName());

                ObjectMetadata meta = getObjectMetadataInternal(bucket, objectName, true);

                InputStream stream = getInputStreamFromSelectedDrive(readDrive, bucket.getId(), objectName);

                if (meta.isEncrypt())
                    return getVirtualFileSystemService().getEncryptionService().decryptStream(stream);
                else
                    return stream;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                getLockService().getBucketLock(bucket).readLock().unlock();
            }
        } finally {
            objectReadUnLock(bucket, objectName);
        }
    }

    /**
     * <p>
     * returns true if the object integrity is ok or if it can be fixed for all
     * copies
     * </p>
     * <p>
     * if it can not be fixed for at least one copy it returns false
     * </p>
     */
    @Override
    public boolean checkIntegrity(ServerBucket bucket, String objectName, boolean forceCheck) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null");

        OffsetDateTime thresholdDate = OffsetDateTime.now()
                .minusDays(getVirtualFileSystemService().getServerSettings().getIntegrityCheckDays());

        boolean retValue = true;

        Boolean iCheck[] = new Boolean[getDrivesEnabled().size()];
        {
            int total = getDrivesEnabled().size();
            for (int p = 0; p < total; p++)
                iCheck[p] = Boolean.valueOf(true);
        }

        Drive goodDrive = null;
        ObjectMetadata goodDriveMeta = null;

        boolean objectLock = false;
        boolean bucketLock = false;

        try {

            try {
                objectLock = getLockService().getObjectLock(bucket, objectName).readLock().tryLock(20, TimeUnit.SECONDS);
                if (!objectLock) {
                    logger.warn("Can not acquire read Lock for Object. Assumes check is ok -> " + objectName);
                    return true;
                }
            } catch (InterruptedException e) {
                logger.warn(e, SharedConstant.NOT_THROWN);
                return true;
            }

            try {
                bucketLock = getLockService().getBucketLock(bucket).readLock().tryLock(20, TimeUnit.SECONDS);
                if (!bucketLock) {
                    logger.warn("Can not acquire read Lock for Bucket. Assumes check is ok -> " + bucket.getName(),
                            SharedConstant.NOT_THROWN);
                    return true;
                }
            } catch (InterruptedException e) {
                return true;
            }

            int n = 0;

            for (Drive drive : getDrivesEnabled()) {

                ObjectMetadata meta = drive.getObjectMetadata(bucket, objectName);

                if ((forceCheck) || (meta.integrityCheck == null) || (meta.integrityCheck.isBefore(thresholdDate))) {

                    String originalSha256 = meta.sha256;
                    String sha256 = null;

                    
                    //File file = ((SimpleDrive) drive).getObjectDataFile(bucket.getId(), objectName);
                    ObjectPath path = new ObjectPath(drive, bucket, objectName);
                    File file = path.dataFilePath().toFile();
                    
                    try {

                        sha256 = OdilonFileUtils.calculateSHA256String(file);

                        if (originalSha256 == null) {
                            meta.sha256 = sha256;
                            originalSha256 = sha256;
                        }

                        if (originalSha256.equals(sha256)) {

                            if (goodDrive == null)
                                goodDrive = drive;

                            meta.integrityCheck = OffsetDateTime.now();
                            drive.saveObjectMetadata(meta);
                            iCheck[n] = Boolean.valueOf(true);

                        } else {
                            logger.error("Integrity Check failed for -> d: " + drive.getName() + " | b:" + bucket.getId() + " | o:"
                                    + objectName);
                            iCheck[n] = Boolean.valueOf(false);
                        }

                    } catch (NoSuchAlgorithmException | IOException e) {
                        logger.error(e, SharedConstant.NOT_THROWN);
                        iCheck[n] = Boolean.valueOf(false);
                    }
                } else
                    iCheck[n] = Boolean.valueOf(true);
                n++;
            }

            retValue = true;

            int total = iCheck.length;
            for (int p = 0; p < total; p++) {
                if (!iCheck[p].booleanValue()) {
                    retValue = false;
                    if (goodDrive != null)
                        goodDriveMeta = goodDrive.getObjectMetadata(bucket, objectName);
                    break;
                }
            }
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

        if (bucketLock && objectLock && (!retValue)) {
            if (goodDrive != null) {
                if (goodDriveMeta == null)
                    goodDriveMeta = goodDrive.getObjectMetadata(bucket, objectName);
                retValue = fix(bucket, objectName, goodDriveMeta, iCheck, goodDrive);
            }
        }
        return retValue;
    }

    @Override
    public RedundancyLevel getRedundancyLevel() {
        return RedundancyLevel.RAID_1;
    }

    /**
     * @param op
     * @param bucket
     * @param objectName
     */
    @Override
    public void rollbackJournal(VFSOperation op, boolean recoveryMode) {

        Check.requireNonNullArgument(op, "VFSOperation is null");

        if (op.getOp() == VFSOp.CREATE_OBJECT) {
            RAIDOneCreateObjectHandler handler = new RAIDOneCreateObjectHandler(this);
            handler.rollbackJournal(op, recoveryMode);
            return;
        } else if (op.getOp() == VFSOp.UPDATE_OBJECT) {
            RAIDOneUpdateObjectHandler handler = new RAIDOneUpdateObjectHandler(this);
            handler.rollbackJournal(op, recoveryMode);
            return;
        } else if (op.getOp() == VFSOp.DELETE_OBJECT) {
            RAIDOneDeleteObjectHandler handler = new RAIDOneDeleteObjectHandler(this);
            handler.rollbackJournal(op, recoveryMode);
            return;
        }

        else if (op.getOp() == VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS) {
            RAIDOneDeleteObjectHandler handler = new RAIDOneDeleteObjectHandler(this);
            handler.rollbackJournal(op, recoveryMode);
            return;
        } else if (op.getOp() == VFSOp.UPDATE_OBJECT_METADATA) {
            RAIDOneUpdateObjectHandler handler = new RAIDOneUpdateObjectHandler(this);
            handler.rollbackJournal(op, recoveryMode);
            return;
        }

        boolean done = false;

        try {

            if (getServerSettings().isStandByEnabled())
                getVirtualFileSystemService().getReplicationService().cancel(op);

            if (op.getOp() == VFSOp.CREATE_BUCKET) {

                done = generalRollbackJournal(op);

            } else if (op.getOp() == VFSOp.DELETE_BUCKET) {

                done = generalRollbackJournal(op);

            } else if (op.getOp() == VFSOp.UPDATE_BUCKET) {

                done = generalRollbackJournal(op);
            }
            if (op.getOp() == VFSOp.CREATE_SERVER_MASTERKEY) {

                done = generalRollbackJournal(op);

            } else if (op.getOp() == VFSOp.CREATE_SERVER_METADATA) {

                done = generalRollbackJournal(op);

            } else if (op.getOp() == VFSOp.UPDATE_SERVER_METADATA) {

                done = generalRollbackJournal(op);
            }

        } catch (InternalCriticalException e) {
            String msg = "Rollback: " + (Optional.ofNullable(op).isPresent() ? op.toString() : "null");
            logger.error(msg);
            if (!recoveryMode)
                throw (e);

        } catch (Exception e) {
            String msg = "Rollback:" + (Optional.ofNullable(op).isPresent() ? op.toString() : "null");
            logger.error(msg);
            if (!recoveryMode)
                throw new InternalCriticalException(e, msg);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            } else {
                if (getVirtualFileSystemService().getServerSettings().isRecoverMode()) {
                    logger.error("---------------------------------------------------------------");
                    logger.error("Cancelling failed operation -> " + op.toString());
                    logger.error("---------------------------------------------------------------");
                    op.cancel();
                }
            }
        }
    }

    /**
     * DATA CONSISTENCY --------------- The system crashes before Commit or Cancel
     * -> next time the system starts up it will CANCEL all operations that are
     * incomplete. REDO in this version means deleting the object from the drives
     * where it completed
     */

    @Override
    protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
        return getReadDrive(bucket, objectName);
    }

    protected Drive getReadDrive(ServerBucket bucket, String objectName) {
        return getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % getDrivesEnabled().size());
    }

    protected Drive getReadDrive(ServerBucket bucket) {
        return getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % getDrivesEnabled().size());
    }

    protected InputStream getInputStreamFromSelectedDrive(Drive readDrive, Long bucketId, String objectName) throws IOException {
        return Files.newInputStream(
                Paths.get(readDrive.getRootDirPath() + File.separator + bucketId.toString() + File.separator + objectName));
    }

    protected InputStream getInputStreamFromSelectedDrive(Drive readDrive, Long bucketId, String objectName, int version)
            throws IOException {
        return Files.newInputStream(Paths.get(readDrive.getRootDirPath() + File.separator + bucketId.toString() + File.separator
                + VirtualFileSystemService.VERSION_DIR + File.separator + objectName + VirtualFileSystemService.VERSION_EXTENSION
                + String.valueOf(version)));
    }

    /**
     * 
     */
    private boolean fix(ServerBucket bucket, String objectName, ObjectMetadata goodDriveMeta, Boolean[] iCheck, Drive goodDrive) {

        Check.requireNonNullArgument(goodDrive, "goodDrive is null");
        Check.requireNonNullArgument(goodDriveMeta, "goodDriveMeta is null");

        boolean retValue = true;

        getLockService().getObjectLock(bucket, objectName).writeLock().lock();

        try {
            getLockService().getBucketLock(bucket).readLock().lock();

            try {

                ObjectMetadata currentMeta = goodDrive.getObjectMetadata(bucket, objectName);

                if (!currentMeta.lastModified.equals(goodDriveMeta.lastModified))
                    return true;

                iCheck[0] = true;
                iCheck[1] = false;

                int total = iCheck.length;

                for (int p = 0; p < total; p++) {

                    if (!iCheck[p].booleanValue()) {
                        SimpleDrive destDrive = (SimpleDrive) getDrivesEnabled().get(p);
                        InputStream in = null;
                        try {
                            if (!goodDrive.equals(destDrive)) {

                                // in = ((SimpleDrive) goodDrive).getObjectInputStream(bucket.getId(), objectName);
                                
                                ObjectPath path = new ObjectPath(goodDrive, bucket, objectName);
                                in = Files.newInputStream(path.dataFilePath());

                                destDrive.putObjectStream(bucket.getId(), objectName, in);

                                goodDriveMeta.setDrive(destDrive.getName());
                                destDrive.saveObjectMetadata(goodDriveMeta);
                                logger.debug("Fixed -> d: " + destDrive.getName() + " " + objectInfo(bucket, objectName));
                            }

                        } catch (IOException e) {
                            logger.error(e, SharedConstant.NOT_THROWN);
                            retValue = false;
                        } finally {

                            if (in != null)
                                in.close();
                        }
                    }
                }

                getVirtualFileSystemService().getObjectMetadataCacheService().remove(bucket.getId(), objectName);

            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
                retValue = false;
            } finally {

                getLockService().getBucketLock(bucket).readLock().unlock();
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
        }
        return retValue;
    }

    /**
     * <p>
     * RAID 1. read is from only 1 drive, selected randomly from all drives
     * </p>
     */
    private ObjectMetadata getOM(ServerBucket bucket, String objectName, Optional<Integer> o_version, boolean addToCacheifMiss) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
        Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() + " or "
                + BucketStatus.ENABLED.getName() + ") | b:" + bucket.getName());

        Drive readDrive = null;

        objectReadLock(bucket, objectName);

        try {
            
            bucketReadLock(bucket);

            try {
                /** read is from only 1 drive */
                readDrive = getReadDrive(bucket, objectName);

                if (!readDrive.existsBucketById(bucket.getId()))
                    throw new IllegalArgumentException("bucket -> b:" + bucket.getName() + " does not exist for -> d:" + readDrive.getName() );

                if (!exists(bucket, objectName))
                    throw new IllegalArgumentException("Object does not exists -> " + objectInfo(bucket, objectName));

                ObjectMetadata meta;

                if (o_version.isPresent())
                    meta = readDrive.getObjectMetadataVersion(bucket, objectName, o_version.get());
                else
                    meta = getObjectMetadataInternal(bucket, objectName, addToCacheifMiss);

                meta.setBucketName(bucket.getName());

                return meta;

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
    public void syncObject(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        logger.error("not done", SharedConstant.NOT_THROWN);
    }
}
