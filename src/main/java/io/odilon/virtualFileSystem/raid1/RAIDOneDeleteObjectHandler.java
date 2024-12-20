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
import java.time.OffsetDateTime;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.util.Check;

import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VFSOp;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 1 Handler <br/>
 * Delete methods ({@link VFSOp.DELETE_OBJECT})
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneDeleteObjectHandler extends RAIDOneHandler {

    private static Logger logger = Logger.getLogger(RAIDOneDeleteObjectHandler.class.getName());

    /**
     * Instances of this class are used internally by {@link RAIDOneDriver}
     * 
     * @param driver
     */
    protected RAIDOneDeleteObjectHandler(RAIDOneDriver driver) {
        super(driver);
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * @param contentType
     */
    protected void delete(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(bucket.getId(), "bucketId is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

        if (!getDriver().exists(bucket, objectName))
            throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName() + " o:" + objectName);

        VirtualFileSystemOperation op = null;
        boolean done = false;
        int headVersion = -1;
        ObjectMetadata meta = null;

        objectWriteLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try {

                meta = getDriver().getReadDrive(bucket, objectName).getObjectMetadata(bucket, objectName);
                headVersion = meta.getVersion();
                op = getJournalService().deleteObject(bucket, objectName, headVersion);
                backupMetadata(meta, bucket);

                // TODO AT: parallel

                for (Drive drive : getDriver().getDrivesAll())
                    ((SimpleDrive) drive).deleteObjectMetadata(bucket, objectName);

                done = op.commit();

            } catch (OdilonObjectNotFoundException e1) {
                done = false;
                throw e1;

            } catch (Exception e) {
                done = false;
                throw new InternalCriticalException(e, opInfo(op) + "  " + objectInfo(bucket, objectName));
            } finally {

                try {

                    if ((!done) && (op != null)) {
                        try {
                            rollbackJournal(op, false);
                        } catch (Exception e) {
                            throw new InternalCriticalException(e, opInfo(op) + "  " + objectInfo(bucket, objectName));
                        }
                    } else if (done)
                        postObjectDeleteCommit(meta, bucket, headVersion);

                    /**
                     * DATA CONSISTENCY: If The system crashes before Commit or Cancel -> next time
                     * the system starts up it will REDO all stored operations. Also, the if there
                     * are error buckets in the drives, they will be normalized when the system
                     * starts.
                     */

                } catch (Exception e) {
                    logger.error(e, opInfo(op) + "  " + objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
                } finally {
                    bucketReadUnLock(bucket);
                }
            }
        } finally {
            objectWriteUnLock(bucket, objectName);
        }

        if (done)
            onAfterCommit(op, meta, headVersion);
    }

    /**
     * <p>
     * Adds a {@link DeleteBucketObjectPreviousVersionServiceRequest} to the
     * {@link SchedulerService} to walk through all objects and delete versions.
     * This process is Async and handler returns immediately.
     * </p>
     * 
     * <p>
     * The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads
     * to scan all Objects and remove previous versions. In case of failure (for
     * example. the server is shutdown before completion), it is retried up to 5
     * times.
     * </p>
     * 
     * <p>
     * Although the removal of all versions for every Object is transactional, the
     * {@link ServiceRequest} itself is not transactional, and it can not be
     * rollback
     * </p>
     */
    protected void wipeAllPreviousVersions() {
        getVirtualFileSystemService().getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                .getBean(DeleteBucketObjectPreviousVersionServiceRequest.class));
    }

    /**
     * <p>
     * Adds a {@link DeleteBucketObjectPreviousVersionServiceRequest} to the
     * {@link SchedulerService} to walk through all objects and delete versions.
     * This process is Async and handler returns immediately.
     * </p>
     * 
     * <p>
     * The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads
     * to scan all Objects and remove previous versions. In case of failure (for
     * example. the server is shutdown before completion), it is retried up to 5
     * times.
     * </p>
     * 
     * <p>
     * Although the removal of all versions for every Object is transactional, the
     * ServiceRequest itself is not transactional, and it can not be rollback
     * </p>
     */
    protected void deleteBucketAllPreviousVersions(ServerBucket bucket) {
        getVirtualFileSystemService().getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                .getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName(), bucket.getId()));
    }

    /**
     * 
     * @param bucket
     * @param objectName
     */
    protected void deleteObjectAllPreviousVersions(ObjectMetadata meta) {

        Check.requireNonNullArgument(meta, "meta is null");

        VirtualFileSystemOperation op = null;
        boolean done = false;
        boolean isMainException = false;

        int headVersion = -1;
        String objectName = meta.getObjectName();

        ServerBucket bucket = null;

        // getLockService().getObjectLock(meta.getBucketId(),
        // objectName).writeLock().lock();
        objectWriteLock(meta.getBucketId(), objectName);
        try {
            bucketReadLock(meta.getBucketId());

            try {
                if (!existsCacheBucket(meta.getBucketId()))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

                bucket = getCacheBucket(meta.getBucketId());

                if (!getDriver().getReadDrive(bucket, objectName).existsObjectMetadata(meta))
                    throw new IllegalArgumentException("object does not exist -> " + objectInfo(bucket, objectName));

                headVersion = meta.getVersion();

                /** It does not delete the head version, only previous versions */
                if (headVersion == 0)
                    return;

                op = getJournalService().deleteObjectPreviousVersions(bucket, objectName, headVersion);

                backupMetadata(meta, bucket);

                /**
                 * remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json"
                 **/
                for (int version = 0; version < headVersion; version++) {
                    for (Drive drive : getDriver().getDrivesAll()) {
                        FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket, objectName, version));
                    }
                }

                /** update head metadata with the tag */
                meta.addSystemTag("delete versions");
                meta.lastModified = OffsetDateTime.now();
                for (Drive drive : getDriver().getDrivesAll()) {
                    ObjectMetadata metaDrive = drive.getObjectMetadata(bucket, objectName);
                    meta.drive = drive.getName();
                    drive.saveObjectMetadata(metaDrive);
                }

                done = op.commit();

            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e);
            } finally {
                try {
                    if ((!done) && (op != null)) {
                        try {
                            rollbackJournal(op, false);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(meta));
                            else
                                logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
                        }
                    } else if (done) {
                        postObjectPreviousVersionDeleteAllCommit(meta, bucket, headVersion);
                    }
                } finally {
                    bucketReadUnLock(meta.getBucketId());
                }
            }
        } finally {
            // getLockService().getObjectLock(meta.getBucketId(),
            // meta.objectName).writeLock().unlock();
            objectWriteUnLock(bucket, objectName);
        }

        if (done)
            onAfterCommit(op, meta, headVersion);
    }

    protected void rollbackJournal(VirtualFileSystemOperation op, boolean recoveryMode) {

        /** checked by the calling driver */
        Check.requireNonNullArgument(op, "op is null");
        Check.requireTrue(op.getOp() == VFSOp.DELETE_OBJECT || op.getOp() == VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS,
                "VFSOperation invalid -> op: " + op.getOp().getName());

        String objectName = op.getObjectName();
        Long bucketId = op.getBucketId();

        Check.requireNonNullArgument(bucketId, "bucketId is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketId.toString());

        boolean done = false;

        try {

            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(op);

            ServerBucket bucket = getCacheBucket(op.getBucketId());

            /** rollback is the same for both operations */
            if (op.getOp() == VFSOp.DELETE_OBJECT)
                restoreMetadata(bucket, objectName);

            else if (op.getOp() == VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
                restoreMetadata(bucket, objectName);

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(opInfo(op), SharedConstant.NOT_THROWN);
        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(op));
            else
                logger.error(opInfo(op), SharedConstant.NOT_THROWN);

        } finally {
            if (done || recoveryMode)
                op.cancel();
        }
    }

    protected void postObjectDelete(ObjectMetadata meta, int headVersion) {
    }

    protected void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion) {
    }

    /**
     * 
     * 
     */
    private void postObjectPreviousVersionDeleteAllCommit(ObjectMetadata meta, ServerBucket bucket, int headVersion) {

        String bucketName = meta.getBucketName();
        String objectName = meta.getObjectName();

        Check.requireNonNullArgument(bucketName, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
        try {
            /** delete data versions(1..n-1). keep headVersion **/
            for (int version = 0; version < headVersion; version++) {
                for (Drive drive : getDriver().getDrivesAll()) {
                    ObjectPath path = new ObjectPath(drive, bucket, objectName);
                    FileUtils.deleteQuietly(path.dataFileVersionPath(version).toFile());
                    // FileUtils.deleteQuietly(((SimpleDrive)
                    // drive).getObjectDataVersionFile(meta.bucketId, objectName, version));
                }
            }

            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll()) {
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket) + File.separator + objectName));
            }

        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }

    }

    /**
     * 
     * <p>
     * This method is called <b>Async</b> by the Scheduler after the transaction is
     * committed It does not require locking <br/>
     * bucketName and objectName may not be the same called in other methods
     * </p>
     * 
     * <p>
     * data (head) data (versions) metadata dir (all versions) backup
     * 
     * metadata dir -> this was already deleted before the commit
     * </p>
     * 
     * @param bucketName
     * @param objectName
     * @param headVersion newest version of the Object just deleted
     */

    private void postObjectDeleteCommit(ObjectMetadata meta, ServerBucket bucket, int headVersion) {

        String objectName = meta.getObjectName();

        try {
            /** delete data versions(1..n-1) */
            for (int version = 0; version <= headVersion; version++) {
                for (Drive drive : getDriver().getDrivesAll()) {

                    ObjectPath path = new ObjectPath(drive, bucket, objectName);
                    FileUtils.deleteQuietly(path.dataFileVersionPath(version).toFile());
                    // FileUtils.deleteQuietly(((SimpleDrive)
                    // drive).getObjectDataVersionFile(meta.getBucketId(), objectName, version));
                }
            }
            /** delete data (head) */
            for (Drive drive : getDriver().getDrivesAll()) {
                ObjectPath path = new ObjectPath(drive, bucket, objectName);
                File file = path.dataFilePath().toFile();
                FileUtils.deleteQuietly(file);
                // FileUtils.deleteQuietly(((SimpleDrive)
                // drive).getObjectDataFile(meta.getBucketId(), objectName));
            }
            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll())
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket) + File.separator + objectName));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    /**
     * @param bucket
     * @param objectName
     */
    private void backupMetadata(ObjectMetadata meta, ServerBucket bucket) {
        /** copy metadata directory */
        try {
            for (Drive drive : getDriver().getDrivesAll()) {
                String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket, meta.objectName);
                String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator + meta.objectName;
                File src = new File(objectMetadataDirPath);
                if (src.exists())
                    FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
            }

        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(meta));
        }
    }

    private void restoreMetadata(ServerBucket bucket, String objectName) {
        /** restore metadata directory */
        for (Drive drive : getDriver().getDrivesAll()) {
            String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator + objectName;
            String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket, objectName);
            try {
                FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
            } catch (IOException e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            }
        }
    }

    /**
     * <p>
     * This method is called after the TRX commit. It is used to clean temp files,
     * if the system crashes those temp files will be removed on system startup
     * </p>
     */
    private void onAfterCommit(VirtualFileSystemOperation op, ObjectMetadata meta, int headVersion) {
        try {
            if (op.getOp() == VFSOp.DELETE_OBJECT || op.getOp() == VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
                getVirtualFileSystemService().getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                        .getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), meta, headVersion));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

}
