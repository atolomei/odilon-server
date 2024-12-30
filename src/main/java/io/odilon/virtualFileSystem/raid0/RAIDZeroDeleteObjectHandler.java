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
import io.odilon.scheduler.SchedulerService;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.ObjectPath;

import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 0. Delete Handler
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDZeroDeleteObjectHandler extends RAIDZeroTransactionHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroDeleteObjectHandler.class.getName());

    protected RAIDZeroDeleteObjectHandler(RAIDZeroDriver driver) {
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
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
        VirtualFileSystemOperation operation = null;
        boolean commitOK = false;
        boolean isMainException = false;
        int headVersion = -1;
        ObjectMetadata meta = null;

        objectWriteLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try {

                /** must be executed inside the critical zone. */
                checkExistsBucket(bucket);

                /** must be executed inside the critical zone. */
                checkExistObject(bucket, objectName);

                meta = getMetadata(bucket, objectName, false);

                if (!meta.isAccesible())
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

                headVersion = meta.getVersion();
                operation = deleteObject(bucket, objectName, headVersion);

                backupMetadata(bucket, meta.getObjectName());
                getDriver().getDrive(bucket, objectName).deleteObjectMetadata(bucket, objectName);

                commitOK = operation.commit();

            } catch (OdilonObjectNotFoundException e1) {
                commitOK = false;
                isMainException = true;
                throw e1;
            } catch (Exception e) {
                commitOK = false;
                isMainException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                try {
                    if (commitOK) {
                        removeVersions(meta, bucket, headVersion);
                    } else {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw e;
                            else
                                logger.error(e, objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
                        }
                    }
                } finally {
                    bucketReadUnLock(bucket);
                }
            }
        } finally {
            objectWriteUnLock(bucket, objectName);
        }
        if (commitOK)
            onAfterCommit(operation, meta, headVersion);
    }

    /**
     * <p>
     * This method does <b>not</b> delete the head version, only previous versions
     * </p>
     * 
     * @param bucket
     * @param objectName
     */

    protected void deleteObjectAllPreviousVersions(ObjectMetadata meta) {

        Check.requireNonNullArgument(meta, "meta is null");
        ServerBucket bucket = null;
        boolean isMainExcetion = false;
        int headVersion = -1;
        boolean done = false;
        VirtualFileSystemOperation operation = null;

        objectWriteLock(meta);
        try {

            bucketReadLock(meta.getBucketName());
            try {

                /** must be executed inside the critical zone. */
                checkExistsBucket(meta.getBucketId());
                bucket = getCacheBucket(meta.getBucketId());

                /** must be executed inside the critical zone. */
                checkExistObject(bucket, meta.getObjectName());

                headVersion = meta.getVersion();

                /** not delete the head version, only previous versions */
                if (headVersion == 0)
                    return;

                operation = getJournalService().deleteObjectPreviousVersions(bucket, meta.getObjectName(), headVersion);
                backupMetadata(bucket, meta.getObjectName());

                /**
                 * remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json"
                 **/
                for (int version = 0; version < headVersion; version++)
                    FileUtils.deleteQuietly(getDriver().getReadDrive(bucket, meta.getObjectName())
                            .getObjectMetadataVersionFile(bucket, meta.getObjectName(), version));
                meta.addSystemTag("delete versions");
                meta.setLastModified(OffsetDateTime.now());
                getDriver().getWriteDrive(bucket, meta.getObjectName()).saveObjectMetadata(meta);
                done = operation.commit();

            } catch (InternalCriticalException e) {
                done = false;
                isMainExcetion = true;
                throw e;

            } catch (Exception e) {
                done = false;
                isMainExcetion = true;
                throw new InternalCriticalException(e, getDriver().objectInfo(meta));
            } finally {
                try {
                    if (!done) {
                        try {
                            rollback(operation);
                        } catch (InternalCriticalException e) {
                            if (!isMainExcetion)
                                throw e;
                            else
                                logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
                        } catch (Exception e) {
                            if (!isMainExcetion)
                                throw new InternalCriticalException(e, objectInfo(bucket, meta.getObjectName()));
                            else
                                logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        try {
                            postObjectPreviousVersionDeleteAllCommit(meta, bucket, headVersion);
                        } catch (Exception e) {
                            logger.error(e, objectInfo(bucket, meta.getObjectName()), SharedConstant.NOT_THROWN);
                        }
                    }
                } finally {
                    bucketReadUnLock(bucket);
                }
            }
        } finally {
            objectWriteUnLock(meta);
        }
        if (done)
            onAfterCommit(operation, meta, headVersion);
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
     * to scan all Objects and remove previous versions.In case of failure (for
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
     * <p>
     * The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads
     * to scan all Objects and remove previous versions. In case of failure (for
     * example. the server is shutdown before completion), it is retried up to 5
     * times.
     * </p>
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
     * <p>
     * This method is ThreadSafe <br/>
     * It does not require lock because once previous versions have been deleted
     * they can not be created again by another Thread
     * </p>
     * 
     */

    /** do nothing by the moment */
    protected void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion) {
    }

    /** do nothing by the moment */
    protected void postObjectDelete(ObjectMetadata meta, int headVersion) {
    }

    private void postObjectPreviousVersionDeleteAllCommit(ObjectMetadata meta, ServerBucket bucket, int headVersion) {
        try {
            ObjectPath path = new ObjectPath(getWriteDrive(bucket, meta.getObjectName()), meta.getBucketId(), meta.getObjectName());

            /** delete data versions(1..n-1). keep headVersion **/
            for (int n = 0; n < headVersion; n++)
                FileUtils.deleteQuietly(path.dataFileVersionPath(n).toFile());

            /** delete backup Metadata */
            FileUtils.deleteQuietly(path.metadataWorkFilePath().toFile());

        } catch (Exception e) {
            logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
        }
    }

    /**
     * <p>
     * This method is executed by the delete thread. It does not need to control
     * concurrent access because the caller method does it. It should also be fast
     * since it is part of the main transaction
     * </p>
     * 
     * @param meta
     * @param headVersion
     */

    private void removeVersions(ObjectMetadata meta, ServerBucket bucket, int headVersion) {
        try {

            ObjectPath path = new ObjectPath(getWriteDrive(bucket, meta.getObjectName()), bucket.getId(), meta.getObjectName());

            // delete data versions(1..n-1)
            for (int version = 0; version <= headVersion; version++)
                FileUtils.deleteQuietly(path.dataFileVersionPath(version).toFile());

            // delete metadata (head)
            // not required because it was done before commit
            // delete data (head)
            FileUtils.deleteQuietly(path.dataFilePath().toFile());

            // delete backup Metadata
            FileUtils.deleteQuietly(path.metadataWorkFilePath().toFile());
        } catch (Exception e) {
            logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
        }
    }

    /**
     * copy metadata directory
     * 
     * @param bucket
     * @param objectName
     */
    private void backupMetadata(ServerBucket bucket, String objectName) {
        try {
            // ObjectPath path = new ObjectPath( getDriver().getWriteDrive(bucket,
            // objectName), bucket,objectName);
            String objectMetadataDirPath = getDriver().getWriteDrive(bucket, objectName).getObjectMetadataDirPath(bucket,
                    objectName);
            String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucket)
                    + File.separator + objectName;
            FileUtils.copyDirectory(new File(objectMetadataDirPath), new File(objectMetadataBackupDirPath));
        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

   
    /**
     * <p>
     * This method is called after the TRX commit. It is used to clean temp files,
     * if the system crashes those temp files will be removed on system startup
     * </p>
     */
    private void onAfterCommit(VirtualFileSystemOperation operation, ObjectMetadata meta, int headVersion) {

        if (operation == null)
            return;

        if (meta == null)
            return;

        try {
            if (operation.getOperationCode() == OperationCode.DELETE_OBJECT
                    || operation.getOperationCode() == OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS)

                getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                        .getBean(AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, headVersion));

        } catch (Exception e) {
            logger.error(e, opInfo(operation), SharedConstant.NOT_THROWN);
        }
    }

}
