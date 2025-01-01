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
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 6. Delete Object handler
 * </p>
 * <p>
 * Auxiliary class used by {@link RaidSixHandler}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixDeleteObjectHandler extends RAIDSixTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDSixDeleteObjectHandler.class.getName());

    /**
     * <p>
     * Instances of this class are used internally by {@link RAIDSixDriver}
     * </p>
     * 
     * @param driver
     */
    protected RAIDSixDeleteObjectHandler(RAIDSixDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    /**
     * @param bucket
     * @param objectName
     */
    protected void delete() {

        VirtualFileSystemOperation operation = null;
        boolean done = false;
        boolean isMainException = false;
        ObjectMetadata meta = null;

        objectWriteLock();
        try {

            bucketReadLock();
            try {

                checkExistsBucket();
                checkExistObject();

                meta = getDriver().getObjectMetadataReadDrive(getBucket(), getObjectName()).getObjectMetadata(getBucket(),
                        getObjectName());

                operation = deleteObject(meta.getVersion());

                backupMetadata(meta);

                for (Drive drive : getDriver().getDrivesAll())
                    drive.deleteObjectMetadata(getBucket(), getObjectName());

                done = operation.commit();

            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e, info());
            } finally {

                try {

                    if ((!done) && (operation != null)) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, info());
                            else
                                logger.error(e, info(), SharedConstant.NOT_THROWN);
                        }
                    } else if (done) {
                        /** inside the thread */
                        postObjectDeleteCommit(meta, getBucket(), meta.getVersion());
                    }

                    /**
                     * DATA CONSISTENCY: If The system crashes before Commit or Cancel -> next time
                     * the system starts up it will REDO all stored operations. Also, if there are
                     * error buckets in the drives, they will be normalized when the system starts.
                     */

                } catch (Exception e) {
                    logger.error(e, opInfo(operation), SharedConstant.NOT_THROWN);
                } finally {
                    bucketReadUnLock();
                }
            }
        } finally {
            objectWriteUnLock();
        }

        if (done) {
            onAfterCommit(operation, meta, meta.getVersion());
        }
    }

    private VirtualFileSystemOperation deleteObject(int version) {
        return getJournalService().deleteObject(getBucket(), getObjectName(), version);
    }

    protected void deleteBucketAllPreviousVersions(ServerBucket bucket) {
        getVirtualFileSystemService().getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                .getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName(), bucket.getId()));
    }

    /**
     * Sync no need to locks
     * 
     * @param bucketName
     * @param objectName
     * @param headVersion newest version of the Object just deleted
     */
    private void postObjectDeleteCommit(@NonNull ObjectMetadata meta, ServerBucket bucket, int headVersion) {

        Check.requireNonNullArgument(meta, "meta is null");

        String bucketName = meta.getBucketName();
        String objectName = meta.getObjectName();

        Check.requireNonNullArgument(bucketName, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);

        try {
            /** delete data versions(0..head-1) */
            for (int n = 0; n < headVersion; n++)
                getDriver().getObjectDataFiles(meta, bucket, Optional.of(n)).forEach(item -> FileUtils.deleteQuietly(item));

            /** delete data (head) */
            getDriver().getObjectDataFiles(meta, bucket, Optional.empty()).forEach(item -> FileUtils.deleteQuietly(item));

            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll())
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket), objectName));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    /**
     * copy metadata directory
     * 
     * @param bucket
     * @param objectName
     */
    private void backupMetadata(ObjectMetadata meta) {
        try {
            for (Drive drive : getDriver().getDrivesAll()) {
                String objectMetadataDirPath = drive.getObjectMetadataDirPath(getBucket(), meta.getObjectName());
                String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(getBucket()) + File.separator
                        + meta.getObjectName();
                File src = new File(objectMetadataDirPath);
                if (src.exists())
                    FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
            }
        } catch (IOException e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(meta));
        }
    }

    /**
     * @param operation
     * @param headVersion
     */
    private void onAfterCommit(VirtualFileSystemOperation operation, ObjectMetadata meta, int headVersion) {
        try {
            if (operation.getOperationCode() == OperationCode.DELETE_OBJECT
                    || operation.getOperationCode() == OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS) {
                getVirtualFileSystemService().getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                        .getBean(AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, headVersion));
            }
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }
}
