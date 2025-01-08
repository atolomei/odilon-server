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

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
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
        boolean commitOK = false;
        boolean isMainException = false;
        ObjectMetadata meta = null;

        objectWriteLock();
        try {

            bucketReadLock();
            try {

                checkExistsBucket();
                checkExistObject();

                meta = getMetadata();

                /** backup */
                backup(meta);
                
                /** start operation */
                operation = deleteObject(meta.getVersion());

                for (Drive drive : getDriver().getDrivesAll())
                    drive.deleteObjectMetadata(getBucket(), getObjectName());
                
                /** commit */
                commitOK = operation.commit();
                
            } catch (InternalCriticalException e) {
                isMainException = true;
                throw new InternalCriticalException(e);
            } catch (Exception e) {
                isMainException = true;
                throw new InternalCriticalException(e, info());
            } finally {
                try {
                    if (!commitOK) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, info());
                            else
                                logger.error(e, info(), SharedConstant.NOT_THROWN);
                        }
                    } else if (commitOK) {
                        /** inside the thread */
                        postCommit(meta, getBucket(), meta.getVersion());
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

        if (commitOK) {
            try {
                getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                        .getBean(AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, meta.getVersion()));
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }
        }
    }

    private VirtualFileSystemOperation deleteObject(int version) {
        return getJournalService().deleteObject(getBucket(), getObjectName(), version);
    }

    protected void deleteBucketAllPreviousVersions(ServerBucket bucket) {
        getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                .getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName(), bucket.getId()));
    }

    /**
     * Sync no need to locks
     * 
     * @param bucketName
     * @param objectName
     * @param headVersion newest version of the Object just deleted
     */
    private void postCommit(ObjectMetadata meta, ServerBucket bucket, int headVersion) {
        String objectName = meta.getObjectName();
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
    private void backup(ObjectMetadata meta) {
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
            throw new InternalCriticalException(e, objectInfo(meta));
        }
    }
}
