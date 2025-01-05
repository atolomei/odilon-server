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

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;

import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 1 Handler <br/>
 * Delete methods ({@link OperationCode.DELETE_OBJECT})
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneDeleteObjectHandler extends RAIDOneTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDOneDeleteObjectHandler.class.getName());

    /**
     * Instances of this class are used internally by {@link RAIDOneDriver}
     * 
     * @param driver
     */
    protected RAIDOneDeleteObjectHandler(RAIDOneDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * @param contentType
     */
    protected void delete() {

        VirtualFileSystemOperation operation = null;
        boolean commitOK = false;
        int headVersion = -1;
        ObjectMetadata meta = null;

        objectWriteLock();
        try {
            bucketReadLock();
            try {

                checkExistsBucket();
                checkExistObject();

                meta = getDriver().getReadDrive(getBucket(), getObjectName()).getObjectMetadata(getBucket(), getObjectName());
                headVersion = meta.getVersion();
                
                
                backupMetadata(meta, getBucket());
                
                
                /** start operation */
                operation = deleteObject(headVersion);
                
                // TODO AT: parallel
                for (Drive drive : getDriver().getDrivesAll())
                    ((SimpleDrive) drive).deleteObjectMetadata(getBucket(), getObjectName());
                
                /** commit */
                commitOK = operation.commit();

            } catch (OdilonObjectNotFoundException e1) {
                commitOK = false;
                throw e1;

            } catch (Exception e) {
                commitOK = false;
                throw new InternalCriticalException(e, opInfo(operation));
            } finally {
                try {
                    if (!commitOK) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            throw new InternalCriticalException(e, opInfo(operation));
                        }
                    } else if (commitOK)
                        postObjectDeleteCommit(headVersion);
                    /**
                     * DATA CONSISTENCY: If The system crashes before Commit or Cancel -> next time
                     * the system starts up it will REDO all stored operations. Also, the if there
                     * are error buckets in the drives, they will be normalized when the system
                     * starts.
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

        if (commitOK)
            onAfterCommit(operation, meta, headVersion);
    }

    private VirtualFileSystemOperation deleteObject(int headVersion) {
        return deleteObject(getBucket(), getObjectName(), headVersion);
    }

    /**
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

    private void postObjectDeleteCommit(int headVersion) {
        
        try {
            /** delete data versions(1..n-1) */
            for (int version = 0; version <= headVersion; version++) {
                for (Drive drive : getDriver().getDrivesAll()) {
                    ObjectPath path = new ObjectPath(drive, getBucket(), getObjectName());
                    FileUtils.deleteQuietly(path.dataFileVersionPath(version).toFile());
                }
            }
            /** delete data (head) */
            for (Drive drive : getDriver().getDrivesAll()) {
                ObjectPath path = new ObjectPath(drive, getBucket(), getObjectName());
                File file = path.dataFilePath().toFile();
                FileUtils.deleteQuietly(file);
            }
            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll())
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(getBucket()) + File.separator + getObjectName()));
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

    /**
     * <p>
     * This method is called after the TRX commit. It is used to clean temp files,
     * if the system crashes those temp files will be removed on system startup
     * </p>
     */
    private void onAfterCommit(VirtualFileSystemOperation op, ObjectMetadata meta, int headVersion) {
        try {
            getVirtualFileSystemService().getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                    .getBean(AfterDeleteObjectServiceRequest.class, op.getOperationCode(), meta, headVersion));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }
}
