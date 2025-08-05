/*
 * Odilon Object Storage
 * (c) kbee 
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

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;

import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDOneDeleteObjectAllPreviousVersionsHandler extends RAIDOneTransactionObjectHandler {
                
    private static Logger logger = Logger.getLogger(RAIDOneDeleteObjectAllPreviousVersionsHandler.class.getName());
    
    public RAIDOneDeleteObjectAllPreviousVersionsHandler(RAIDOneDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }
    
    /**
     * remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json"
     **/
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
                
                if (meta.getVersion() == VERSION_ZERO)
                    return;

                backupMetadata();
                
                /** start operation */
                operation = deleteObjectPreviousVersions(meta.getVersion());
                
                for (int version = 0; version < meta.getVersion(); version++) {
                    for (Drive drive : getDriver().getDrivesAll()) {
                        FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile( getBucket(), getObjectName(), version));
                    }
                }
                /** update head metadata with the tag */
                meta.addSystemTag("delete versions");
                meta.lastModified = OffsetDateTime.now();
                for (Drive drive : getDriver().getDrivesAll()) {
                    ObjectMetadata metaDrive = drive.getObjectMetadata( getBucket(), getObjectName());
                    meta.drive = drive.getName();
                    drive.saveObjectMetadata(metaDrive);
                }

                /** commit */
                commitOK = operation.commit();

            
            } catch (InternalCriticalException e1) {
                isMainException = true;
                throw e1;
            } catch (Exception e) {
                isMainException = true;
                throw new InternalCriticalException(e, info());
            } finally {
                try {
                    if (commitOK) {
                        postObjectPreviousVersionDeleteAllCommit(meta.getVersion());
                    }
                    else {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, info());
                            else
                                logger.error(e, info(), SharedConstant.NOT_THROWN);
                        }
                    } 
                } finally {
                    bucketReadUnLock();
                }
            }
        } finally {
            objectWriteUnLock();
        }
        
        if (commitOK) {
            try {
                /** after the TRX commit. It is used to clean temp files,
                * if the system crashes those temp files will be removed on system startup
                */
                getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                        .getBean(AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, meta.getVersion()));
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }   
        }
    }

    /**
     * @param bucket
     * @param objectName
     */
    private void backupMetadata() {
        /** copy metadata directory */
        try {
            for (Drive drive : getDriver().getDrivesAll()) {
                String objectMetadataDirPath = drive.getObjectMetadataDirPath(getBucket(), getObjectName());
                String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(getBucket()) + File.separator + getObjectName();
                File src = new File(objectMetadataDirPath);
                if (src.exists())
                    FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
            }
        } catch (IOException e) {
            throw new InternalCriticalException(e, info());
        }
    }
    
    private void postObjectPreviousVersionDeleteAllCommit( int headVersion) {
        try {
            /** delete data versions(1..n-1). keep headVersion **/
            for (int version = 0; version < headVersion; version++) {
                for (Drive drive : getDriver().getDrivesAll()) {
                    ObjectPath path = new ObjectPath(drive, getBucket(), getObjectName());
                    FileUtils.deleteQuietly(path.dataFileVersionPath(version).toFile());
                }
            }
            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll()) {
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(getBucket()) + File.separator + getObjectName()));
            }
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    private VirtualFileSystemOperation deleteObjectPreviousVersions(int headVersion) {
        return getJournalService().deleteObjectPreviousVersions(getBucket(), getObjectName(), headVersion);
    }

}
