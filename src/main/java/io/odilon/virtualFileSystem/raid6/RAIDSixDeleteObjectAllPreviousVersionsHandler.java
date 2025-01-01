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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.virtualFileSystem.model.Drive;

import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei) 
 */
public class RAIDSixDeleteObjectAllPreviousVersionsHandler extends RAIDSixTransactionObjectHandler {
            
    private static Logger logger = Logger.getLogger(RAIDSixDeleteObjectAllPreviousVersionsHandler.class.getName());
    
    public RAIDSixDeleteObjectAllPreviousVersionsHandler(RAIDSixDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }
    
    protected void delete() {

        VirtualFileSystemOperation operation = null;
        boolean done = false;
        boolean isMainException = false;
        ObjectMetadata meta = null;
                
        objectWriteLock();
        try {

            bucketReadLock();
            try {
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                checkExistsBucket();
                checkExistObject();
                
                meta = getDriver().getObjectMetadataReadDrive(getBucket(), getObjectName()).getObjectMetadata(getBucket(), getObjectName());
                
                /** It does not delete the head version, only previous versions */
                if (meta.getVersion() == VERSION_ZERO)
                    return;

                operation = deleteObjectPreviousVersions(meta.getVersion()); 

                backupMetadata();
                /**
                 * remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json"
                 **/
                for (int version = 0; version < meta.getVersion(); version++) {
                    for (Drive drive : getDriver().getDrivesAll()) {
                        FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile( getBucket(), getObjectName(), version));
                    }
                }

                /** update head metadata with the tag */
                meta.addSystemTag("delete versions");
                meta.setLastModified(OffsetDateTime.now());

                final List<Drive> drives = getDriver().getDrivesAll();
                final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

                getDriver().getDrivesAll().forEach(d -> list.add(d.getObjectMetadata(getBucket(), getObjectName())));
                saveRAIDSixObjectMetadataToDisk(drives, list, true);

                done = operation.commit();

            } catch (OdilonObjectNotFoundException e1) {
                done = false;
                isMainException = true;
                throw (e1);

            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e);
            }

            finally {

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
                        postObjectPreviousVersionDeleteAllCommit(meta, getBucket(), meta.getVersion());
                    }
                } finally {
                    bucketReadLock();
                }
            }
        } finally {
            objectWriteLock();
        }

        if (done) {
            onAfterCommit(operation, meta, meta.getVersion());
        }

    }

    
    
    /**
     * copy metadata directory
     * 
     * @param bucket
     * @param objectName
     */
    private void backupMetadata() {
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


    /**
     * @param operation
     * @param headVersion
     */
    private void onAfterCommit(VirtualFileSystemOperation operation, ObjectMetadata meta, int headVersion) {
        try {
                getVirtualFileSystemService().getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                        .getBean(AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, headVersion));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }


    private void postObjectPreviousVersionDeleteAllCommit(ObjectMetadata meta, ServerBucket bucket, int headVersion) {

        String objectName = meta.getObjectName();

        try {
            /** delete data versions(0..headVersion-1). keep headVersion **/
            for (int n = 0; n < headVersion; n++) {
                getDriver().getObjectDataFiles(meta, bucket, Optional.of(n)).forEach(item -> {
                    FileUtils.deleteQuietly(item);
                });
            }

            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll())
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket), objectName));

        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }
    
    private VirtualFileSystemOperation deleteObjectPreviousVersions(int headVersion) {
            return getJournalService().deleteObjectPreviousVersions(getBucket(), getObjectName(), headVersion);
    }

    
}
