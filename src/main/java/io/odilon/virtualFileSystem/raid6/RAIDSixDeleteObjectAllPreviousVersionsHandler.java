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

                /** backup */
                backup();

                /** start operation */
                operation = deleteObjectPreviousVersions(meta.getVersion());

                /**
                 * remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json"
                 */
                for (int version = 0; version < meta.getVersion(); version++) {
                    for (Drive drive : getDriver().getDrivesAll()) {
                        FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(getBucket(), getObjectName(), version));
                    }
                }

                meta.addSystemTag("delete versions");
                meta.setLastModified(OffsetDateTime.now());

                final List<Drive> drives = getDriver().getDrivesAll();
                final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

                getDriver().getDrivesAll().forEach(d -> list.add(d.getObjectMetadata(getBucket(), getObjectName())));
                saveRAIDSixObjectMetadataToDisk(drives, list, true);

                /** commit */
                commitOK = operation.commit();

            } catch (OdilonObjectNotFoundException e1) {
                isMainException = true;
                throw (e1);

            } catch (Exception e) {
                isMainException = true;
                throw new InternalCriticalException(e);
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
                        postCommit(meta, getBucket(), meta.getVersion());
                    }
                } finally {
                    bucketReadLock();
                }
            }
        } finally {
            objectWriteLock();
        }

        if (commitOK) {
            onAfterCommit(operation, meta, meta.getVersion());
        }
    }

    /**
     * copy metadata directory
     * 
     * @param bucket
     * @param objectName
     */
    private void backup() {
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
            getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                    .getBean(AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, headVersion));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    private void postCommit(ObjectMetadata meta, ServerBucket bucket, int headVersion) {

        try {
            /** delete data versions(0..headVersion-1). keep headVersion **/
            for (int n = 0; n < headVersion; n++) {
                getDriver().getObjectDataFiles(meta, bucket, Optional.of(n)).forEach(item -> {
                    FileUtils.deleteQuietly(item);
                });
            }

            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll())
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket), meta.getObjectName()));

        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    private VirtualFileSystemOperation deleteObjectPreviousVersions(int headVersion) {
        return getJournalService().deleteObjectPreviousVersions(getBucket(), getObjectName(), headVersion);
    }

}
