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

import java.io.IOException;
import java.time.OffsetDateTime;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 0. Delete Object Previous Versions
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDZeroDeleteObjectAllPreviousVersionsHandler extends RAIDZeroTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroDeleteObjectAllPreviousVersionsHandler.class.getName());

    public RAIDZeroDeleteObjectAllPreviousVersionsHandler(RAIDZeroDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    protected void delete() {

        boolean isMainExcetion = false;
        boolean commitOK = false;
        VirtualFileSystemOperation operation = null;
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

                backup();
                operation = deleteObjectPreviousVersions(meta.getVersion());

                /**
                 * remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json"
                 **/
                for (int version = 0; version < meta.getVersion(); version++)
                    FileUtils.deleteQuietly(getObjectPath().metadataFileVersionPath(version).toFile());

                meta.addSystemTag("delete versions");
                meta.setLastModified(OffsetDateTime.now());

                getDrive().saveObjectMetadata(meta);

                commitOK = operation.commit();

            } catch (InternalCriticalException e) {
                isMainExcetion = true;
                throw e;

            } catch (Exception e) {
                isMainExcetion = true;
                throw new InternalCriticalException(e, info());
            } finally {
                try {
                    if (!commitOK) {
                        try {
                            rollback(operation);
                        } catch (InternalCriticalException e) {
                            if (!isMainExcetion)
                                throw e;
                            else
                                logger.error(e, info(), SharedConstant.NOT_THROWN);
                        } catch (Exception e) {
                            if (!isMainExcetion)
                                throw new InternalCriticalException(e, info());
                            else
                                logger.error(e, info(), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        try {
                            postCommit(meta, getBucket(), meta.getVersion());
                        } catch (Exception e) {
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
                getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext()
                        .getBean(AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, meta.getVersion()));

            } catch (Exception e) {
                logger.error(e, opInfo(operation), SharedConstant.NOT_THROWN);
            }
        }
    }

    private Drive getDrive() {
        return getDriver().getWriteDrive(getBucket(), getObjectName());
    }

    private VirtualFileSystemOperation deleteObjectPreviousVersions(int headVersion) {
        return deleteObjectPreviousVersions(getBucket(), getObjectName(), headVersion);
    }

    /**
     * copy metadata directory
     * 
     * @param bucket
     * @param objectName
     */
    private void backup() {
        try {
            FileUtils.copyDirectory(getObjectPath().metadataDirPath().toFile(), getObjectPath().metadataBackupDirPath().toFile());
        } catch (IOException e) {
            throw new InternalCriticalException(e, info());
        }
    }

    private void postCommit(ObjectMetadata meta, ServerBucket bucket, int headVersion) {
        try {
            /** delete data versions(1..n-1). keep headVersion **/
            for (int n = 0; n < headVersion; n++)
                FileUtils.deleteQuietly(getObjectPath().dataFileVersionPath(n).toFile());

            /** delete backup Metadata */
            FileUtils.deleteQuietly(getObjectPath().metadataWorkFilePath().toFile());

        } catch (Exception e) {
            logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
        }
    }

}
