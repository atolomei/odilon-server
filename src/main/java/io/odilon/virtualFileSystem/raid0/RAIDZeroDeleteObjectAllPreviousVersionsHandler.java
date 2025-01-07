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

import java.nio.file.Files;
import java.time.OffsetDateTime;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
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

    /**
     * remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json"
     */
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

                /** backup */
                FileUtils.copyDirectory(getObjectPath().metadataDirPath().toFile(),
                        getObjectPath().metadataBackupDirPath().toFile());

                /** start operation */
                operation = deleteObjectPreviousVersions(meta.getVersion());

                /** delete versions (metadata) */
                for (int version = 0; version < meta.getVersion(); version++)
                    FileUtils.deleteQuietly(getObjectPath().metadataFileVersionPath(version).toFile());

                meta.addSystemTag("delete versions");
                meta.setLastModified(OffsetDateTime.now());
                Files.writeString(getObjectPath().metadataFilePath(), getObjectMapper().writeValueAsString(meta));

                /** commit */
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

                            /** after commit is ok -> remove all data files */
                            /** delete data versions(1..n-1). keep headVersion **/
                            for (int version = 0; version < meta.getVersion(); version++)
                                FileUtils.deleteQuietly(getObjectPath().dataFileVersionPath(version).toFile());

                            /** delete backup Metadata */
                            FileUtils.deleteQuietly(getObjectPath().metadataWorkFilePath().toFile());

                            /** async job that sweeps away temporary files (not doing anything by the moment though) */
                            getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext().getBean(
                                    AfterDeleteObjectServiceRequest.class, operation.getOperationCode(), meta, meta.getVersion()));

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
    }

    private VirtualFileSystemOperation deleteObjectPreviousVersions(int headVersion) {
        return deleteObjectPreviousVersions(getBucket(), getObjectName(), headVersion);
    }
}
