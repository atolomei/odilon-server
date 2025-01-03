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

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;

import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 0. Delete Object Handler
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDZeroDeleteObjectHandler extends RAIDZeroTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroDeleteObjectHandler.class.getName());

    protected RAIDZeroDeleteObjectHandler(RAIDZeroDriver driver, ServerBucket bucket, String objectName) {
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
        boolean isMainException = false;
        int headVersion = -1;

        objectWriteLock();
        try {

            bucketReadLock();
            try {

                checkExistsBucket();
                checkExistObject();

                operation = deleteObjectOperation(getMetadata().getVersion());

                /** backup */
                FileUtils.copyDirectory(getObjectPath().metadataDirPath().toFile(),
                        getObjectPath().metadataBackupDirPath().toFile());

                /** Delete Metadata directory */
                FileUtils.deleteQuietly(getObjectPath().metadataDirPath().toFile());

                commitOK = operation.commit();

            } catch (OdilonObjectNotFoundException e1) {
                isMainException = true;
                throw e1;
            } catch (Exception e) {
                isMainException = true;
                throw new InternalCriticalException(e, info());
            } finally {
                try {
                    if (commitOK) {
                        remove(headVersion);
                    } else {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw e;
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
    }

    /**
     * <p>
     * This method is executed by the delete thread. It does not need to control
     * concurrent access because the caller method does it. It must be fast since it
     * is part of the main transaction
     * </p>
     * 
     * @param meta
     * @param headVersion
     */
    private void remove(int headVersion) {
        try {
            /** delete data versions(1..n-1) */
            for (int version = 0; version <= headVersion; version++)
                FileUtils.deleteQuietly(getObjectPath().dataFileVersionPath(version).toFile());
            /**
             * delete metadata (head) not required because it was done before commit delete
             * data (head)
             */
            FileUtils.deleteQuietly(getObjectPath().dataFilePath().toFile());

            /** delete backup Metadata */
            FileUtils.deleteQuietly(getObjectPath().metadataWorkFilePath().toFile());
        } catch (Exception e) {
            logger.error(e, info(), SharedConstant.NOT_THROWN);
        }
    }

    private VirtualFileSystemOperation deleteObjectOperation(int headVersion) {
        return deleteObject(getBucket(), getObjectName(), headVersion);
    }
}
