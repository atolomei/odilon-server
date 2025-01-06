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
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 0. Update Handler
 * </p>
 * <p>
 * All {@link RAIDHandler} are used internally by the corresponding RAID Driver
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDZeroUpdateObjectHandler extends RAIDZeroTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroUpdateObjectHandler.class.getName());

    /**
     * <p>
     * All {@link RAIDHandler} are used internally by the corresponding RAID Driver.
     * in this case by {@code RAIDZeroDriver}
     * </p>
     */
    protected RAIDZeroUpdateObjectHandler(RAIDZeroDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    protected void update(InputStream stream, String srcFileName, String contentType, Optional<List<String>> customTags) {

        boolean isMaixException = false;
        boolean commitOK = false;
        VirtualFileSystemOperation operation = null;
        int beforeHeadVersion = -1;
        objectWriteLock();
        try {

            bucketReadLock();
            try (stream) {

                checkExistsBucket();
                checkExistObject();

                ObjectMetadata meta = getMetadata();
                beforeHeadVersion = meta.getVersion();

                /** backup (current head version) */
                backup(meta.getVersion());

                /** start operation */
                operation = updateObject(meta.getVersion());

                /** copy new version head version */
                saveData(stream, srcFileName);
                saveMetadata(srcFileName, contentType, meta.getVersion() + 1, customTags);

                /** commit */
                commitOK = operation.commit();

            } catch (OdilonObjectNotFoundException e1) {
                isMaixException = true;
                throw e1;
            } catch (InternalCriticalException e2) {
                isMaixException = true;
                throw e2;
            } catch (Exception e) {
                isMaixException = true;
                throw new InternalCriticalException(e, info());

            } finally {
                try {
                    if (!commitOK) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMaixException)
                                throw new InternalCriticalException(e, info());
                            else
                                logger.error(e, info(), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /**
                         * TODO AT -> This is Sync by the moment, see how to make it Async This clean up
                         * is executed after the commit by the transaction thread, and therefore all
                         * locks are still applied. Also it is required to be fast<br/>
                         */
                        try {
                            if ((!getServerSettings().isVersionControl()) && (beforeHeadVersion > -1)) {
                                FileUtils.deleteQuietly(getObjectPath().metadataFileVersionPath(beforeHeadVersion).toFile());
                                FileUtils.deleteQuietly(getObjectPath().dataFileVersionPath(beforeHeadVersion).toFile());
                            }
                        } catch (Exception e) {
                            logger.error(e, SharedConstant.NOT_THROWN);
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

    private void saveMetadata(String srcFileName, String contentType, int afterHeadVersion, Optional<List<String>> customTags) {
        saveMetadata(getBucket(), getObjectName(), srcFileName, contentType, afterHeadVersion, customTags);
    }

    private void saveData(InputStream stream, String srcFileName) {
        saveData(getBucket(), getObjectName(), stream, srcFileName);
    }

    private VirtualFileSystemOperation updateObject(int beforeHeadVersion) {
        return updateObject(getBucket(), getObjectName(), beforeHeadVersion);
    }

    /**
     * backup current head version
     */
    private void backup(int version) {

        /** version data */
        Drive drive = getWriteDrive(getBucket(), getObjectName());
        try {
            ObjectPath path = getObjectPath();
            File file = path.dataFilePath().toFile();
            ((SimpleDrive) drive).putObjectDataVersionFile(getBucket().getId(), getObjectName(), version, file);
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }

        /** version metadata */
        try {
            File file = drive.getObjectMetadataFile(getBucket(), getObjectName());
            drive.putObjectMetadataVersionFile(getBucket(), getObjectName(), version, file);
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }
    }
}
