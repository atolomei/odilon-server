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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDZeroRestoreObjectPreviousVersionHandler extends RAIDZeroTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroRestoreObjectPreviousVersionHandler.class.getName());

    public RAIDZeroRestoreObjectPreviousVersionHandler(RAIDZeroDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    /**
     * <p>
     * The Object does not have a previous version (ie. version=0) . The Object
     * previous versions were deleted by a {@code deleteObjectAllPreviousVersions}
     * </p>
     */
    protected ObjectMetadata restorePreviousVersion() {

        boolean commitOK = false;
        boolean isMainException = false;
        int beforeHeadVersion = -1;
        VirtualFileSystemOperation operation = null;

        objectWriteLock();
        try {

            bucketReadLock();
            try {

                checkExistsBucket();
                checkExistObject();

                ObjectMetadata meta = getMetadata();

                if (meta.getVersion() == VERSION_ZERO)
                    throw new IllegalArgumentException("Object does not have versions -> " + info());

                beforeHeadVersion = meta.getVersion();
                List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
                for (int version = 0; version < beforeHeadVersion; version++) {
                    ObjectMetadata metaVersion = getObjectMapper()
                            .readValue(getObjectPath().metadataFileVersionPath(version).toFile(), ObjectMetadata.class);
                    if (metaVersion != null)
                        metaVersions.add(metaVersion);
                }
                if (metaVersions.isEmpty())
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                /** backup */
                backup(meta.getVersion());

                /** start operation */
                operation = restoreObjectPreviousVersion(beforeHeadVersion);

                /** save previous version as head */
                ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size() - 1);
                metaToRestore.setBucketName(getBucket().getName());

                if (!restoreVersionDataFile(metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                if (!restoreVersionMetadata(getBucket(), metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                /** commit */
                commitOK = operation.commit();

                return metaToRestore;

            } catch (OdilonObjectNotFoundException e1) {
                isMainException = true;
                e1.setErrorMessage(e1.getErrorMessage() + " | " + info());
                throw e1;

            } catch (InternalCriticalException e) {
                isMainException = true;
                throw e;
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
                    } else {
                        /**
                         * TODO AT ->Sync by the moment see how to make it Async
                         */
                        if ((operation != null) && ((beforeHeadVersion >= 0))) {
                            try {

                                /** metadata file */
                                FileUtils.deleteQuietly(getObjectPath().metadataFileVersionPath(beforeHeadVersion).toFile());

                                /** data file */
                                FileUtils.deleteQuietly(getObjectPath().dataFileVersionPath(beforeHeadVersion).toFile());
                                
                            } catch (Exception e) {
                                logger.error(e, SharedConstant.NOT_THROWN);
                            }
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

    private VirtualFileSystemOperation restoreObjectPreviousVersion(int beforeHeadVersion) {
        return getJournalService().restoreObjectPreviousVersion(getBucket(), getObjectName(), beforeHeadVersion);
    }

    /**
     * backup current head version save current head version MetadataFile .vN and
     * data File vN - no need to additional backup
     */
    private void backup(int version) {

        /** version data */
        Drive drive = getWriteDrive(getBucket(), getObjectName());
        try {
            File file = getObjectPath().dataFilePath().toFile();
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

    private boolean restoreVersionMetadata(ServerBucket bucket, String objectName, int versionToRestore) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            File file = drive.getObjectMetadataVersionFile(bucket, objectName, versionToRestore);
            if (file.exists()) {
                drive.putObjectMetadataFile(bucket, objectName, file);
                FileUtils.deleteQuietly(file);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }
    }

    private boolean restoreVersionDataFile(int version) {
        try {
            Drive drive = getWriteDrive(getBucket(), getObjectName());
            ObjectPath path = getObjectPath();
            File file = path.dataFileVersionPath(version).toFile();
            if (file.exists()) {
                ((SimpleDrive) drive).putObjectDataFile(getBucket().getId(), getObjectName(), file);
                FileUtils.deleteQuietly(file);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }
    }

}
