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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
        int beforeHeadVersion = -1;
        int afterHeadVersion = -1;
        VirtualFileSystemOperation operation = null;

        objectWriteLock();
        try {

            bucketReadLock();
            try (stream) {

                checkExistsBucket();
                checkExistObject();

                ObjectMetadata meta = getMetadata();
                beforeHeadVersion = meta.getVersion();
                operation = updateObject(beforeHeadVersion);

                /** backup current head version */
                backup(beforeHeadVersion);

                /** copy new version head version */
                afterHeadVersion = beforeHeadVersion + 1;
                saveData(stream, srcFileName);
                saveMetadata(srcFileName, contentType, afterHeadVersion, customTags);

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
                            if (!getServerSettings().isVersionControl()) {
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
     * <p>
     * . The Object does not have a previous version (ie. version=0) . The Object
     * previous versions were deleted by a {@code deleteObjectAllPreviousVersions}
     * </p>
     */
    protected ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {

        boolean commitOK = false;
        boolean isMainException = false;
        int beforeHeadVersion = -1;
        VirtualFileSystemOperation operation = null;

        objectWriteLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try {

                checkExistsBucket(bucket);
                checkExistObject(bucket, objectName);

                ObjectMetadata meta = getMetadata(bucket, objectName, false);

                if (meta.getVersion() == VERSION_ZERO)
                    throw new IllegalArgumentException("Object does not have versions -> " + objectInfo(bucket, objectName));

                beforeHeadVersion = meta.getVersion();
                List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
                for (int version = 0; version < beforeHeadVersion; version++) {
                    ObjectMetadata metaVersion = getDriver().getReadDrive(bucket, objectName).getObjectMetadataVersion(bucket,
                            objectName, version);
                    if (metaVersion != null)
                        metaVersions.add(metaVersion);
                }
                if (metaVersions.isEmpty())
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                operation = getJournalService().restoreObjectPreviousVersion(bucket, objectName, beforeHeadVersion);

                /**
                 * save current head version MetadataFile .vN and data File vN - no need to
                 * additional backup
                 */
                backup(meta.getVersion());

                /** save previous version as head */
                ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size() - 1);
                metaToRestore.setBucketName(bucket.getName());

                if (!restoreVersionDataFile(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                if (!restoreVersionMetadata(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                commitOK = operation.commit();
                return metaToRestore;

            } catch (OdilonObjectNotFoundException e1) {
                commitOK = false;
                isMainException = true;
                e1.setErrorMessage(e1.getErrorMessage() + " | " + objectInfo(bucket, objectName));
                throw e1;

            } catch (Exception e) {
                commitOK = false;
                isMainException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));

            } finally {
                try {
                    if (!commitOK) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
                            else
                                logger.error(e, objectInfo(bucket, objectName) + SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /**
                         * TODO AT ->Sync by the moment see how to make it Async
                         */
                        if ((operation != null) && ((beforeHeadVersion >= 0))) {
                            try {
                                ObjectPath path = new ObjectPath(getDriver().getWriteDrive(bucket, objectName), bucket, objectName);
                                /** metadata file */
                                FileUtils.deleteQuietly(getDriver().getWriteDrive(bucket, objectName)
                                        .getObjectMetadataVersionFile(bucket, objectName, beforeHeadVersion));
                                /** data file */
                                FileUtils.deleteQuietly(path.dataFileVersionPath(beforeHeadVersion).toFile());
                            } catch (Exception e) {
                                logger.error(e, SharedConstant.NOT_THROWN);
                            }
                        }
                    }
                } finally {
                    bucketReadUnLock(bucket);
                }
            }
        } finally {
            objectWriteUnLock(bucket, objectName);
        }
    }

    /**
     * <p>
     * This update does not generate a new Version of the ObjectMetadata. It
     * maintains the same ObjectMetadata version.<br/>
     * The only way to version Object is when the Object Data is updated
     * </p>
     * 
     * @param meta
     */
    protected void updateObjectMetadata(ObjectMetadata meta) {

        VirtualFileSystemOperation operation = null;
        boolean commitOK = false;
        boolean isMainException = false;

        objectWriteLock(meta);
        try {
            bucketReadLock(meta.getBucketId());
            ServerBucket bucket = null;
            try {

                checkExistsBucket(meta.getBucketId());

                bucket = getBucketCache().get(meta.getBucketId());

                operation = getJournalService().updateObjectMetadata(bucket, meta.getObjectName(), meta.getVersion());
                backupMetadata(meta, bucket);
                getWriteDrive(bucket, meta.getObjectName()).saveObjectMetadata(meta);

                commitOK = operation.commit();

            } catch (Exception e) {
                isMainException = true;
                commitOK = false;
                throw new InternalCriticalException(e, objectInfo(meta));

            } finally {
                try {
                    if (!commitOK) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(meta));
                            else
                                logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /** TODO-> Delete backup Metadata. change to Async */
                        try {
                            if (bucket != null)
                                FileUtils.deleteQuietly(new File(
                                        getDriver().getWriteDrive(bucket, meta.getObjectName()).getBucketWorkDirPath(bucket)
                                                + File.separator + meta.getObjectName()));
                        } catch (Exception e) {
                            logger.error(e, SharedConstant.NOT_THROWN);
                        }

                    }
                } finally {
                    bucketReadUnLock(meta.getBucketId());
                }
            }
        } finally {
            objectWriteUnLock(meta);
        }
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

    /**
     * @param bucketName
     * @param objectName
     * @param version
     */
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

    private boolean restoreVersionDataFile(ServerBucket bucket, String objectName, int version) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            ObjectPath path = new ObjectPath(drive, bucket, objectName);
            File file = path.dataFileVersionPath(version).toFile();

            if (file.exists()) {
                ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), objectName, file);
                FileUtils.deleteQuietly(file);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }
    }

    /**
     * copy metadata directory
     */
    private void backupMetadata(ObjectMetadata meta, ServerBucket bucket) {
        try {
            String objectMetadataDirPath = getDriver().getWriteDrive(bucket, meta.getObjectName()).getObjectMetadataDirPath(bucket,
                    meta.getObjectName());
            String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, meta.getObjectName())
                    .getBucketWorkDirPath(bucket) + File.separator + meta.getObjectName();
            File src = new File(objectMetadataDirPath);
            if (src.exists())
                FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
        } catch (IOException e) {
            throw new InternalCriticalException(e, info());
        }
    }

}
