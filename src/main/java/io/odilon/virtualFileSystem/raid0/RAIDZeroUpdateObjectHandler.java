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
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.OperationCode;
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
public class RAIDZeroUpdateObjectHandler extends RAIDZeroHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroUpdateObjectHandler.class.getName());

    /**
     * <p>
     * All {@link RAIDHandler} are used internally by the corresponding RAID Driver.
     * in this case by {@code RAIDZeroDriver}
     * </p>
     */
    protected RAIDZeroUpdateObjectHandler(RAIDZeroDriver driver) {
        super(driver);
    }

    protected void update(ServerBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType,
            Optional<List<String>> customTags) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null | " + objectInfo(bucket));
        Check.requireNonNullArgument(stream, "stream is null");

        boolean isMaixException = false;
        boolean done = false;
        int beforeHeadVersion = -1;
        int afterHeadVersion = -1;
        VirtualFileSystemOperation operation = null;

        objectWriteLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try (stream) {

                /** must be executed inside the critical zone */
                checkExistsBucket(bucket);

                /** must be executed inside the critical zone */
                checkExistObject(bucket, objectName);

                ObjectMetadata meta = getMetadata(bucket, objectName, true);
                beforeHeadVersion = meta.getVersion();

                operation = updateObject(bucket, objectName, beforeHeadVersion);

                /** backup current head version */
                versionObject(bucket, objectName, beforeHeadVersion);

                /** copy new version head version */
                afterHeadVersion = beforeHeadVersion + 1;

                saveObjectDataFile(bucket, objectName, stream, srcFileName);
                saveObjectMetadata(bucket, objectName, srcFileName, contentType, afterHeadVersion, customTags);

                done = operation.commit();

            } catch (OdilonObjectNotFoundException e1) {
                done = false;
                isMaixException = true;
                throw e1;
            } catch (InternalCriticalException e2) {
                done = false;
                isMaixException = true;
                throw e2;
            } catch (Exception e) {
                done = false;
                isMaixException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));

            } finally {
                try {
                    if (!done) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMaixException)
                                throw new InternalCriticalException(objectInfo(bucket, objectName, srcFileName));
                            else
                                logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /**
                         * TODO AT -> This is Sync by the moment, see how to make it Async This clean up
                         * is executed after the commit by the transaction thread, and therefore all
                         * locks are still applied. Also it is required to be fast<br/>
                         */
                        try {
                            if (!getServerSettings().isVersionControl()) {
                                ObjectPath path = new ObjectPath(getWriteDrive(bucket, objectName), bucket, objectName);
                                FileUtils.deleteQuietly(path.metadataFileVersionPath(beforeHeadVersion).toFile());
                                FileUtils.deleteQuietly(path.dataFileVersionPath(beforeHeadVersion).toFile());
                            }
                        } catch (Exception e) {
                            logger.error(e, SharedConstant.NOT_THROWN);
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
     * . The Object does not have a previous version (ie. version=0) . The Object
     * previous versions were deleted by a {@code deleteObjectAllPreviousVersions}
     * </p>
     */
    protected ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {

        boolean done = false;
        boolean isMainException = false;
        int beforeHeadVersion = -1;
        VirtualFileSystemOperation operation = null;

        objectWriteLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try {

                /** must be executed inside the critical zone. */
                checkExistsBucket(bucket);

                /** must be executed inside the critical zone. */
                checkExistObject(bucket, objectName);

                ObjectMetadata meta = getMetadata(bucket, objectName, false);

                if (meta.getVersion() == 0)
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
                versionObject(bucket, objectName, meta.getVersion());

                /** save previous version as head */
                ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size() - 1);
                metaToRestore.setBucketName(bucket.getName());

                if (!restoreVersionDataFile(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                if (!restoreVersionMetadata(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                done = operation.commit();
                return metaToRestore;

            } catch (OdilonObjectNotFoundException e1) {
                done = false;
                isMainException = true;
                e1.setErrorMessage(e1.getErrorMessage() + " | " + objectInfo(bucket, objectName));
                throw e1;

            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));

            } finally {
                try {
                    if (!done) {
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
        boolean done = false;
        boolean isMainException = false;

        objectWriteLock(meta);
        try {
            bucketReadLock(meta.getBucketId());
            ServerBucket bucket = null;
            try {

                /** must be executed inside the critical zone */
                checkExistsBucket(meta.getBucketId());

                bucket = getBucketCache().get(meta.getBucketId());

                operation = getJournalService().updateObjectMetadata(bucket, meta.getObjectName(), meta.getVersion());

                backupMetadata(meta, bucket);
                getWriteDrive(bucket, meta.getObjectName()).saveObjectMetadata(meta);

                done = operation.commit();
            } catch (Exception e) {
                isMainException = true;
                done = false;
                throw new InternalCriticalException(e, objectInfo(meta));

            } finally {
                try {
                    if (!done) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(meta));
                            else
                                logger.error(e, objectInfo(meta), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /**
                         * TODO AT -> Delete backup Metadata. Sync by the moment see how to make it
                         * Async.
                         */
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
     * </p>
     * There is nothing to do here by the moment
     * </p>
     */

    protected void onAfterCommit(ServerBucket bucket, String objectName, int previousVersion, int currentVersion) {
    }

    /**
     * <p>
     * The procedure is the same for Version Control
     * </p>
     */

    protected void rollbackJournal(VirtualFileSystemOperation operation, boolean recoveryMode) {

        Check.requireNonNullArgument(operation, "op is null");
        Check.requireTrue(
                (operation.getOperationCode() == OperationCode.UPDATE_OBJECT
                        || operation.getOperationCode() == OperationCode.UPDATE_OBJECT_METADATA
                        || operation.getOperationCode() == OperationCode.RESTORE_OBJECT_PREVIOUS_VERSION),
                "invalid state ->  op: " + opInfo(operation));

        switch (operation.getOperationCode()) {
        case UPDATE_OBJECT: {
            rollbackJournalUpdate(operation, recoveryMode);
            break;
        }
        case UPDATE_OBJECT_METADATA: {
            rollbackJournalUpdateMetadata(operation, recoveryMode);
            break;
        }
        case RESTORE_OBJECT_PREVIOUS_VERSION: {
            rollbackJournalUpdate(operation, recoveryMode);
            break;
        }
        default: {
            break;
        }
        }
    }

    /**
     * @param op
     * @param recoveryMode
     */
    private void rollbackJournalUpdate(VirtualFileSystemOperation op, boolean recoveryMode) {

        boolean done = false;

        try {
            if (isStandByEnabled())
                getReplicationService().cancel(op);

            ServerBucket bucket = getBucketCache().get(op.getBucketId());

            restoreVersionDataFile(bucket, op.getObjectName(), op.getVersion());
            restoreVersionMetadata(bucket, op.getObjectName(), op.getVersion());

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(opInfo(op), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, "Rollback | " + getDriver().opInfo(op));
            else
                logger.error(opInfo(op), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            }
        }
    }

    /**
     * @param operation
     * @param recoveryMode
     */
    private void rollbackJournalUpdateMetadata(VirtualFileSystemOperation operation, boolean recoveryMode) {

        boolean done = false;
        try {
            ServerBucket bucket = getBucketCache().get(operation.getBucketId());

            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(operation);

            if (operation.getOperationCode() == OperationCode.UPDATE_OBJECT_METADATA)
                restoreMetadata(bucket, operation.getObjectName());

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(e, getDriver().opInfo(operation), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(operation));
            else
                logger.error(e, opInfo(operation), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                operation.cancel();
            }
        }
    }

    private void versionObject(ServerBucket bucket, String objectName, int version) {
        Drive drive = getWriteDrive(bucket, objectName);
        try {
            ObjectPath path = new ObjectPath(drive, bucket.getId(), objectName);
            File file = path.dataFilePath().toFile();
            ((SimpleDrive) drive).putObjectDataVersionFile(bucket.getId(), objectName, version, file);
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }

        try {
            File file = drive.getObjectMetadataFile(bucket, objectName);
            drive.putObjectMetadataVersionFile(bucket, objectName, version, file);
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
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
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
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
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    /**
     * 
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
            throw new InternalCriticalException(e, objectInfo(bucket, meta.getObjectName()));
        }
    }

    /**
     * restore metadata directory
     */
    private void restoreMetadata(ServerBucket bucket, String objectName) {
        String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucket)
                + File.separator + objectName;
        String objectMetadataDirPath = getDriver().getWriteDrive(bucket, objectName).getObjectMetadataDirPath(bucket, objectName);
        try {
            FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }
}
