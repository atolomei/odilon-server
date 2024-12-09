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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.OdilonVersion;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VFSOp;
import io.odilon.virtualFileSystem.model.VFSOperation;

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

    /**
     * @param customTags
     * 
     */
    protected void update(ServerBucket bucket, String objectName, InputStream stream, String srcFileName,
            String contentType, Optional<List<String>> customTags) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null | " + objectInfo(bucket));
        Check.requireNonNullArgument(stream, "stream is null");

        VFSOperation op = null;
        boolean done = false;

        boolean isMaixException = false;

        int beforeHeadVersion = -1;
        int afterHeadVersion = -1;

        objectWriteLock(bucket, objectName);

        try {
            bucketReadLock(bucket);

            try (stream) {
                if (!getDriver().getWriteDrive(bucket, objectName).existsObjectMetadata(bucket, objectName))
                    throw new IllegalArgumentException(
                            "Object does not exist -> " + objectInfo(bucket, objectName, srcFileName));

                ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket, objectName, false);

                beforeHeadVersion = meta.getVersion();
                op = getJournalService().updateObject(bucket, objectName, beforeHeadVersion);

                /** backup current head version */
                saveVersionObjectDataFile(bucket, objectName, beforeHeadVersion);
                saveVersionObjectMetadata(bucket, objectName, beforeHeadVersion);

                /** copy new version head version */
                afterHeadVersion = beforeHeadVersion + 1;

                saveObjectDataFile(bucket, objectName, stream, srcFileName, afterHeadVersion);
                saveObjectMetadataHead(bucket, objectName, srcFileName, contentType, afterHeadVersion, customTags);

                done = op.commit();

            } catch (OdilonObjectNotFoundException e1) {
                done = false;
                isMaixException = true;
                throw e1;

            } catch (Exception e) {
                done = false;
                isMaixException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));

            } finally {

                try {

                    if ((!done) && (op != null)) {
                        try {
                            rollbackJournal(op, false);
                        } catch (Exception e) {
                            if (!isMaixException)
                                throw new InternalCriticalException(objectInfo(bucket, objectName, srcFileName));
                            else
                                logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /** TODO AT -> This is Sync by the moment, see how to make it Async */
                        cleanUpUpdate(op, bucket, objectName, beforeHeadVersion, afterHeadVersion);
                    }
                } finally {
                    bucketReadUnlock(bucket);
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

        VFSOperation op = null;
        boolean done = false;
        boolean isMainException = false;

        Long bucket_id = bucket.getId();

        int beforeHeadVersion = -1;

        objectWriteLock(bucket, objectName);

        try {
            bucketReadLock(bucket);

            try {

                ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket, objectName, false);

                if (meta.getVersion() == 0)
                    throw new IllegalArgumentException(
                            "Object does not have any previous version | " + objectInfo(bucket, objectName));

                beforeHeadVersion = meta.version;
                List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();

                for (int version = 0; version < beforeHeadVersion; version++) {
                    ObjectMetadata mv = getDriver().getReadDrive(bucket, objectName).getObjectMetadataVersion(bucket_id,
                            objectName, version);
                    if (mv != null)
                        metaVersions.add(mv);
                }

                if (metaVersions.isEmpty())
                    throw new OdilonObjectNotFoundException(
                            Optional.of(meta.systemTags).orElse("previous versions deleted"));

                op = getJournalService().restoreObjectPreviousVersion(bucket, objectName, beforeHeadVersion);

                /**
                 * save current head version MetadataFile .vN and data File vN - no need to
                 * additional backup
                 */
                saveVersionObjectDataFile(bucket, objectName, meta.getVersion());
                saveVersionObjectMetadata(bucket, objectName, meta.getVersion());

                /** save previous version as head */
                ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size() - 1);
                metaToRestore.setBucketName(bucket.getName());

                if (!restoreVersionObjectDataFile(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(
                            Optional.of(meta.systemTags).orElse("previous versions deleted"));

                if (!restoreVersionObjectMetadata(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(
                            Optional.of(meta.systemTags).orElse("previous versions deleted"));

                done = op.commit();

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

                    if ((!done) && (op != null)) {

                        try {
                            rollbackJournal(op, false);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
                            else
                                logger.error(e,
                                        " finally | " + objectInfo(bucket, objectName) + SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /**
                         * TODO AT ->Sync by the moment see how to make it Async
                         */
                        if ((op != null) && ((beforeHeadVersion >= 0))) {
                            try {

                                FileUtils.deleteQuietly(getDriver().getWriteDrive(bucket, objectName)
                                        .getObjectMetadataVersionFile(bucket.getId(), objectName, beforeHeadVersion));

                                FileUtils.deleteQuietly(((SimpleDrive) getDriver().getWriteDrive(bucket, objectName))
                                        .getObjectDataVersionFile(bucket.getId(), objectName, beforeHeadVersion));

                            } catch (Exception e) {
                                logger.error(e, SharedConstant.NOT_THROWN);
                            }
                        }
                    }
                } finally {
                    bucketReadUnlock(bucket);
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

        Check.requireNonNullArgument(meta, "meta is null");
        VFSOperation op = null;
        boolean done = false;
        boolean isMainException = false;

        ServerBucket bucket = getDriver().getVirtualFileSystemService().getBucketById(meta.getBucketId());

        objectWriteLock(meta);

        try {
            getLockService().getBucketLock(bucket).readLock().lock();

            try {
                op = getJournalService().updateObjectMetadata(bucket, meta.getObjectName(), meta.version);
                backupMetadata(meta);
                getWriteDrive(bucket, meta.getObjectName()).saveObjectMetadata(meta);
                done = op.commit();
            } catch (Exception e) {
                isMainException = true;
                done = false;
                throw new InternalCriticalException(e, objectInfo(meta));

            } finally {
                try {
                    if ((!done) && (op != null)) {
                        try {
                            rollbackJournal(op, false);
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
                            FileUtils.deleteQuietly(new File(getDriver().getWriteDrive(bucket, meta.getObjectName())
                                    .getBucketWorkDirPath(meta.getBucketId()) + File.separator + meta.getObjectName()));
                        } catch (Exception e) {
                            logger.error(e, SharedConstant.NOT_THROWN);
                        }

                    }
                } finally {
                    getLockService().getBucketLock(bucket).readLock().unlock();
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

    protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {

        Check.requireNonNullArgument(op, "op is null");
        Check.requireTrue(
                (op.getOp() == VFSOp.UPDATE_OBJECT || op.getOp() == VFSOp.UPDATE_OBJECT_METADATA
                        || op.getOp() == VFSOp.RESTORE_OBJECT_PREVIOUS_VERSION),
                "VFSOperation can not be  ->  op: " + op.getOp().getName());

        switch (op.getOp()) {
        case UPDATE_OBJECT: {
            rollbackJournalUpdate(op, recoveryMode);
            break;
        }
        case UPDATE_OBJECT_METADATA: {
            rollbackJournalUpdateMetadata(op, recoveryMode);
            break;
        }
        case RESTORE_OBJECT_PREVIOUS_VERSION: {
            rollbackJournalUpdate(op, recoveryMode);
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
    private void rollbackJournalUpdate(VFSOperation op, boolean recoveryMode) {

        boolean done = false;

        try {
            if (getVirtualFileSystemService().getServerSettings().isStandByEnabled())
                getVirtualFileSystemService().getReplicationService().cancel(op);

            ServerBucket bucket = getDriver().getVirtualFileSystemService().getBucketById(op.getBucketId());

            restoreVersionObjectDataFile(bucket, op.getObjectName(), op.getVersion());
            restoreVersionObjectMetadata(bucket, op.getObjectName(), op.getVersion());

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(getDriver().opInfo(op), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, "Rollback | " + getDriver().opInfo(op));
            else
                logger.error(getDriver().opInfo(op), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            }
        }
    }

    /**
     * @param op
     * @param recoveryMode
     */
    private void rollbackJournalUpdateMetadata(VFSOperation op, boolean recoveryMode) {

        Check.requireNonNullArgument(op, "op is null");

        boolean done = false;

        try {
            ServerBucket bucket = getDriver().getVirtualFileSystemService().getBucketById(op.getBucketId());

            if (getVirtualFileSystemService().getServerSettings().isStandByEnabled())
                getVirtualFileSystemService().getReplicationService().cancel(op);

            if (op.getOp() == VFSOp.UPDATE_OBJECT_METADATA)
                restoreMetadata(bucket, op.getObjectName());

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(e, getDriver().opInfo(op), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            String msg = "Rollback | " + getDriver().opInfo(op);
            if (!recoveryMode)
                throw new InternalCriticalException(e, msg);
            else
                logger.error(e, msg, SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            }
        }
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * @param newVersion
     */
    private void saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream, String srcFileName,
            int newVersion) {

        byte[] buf = new byte[ServerConstant.BUFFER_SIZE];

        BufferedOutputStream out = null;

        boolean isMainException = false;

        try (InputStream sourceStream = isEncrypt()
                ? getVirtualFileSystemService().getEncryptionService().encryptStream(stream)
                : stream) {

            out = new BufferedOutputStream(new FileOutputStream(((SimpleDrive) getWriteDrive(bucket, objectName))
                    .getObjectDataFilePath(bucket.getId(), objectName)), ServerConstant.BUFFER_SIZE);
            int bytesRead;
            while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0) {
                out.write(buf, 0, bytesRead);
            }

        } catch (Exception e) {
            isMainException = true;
            throw new InternalCriticalException(e);

        } finally {
            IOException secEx = null;
            try {

                if (out != null)
                    out.close();

            } catch (IOException e) {
                logger.error(e, objectInfo(bucket, objectName, srcFileName)
                        + (isMainException ? SharedConstant.NOT_THROWN : ""));
                secEx = e;
            }

            if (!isMainException && (secEx != null))
                throw new InternalCriticalException(secEx);
        }
    }

    /**
     * 
     * <p>
     * sha256 is calculated on the encrypted file
     * </p>
     * 
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     */
    private void saveObjectMetadataHead(ServerBucket bucket, String objectName, String srcFileName, String contentType,
            int version, Optional<List<String>> customTags) {

        OffsetDateTime now = OffsetDateTime.now();
        Drive drive = getWriteDrive(bucket, objectName);
        File file = ((SimpleDrive) drive).getObjectDataFile(bucket.getId(), objectName);

        try {
            String sha256 = OdilonFileUtils.calculateSHA256String(file);
            ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
            meta.setFileName(srcFileName);
            meta.setAppVersion(OdilonVersion.VERSION);
            meta.setContentType(contentType);
            meta.setEncrypt(getVirtualFileSystemService().isEncrypt());
            meta.setVault(getVirtualFileSystemService().isUseVaultNewFiles());
            meta.setCreationDate(now);
            meta.setVersion(version);
            meta.setVersioncreationDate(meta.getCreationDate());
            meta.setLength(file.length());
            meta.setEtag(sha256); /** sha256 is calculated on the encrypted file */
            meta.setIntegrityCheck(now);
            meta.setSha256(sha256);
            meta.setStatus(ObjectStatus.ENABLED);
            meta.setDrive(drive.getName());
            meta.setRaid(String.valueOf(getRedundancyLevel().getCode()).trim());
            if (customTags.isPresent())
                meta.setCustomTags(customTags.get());

            drive.saveObjectMetadata(meta);

        } catch (Exception e) {
            throw new InternalCriticalException(e);
        }
    }

    private void saveVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            File file = drive.getObjectMetadataFile(bucket.getId(), objectName);
            drive.putObjectMetadataVersionFile(bucket.getId(), objectName, version, file);
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    private void saveVersionObjectDataFile(ServerBucket bucket, String objectName, int version) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            File file = ((SimpleDrive) drive).getObjectDataFile(bucket.getId(), objectName);
            ((SimpleDrive) drive).putObjectDataVersionFile(bucket.getId(), objectName, version, file);
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    /**
     * @param bucketName
     * @param objectName
     * @param version
     */
    private boolean restoreVersionObjectMetadata(ServerBucket bucket, String objectName, int versionToRestore) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            File file = drive.getObjectMetadataVersionFile(bucket.getId(), objectName, versionToRestore);
            if (file.exists()) {
                drive.putObjectMetadataFile(bucket.getId(), objectName, file);
                FileUtils.deleteQuietly(file);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    private boolean restoreVersionObjectDataFile(ServerBucket bucket, String objectName, int version) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            File file = ((SimpleDrive) drive).getObjectDataVersionFile(bucket.getId(), objectName, version);
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
    private void backupMetadata(ObjectMetadata meta) {

        ServerBucket bucket = getDriver().getVirtualFileSystemService().getBucketById(meta.getBucketId());

        try {
            String objectMetadataDirPath = getDriver().getWriteDrive(bucket, meta.getObjectName())
                    .getObjectMetadataDirPath(meta.getBucketId(), meta.getObjectName());
            String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, meta.getObjectName())
                    .getBucketWorkDirPath(meta.getBucketId()) + File.separator + meta.getObjectName();
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

        String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, objectName)
                .getBucketWorkDirPath(bucket.getId()) + File.separator + objectName;
        String objectMetadataDirPath = getDriver().getWriteDrive(bucket, objectName)
                .getObjectMetadataDirPath(bucket.getId(), objectName);
        try {
            FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    /**
     * <p>
     * This clean up is executed after the commit by the transaction thread, and
     * therefore all locks are still applied. Also it is required to be fast<br/>
     * 
     * <b>TODO AT</b> -> <i>This method should be Async</i><br/>
     * 
     * <h3>Version Control</h3>
     * <ul>
     * <li>do not remove previous version Metadata</li>
     * <li>do not remove previous version Data</li>
     * </ul>
     * 
     * <h3>No Version Control</h3>
     * <ul>
     * <li>remove previous version Metadata</li>
     * <li>remove previous version Data</li>
     * </ul>
     * </p>
     * 
     * @param op              can be null (no need to do anything)
     * @param bucket          not null
     * @param objectName      not null
     * @param previousVersion >=0
     * @param currentVersion  >0
     */
    private void cleanUpUpdate(VFSOperation op, ServerBucket bucket, String objectName, int previousVersion,
            int currentVersion) {

        if (op == null)
            return;

        try {
            if (!getVirtualFileSystemService().getServerSettings().isVersionControl()) {

                FileUtils.deleteQuietly(getDriver().getWriteDrive(bucket, objectName)
                        .getObjectMetadataVersionFile(bucket.getId(), objectName, previousVersion));

                FileUtils.deleteQuietly(((SimpleDrive) getDriver().getWriteDrive(bucket, objectName))
                        .getObjectDataVersionFile(bucket.getId(), objectName, previousVersion));

            }
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

}
