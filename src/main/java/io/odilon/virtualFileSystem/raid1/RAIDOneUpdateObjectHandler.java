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
package io.odilon.virtualFileSystem.raid1;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 1. Update Handler
 * </p>
 * <ul>
 * <li>VFSop.UPDATE_OBJECT</li>
 * <li>VFSop.UPDATE_OBJECT_METADATA</li>
 * <li>VFSop.RESTORE_OBJECT_PREVIOUS_VERSION</li>
 * <ul>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneUpdateObjectHandler extends RAIDOneHandler {

    private static Logger logger = Logger.getLogger(RAIDOneUpdateObjectHandler.class.getName());

    /**
     * Instances of this class are used internally by {@link RAIDOneDriver}
     * 
     * @param driver
     */
    protected RAIDOneUpdateObjectHandler(RAIDOneDriver driver) {
        super(driver);
    }

    @Override
    protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
        return getDriver().getReadDrive(bucket, objectName);
    }
    
    /**
     * This check must be executed inside the critical section
     * 
     * @param bucket
     * @param objectName
     * @return
     */
   protected boolean existsObjectMetadata(ServerBucket bucket, String objectName) {
       if (existsCacheObject(bucket, objectName))
            return true;
         return getDriver().getReadDrive(bucket, objectName).existsObjectMetadata(bucket, objectName);
    }
   
    
    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * @param contentType
     * @param customTags
     */

    protected void update(ServerBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType,
            Optional<List<String>> customTags) {

        VirtualFileSystemOperation op = null;
        boolean done = false;

        int beforeHeadVersion = -1;
        int afterHeadVersion = -1;
        boolean isMainException = false;

        getLockService().getObjectLock(bucket, objectName).writeLock().lock();
        try {
            getLockService().getBucketLock(bucket).readLock().lock();
            try {
                
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(bucket))
                    throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));
                
                if (!existsObjectMetadata(bucket, objectName))
                    throw new IllegalArgumentException("Object does not exist -> " + objectInfo(bucket, objectName));
                
                ObjectMetadata meta = getHandlerObjectMetadataInternal(bucket, objectName, true);
                
                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));
                
                beforeHeadVersion = meta.getVersion();

                op = getJournalService().updateObject(bucket, objectName, beforeHeadVersion);

                /** backup current head version */
                saveVersionObjectDataFile(bucket, objectName, meta.getVersion());
                saveVersionObjectMetadata(bucket, objectName, meta.getVersion());

                /** copy new version as head version */
                afterHeadVersion = meta.version + 1;
                saveObjectDataFile(bucket, objectName, stream, srcFileName, meta.getVersion() + 1);
                saveObjectMetadata(bucket, objectName, srcFileName, contentType, meta.getVersion() + 1, customTags);

                done = op.commit();

            } catch (InternalCriticalException e) {
                done = false;
                isMainException = true;
                throw e;
                        
            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));

            } finally {

                try {
                    try {
                        if (stream != null)
                            stream.close();
                    } catch (IOException e) {
                        logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                    }

                    if (!done) {
                        try {
                            rollbackJournal(op, false);

                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
                            else
                                logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /**
                         * TODO AT -> this is after commit, Sync by the moment. see how to make it Async
                         */
                        cleanUpUpdate(op, bucket, objectName, beforeHeadVersion, afterHeadVersion);
                    }
                } finally {
                    getLockService().getBucketLock(bucket).readLock().unlock();

                }
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
        }
    }

    protected ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {

        VirtualFileSystemOperation op = null;
        boolean done = false;
        boolean isMainException = false;

        int beforeHeadVersion = -1;

        getLockService().getObjectLock(bucket, objectName).writeLock().lock();

        try {
            getLockService().getBucketLock(bucket).readLock().lock();

            try {

                ObjectMetadata meta = getHandlerObjectMetadataInternal(bucket, objectName, false);

                if ((meta == null) || (!meta.isAccesible()))
                    throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));
                
                if (meta.getVersion() == 0)
                    throw new IllegalArgumentException("Object does not have any previous version | " + "b:"
                            + (Optional.ofNullable(bucket).isPresent() ? (bucket.getId()) : "null") + ", o:"
                            + (Optional.ofNullable(objectName).isPresent() ? (objectName) : "null"));

                beforeHeadVersion = meta.version;
                List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();

                for (int version = 0; version < beforeHeadVersion; version++) {

                    ObjectMetadata mv = getDriver().getReadDrive(bucket, objectName).getObjectMetadataVersion(bucket, objectName,
                            version);

                    if (mv != null)
                        metaVersions.add(mv);
                }

                if (metaVersions.isEmpty())
                    throw new OdilonObjectNotFoundException(Optional.of(meta.getSystemTags()).orElse("previous versions deleted"));

                op = getJournalService().restoreObjectPreviousVersion(bucket, objectName, beforeHeadVersion);

                /**
                 * save current head version MetadataFile .vN and data File vN - no need to
                 * additional backup
                 */
                saveVersionObjectDataFile(bucket, objectName, meta.getVersion());
                saveVersionObjectMetadata(bucket, objectName, meta.getVersion());

                /** save previous version as head */
                ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size() - 1);

                if (!restoreVersionObjectDataFile(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));

                if (!restoreVersionObjectMetadata(bucket, metaToRestore.getObjectName(), metaToRestore.getVersion()))
                    throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));

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
                            String msg = getDriver().objectInfo(bucket, objectName);
                            if (!isMainException)
                                throw new InternalCriticalException(e, msg);
                            else
                                logger.error(e, msg, SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /** this is after commit, Sync by the moment see how to make it Async */
                        cleanUpRestoreVersion(op, bucket, objectName, beforeHeadVersion);
                    }
                } finally {
                    getLockService().getBucketLock(bucket).readLock().unlock();
                }
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
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
        Check.requireNonNullArgument(meta.bucketId, "meta.bucketId is null");

        VirtualFileSystemOperation op = null;
        boolean done = false;
        boolean isMainException = false;
        ServerBucket bucket = null;

        getLockService().getObjectLock(meta.getBucketId(), meta.getObjectName()).writeLock().lock();

        try {

            getLockService().getBucketLock(meta.getBucketId()).readLock().lock();

            try {

                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(meta.getBucketName()))
                    throw new IllegalArgumentException("bucket does not exist -> " + meta.getBucketName());

                // TODO VER
                bucket = getBucketCache().get(meta.getBucketId());

                op = getJournalService().updateObjectMetadata(getBucketCache().get(meta.getBucketId()), meta.getObjectName(),
                        meta.getVersion());

                backupMetadata(meta, bucket);
                saveObjectMetadata(meta);

                done = op.commit();

            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e);

            } finally {

                try {
                    if ((!done) && (op != null)) {
                        try {
                            rollbackJournal(op, false);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, getDriver().objectInfo(meta.bucketId, meta.objectName));
                            else
                                logger.error(e, "b:" + meta.bucketId.toString() + " o:" + meta.objectName,
                                        SharedConstant.NOT_THROWN);
                        }
                    } else {
                        /**
                         * TODO AT -> Sync by the moment see how to make it Async
                         */
                        cleanUpBackupMetadataDir(bucket, meta.objectName);
                    }

                } finally {
                    getLockService().getBucketLock(bucket).readLock().unlock();
                }
            }
        } finally {
            getLockService().getObjectLock(bucket, meta.getObjectName()).writeLock().unlock();
        }
    }

    protected void onAfterCommit(ServerBucket bucket, String objectName, int previousVersion, int currentVersion) {
    }

    protected void rollbackJournal(VirtualFileSystemOperation op, boolean recoveryMode) {

        Check.requireNonNullArgument(op, "op is null");
        Check.requireTrue(
                (op.getOperationCode() == OperationCode.UPDATE_OBJECT
                        || op.getOperationCode() == OperationCode.UPDATE_OBJECT_METADATA
                        || op.getOperationCode() == OperationCode.RESTORE_OBJECT_PREVIOUS_VERSION),
                VirtualFileSystemOperation.class.getSimpleName() + " can not be  ->  op: " + op.getOperationCode().getName());

        if (op.getOperationCode() == OperationCode.UPDATE_OBJECT)
            rollbackJournalUpdate(op, recoveryMode);

        else if (op.getOperationCode() == OperationCode.UPDATE_OBJECT_METADATA)
            rollbackJournalUpdateMetadata(op, recoveryMode);

        else if (op.getOperationCode() == OperationCode.RESTORE_OBJECT_PREVIOUS_VERSION)
            rollbackJournalUpdate(op, recoveryMode);
    }

    private void rollbackJournalUpdate(VirtualFileSystemOperation op, boolean recoveryMode) {

        boolean done = false;

        try {

            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(op);

            ServerBucket bucket = getBucketCache().get(op.getBucketId());

            restoreVersionObjectDataFile(bucket, op.getObjectName(), op.getVersion());
            restoreVersionObjectMetadata(bucket, op.getObjectName(), op.getVersion());

            done = true;

        } catch (InternalCriticalException e) {
            logger.error(getDriver().opInfo(op));
            if (!recoveryMode)
                throw (e);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(op));
            else
                logger.error(e, opInfo(op), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            }
        }
    }

    private void rollbackJournalUpdateMetadata(VirtualFileSystemOperation op, boolean recoveryMode) {

        boolean done = false;
        try {
            if (getVirtualFileSystemService().getServerSettings().isStandByEnabled())
                getVirtualFileSystemService().getReplicationService().cancel(op);

            ServerBucket bucket = getBucketCache().get(op.getBucketId());

            restoreVersionObjectMetadata(bucket, op.getObjectName(), op.getVersion());

            done = true;

        } catch (InternalCriticalException e) {
            logger.error(opInfo(op));
            if (!recoveryMode)
                throw (e);

        } catch (Exception e) {

            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(op));
            else
                logger.error(opInfo(op), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            }
        }
    }

    private void saveVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {
        // TODO AT: parallel
        try {
            for (Drive drive : getDriver().getDrivesAll())
                drive.putObjectMetadataVersionFile(bucket, objectName, version, drive.getObjectMetadataFile(bucket, objectName));

        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }

    }

    private void saveVersionObjectDataFile(ServerBucket bucket, String objectName, int version) {
        // TODO AT: parallel
        try {
            for (Drive drive : getDriver().getDrivesAll()) {

                ObjectPath path = new ObjectPath(drive, bucket, objectName);
                File file = path.dataFilePath().toFile();

                // File file = ((SimpleDrive) drive).getObjectDataFile(bucket.getId(),
                // objectName);
                ((SimpleDrive) drive).putObjectDataVersionFile(bucket.getId(), objectName, version, file);
            }
        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }

    private void saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream, String srcFileName,
            int newVersion) {

        int total_drives = getDriver().getDrivesAll().size();
        byte[] buf = new byte[ServerConstant.BUFFER_SIZE];

        BufferedOutputStream out[] = new BufferedOutputStream[total_drives];
        InputStream sourceStream = null;

        boolean isMainException = false;

        try {

            sourceStream = isEncrypt() ? getVirtualFileSystemService().getEncryptionService().encryptStream(stream) : stream;

            int n_d = 0;
            for (Drive drive : getDriver().getDrivesAll()) {

                ObjectPath path = new ObjectPath(drive, bucket.getId(), objectName);
                String sPath = path.dataFilePath().toString();

                // String sPath = ((SimpleDrive) drive).getObjectDataFilePath(bucket.getId(),
                // objectName);
                out[n_d++] = new BufferedOutputStream(new FileOutputStream(sPath), ServerConstant.BUFFER_SIZE);
            }
            int bytes_read = 0;

            if (getDriver().getDrivesAll().size() < 2) {

                while ((bytes_read = sourceStream.read(buf, 0, buf.length)) >= 0)
                    for (int bytes = 0; bytes < total_drives; bytes++) {
                        out[bytes].write(buf, 0, bytes_read);
                    }
            } else {

                final int size = getDriver().getDrivesAll().size();
                ExecutorService executor = Executors.newFixedThreadPool(size);

                while ((bytes_read = sourceStream.read(buf, 0, buf.length)) >= 0) {

                    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(size);

                    for (int index = 0; index < total_drives; index++) {

                        final int t_index = index;
                        final int t_bytes_read = bytes_read;

                        tasks.add(() -> {
                            try {
                                out[t_index].write(buf, 0, t_bytes_read);
                                return Boolean.valueOf(true);
                            } catch (Exception e) {
                                logger.error(e, SharedConstant.NOT_THROWN);
                                return Boolean.valueOf(false);
                            }
                        });
                    }

                    try {
                        List<Future<Boolean>> future = executor.invokeAll(tasks, 5, TimeUnit.MINUTES);
                        Iterator<Future<Boolean>> it = future.iterator();
                        while (it.hasNext()) {
                            if (!it.next().get())
                                throw new InternalCriticalException(getDriver().objectInfo(bucket, objectName, srcFileName));
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new InternalCriticalException(e);
                    }

                }

            } // else

        } catch (Exception e) {
            isMainException = true;
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName, srcFileName));

        } finally {
            IOException secEx = null;

            if (out != null) {
                try {
                    for (int n = 0; n < total_drives; n++) {
                        if (out[n] != null)
                            out[n].close();
                    }
                } catch (IOException e) {
                    logger.error(e, getDriver().objectInfo(bucket, objectName, srcFileName)
                            + (isMainException ? SharedConstant.NOT_THROWN : ""));
                    secEx = e;
                }
            }

            try {
                if (sourceStream != null)
                    sourceStream.close();
            } catch (IOException e) {
                logger.error(e, getDriver().objectInfo(bucket, objectName, srcFileName)
                        + (isMainException ? SharedConstant.NOT_THROWN : ""));
                secEx = e;
            }
            if (!isMainException && (secEx != null))
                throw new InternalCriticalException(secEx);
        }
    }

    private void saveObjectMetadata(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        for (Drive drive : getDriver().getDrivesAll()) {
            drive.saveObjectMetadata(meta);
        }
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     */
    private void saveObjectMetadata(ServerBucket bucket, String objectName, String srcFileName, String contentType, int version,
            Optional<List<String>> customTags) {

        Check.requireNonNullArgument(bucket, "bucket is null");

        OffsetDateTime now = OffsetDateTime.now();
        String sha = null;
        String basedrive = null;

        final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

        // try {

        for (Drive drive : getDriver().getDrivesAll()) {

            // File file = ((SimpleDrive) drive).getObjectDataFile(bucket.getId(),
            // objectName);

            ObjectPath path = new ObjectPath(drive, bucket, objectName);
            File file = path.dataFilePath().toFile();

            try {

                String sha256 = OdilonFileUtils.calculateSHA256String(file);

                if (sha == null) {
                    sha = sha256;
                    basedrive = drive.getName();
                } else {
                    if (!sha256.equals(sha))
                        throw new InternalCriticalException("SHA 256 are not equal for drives -> " + basedrive + ":" + sha + " vs "
                                + drive.getName() + ":" + sha256);
                }

                ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
                meta.fileName = srcFileName;
                meta.appVersion = OdilonVersion.VERSION;
                meta.contentType = contentType;
                meta.encrypt = getVirtualFileSystemService().isEncrypt();
                meta.vault = getVirtualFileSystemService().isUseVaultNewFiles();
                meta.creationDate = now;
                meta.version = version;
                meta.versioncreationDate = meta.creationDate;
                meta.length = file.length();
                meta.etag = sha256; /** sha256 is calculated on the encrypted file */
                meta.integrityCheck = now;
                meta.sha256 = sha256;
                meta.status = ObjectStatus.ENABLED;
                meta.drive = drive.getName();
                meta.raid = String.valueOf(getRedundancyLevel().getCode()).trim();
                if (customTags.isPresent())
                    meta.customTags = customTags.get();

                list.add(meta);

            } catch (Exception e) {
                String msg = getDriver().objectInfo(bucket, objectName, srcFileName);
                logger.error(e, msg);
                throw new InternalCriticalException(e, msg);
            }
        }

        /** save in parallel */
        saveRAIDOneObjectMetadataToDisk(getDriver().getDrivesAll(), list, true);
    }
    

    

    private boolean restoreVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {
        try {

            boolean success = true;
            for (Drive drive : getDriver().getDrivesAll()) {
                File file = drive.getObjectMetadataVersionFile(bucket, objectName, version);
                if (file.exists()) {
                    drive.putObjectMetadataFile(bucket, objectName, file);
                    FileUtils.deleteQuietly(file);
                } else
                    success = false;
            }
            return success;
        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }

    private boolean restoreVersionObjectDataFile(ServerBucket bucket, String objectName, int version) {
        try {
            boolean success = true;

            for (Drive drive : getDriver().getDrivesAll()) {
                ObjectPath path = new ObjectPath(drive, bucket, objectName);
                File file = path.dataFileVersionPath(version).toFile();
                // File file = ((SimpleDrive) drive).getObjectDataVersionFile(bucket.getId(),
                // objectName, version);

                if (file.exists()) {
                    ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), objectName, file);
                    FileUtils.deleteQuietly(file);
                } else
                    success = false;
            }
            return success;
        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }

    /**
     * 
     * 
     * @param op               can be null
     * @param bucket           not null
     * @param objectName       not null
     * @param versionDiscarded if<0 do nothing
     */
    private void cleanUpRestoreVersion(VirtualFileSystemOperation op, ServerBucket bucket, String objectName,
            int versionDiscarded) {

        if ((op == null) || (versionDiscarded < 0))
            return;

        try {
            for (Drive drive : getDriver().getDrivesAll()) {
                FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket, objectName, versionDiscarded));

                ObjectPath path = new ObjectPath(drive, bucket, objectName);
                FileUtils.deleteQuietly(path.dataFileVersionPath(versionDiscarded).toFile());
                // FileUtils.deleteQuietly(((SimpleDrive)
                // drive).getObjectDataVersionFile(bucket.getId(), objectName,
                // versionDiscarded));

            }
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    /**
     * copy metadata directory
     * 
     * @param bucket
     * @param objectName
     */
    private void backupMetadata(ObjectMetadata meta, ServerBucket bucket) {
        Check.requireNonNullArgument(meta, "meta is null");
        try {
            for (Drive drive : getDriver().getDrivesAll()) {
                String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket, meta.objectName);
                String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator + meta.objectName;
                File src = new File(objectMetadataDirPath);
                if (src.exists())
                    FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
            }

        } catch (IOException e) {
            throw new InternalCriticalException(e, meta.toString());
        }
    }

    /**
     * 
     * @param op              can be null (do nothing)
     * @param bucket          not null
     * @param objectName      not null
     * @param previousVersion >=0
     * @param currentVersion  > 0
     */
    private void cleanUpUpdate(VirtualFileSystemOperation op, ServerBucket bucket, String objectName, int previousVersion,
            int currentVersion) {

        if (op == null)
            return;

        try {
            Check.requireNonNullArgument(bucket, "meta is null");
            if (!getVirtualFileSystemService().getServerSettings().isVersionControl()) {
                for (Drive drive : getDriver().getDrivesAll()) {
                    FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket, objectName, previousVersion));
                    ObjectPath path = new ObjectPath(drive, bucket, objectName);
                    File file = path.dataFileVersionPath(previousVersion).toFile();
                    FileUtils.deleteQuietly(file);
                    // FileUtils.deleteQuietly(((SimpleDrive)
                    // drive).getObjectDataVersionFile(bucket.getId(), objectName,
                    // previousVersion));

                }
            }
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    private void cleanUpBackupMetadataDir(ServerBucket bucket, String objectName) {
        try {
            /** delete backup Metadata */
            for (Drive drive : getDriver().getDrivesAll()) {
                FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket) + File.separator + objectName));
            }
        } catch (Exception e) {
            logger.error(e, objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
        }
    }

}
