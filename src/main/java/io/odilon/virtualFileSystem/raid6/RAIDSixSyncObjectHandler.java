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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 6. Sync Object. This class regenerates the object's chunks when a new
 * disk is added
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixSyncObjectHandler extends RAIDSixHandler {

    private static Logger logger = Logger.getLogger(RAIDSixSyncObjectHandler.class.getName());

    @JsonIgnore
    private List<Drive> drives;

    @JsonIgnore
    private List<Drive> drivesToSync;

    /**
     * @param driver can not be null
     */
    protected RAIDSixSyncObjectHandler(RAIDSixDriver driver) {
        super(driver);
    }

    /**
     * @param meta can not be null
     */
    public void sync(ObjectMetadata meta) {

        Check.requireNonNullArgument(meta, "meta is null");
        Check.requireNonNullStringArgument(meta.getBucketName(), "bucketName is null");
        Check.requireNonNullStringArgument(meta.getObjectName(), "objectName is null or empty | b:" + meta.getBucketName());

        String objectName = meta.getObjectName();

        VirtualFileSystemOperation operation = null;
        boolean done = false;
        ServerBucket bucket;

       objectWriteLock(meta.getBucketId(), objectName);
        try {

            bucketReadLock(meta.getBucketId());
            try {
                /**
                 * This check was executed by the VirtualFilySystemService, but it must be
                 * executed also inside the critical zone.
                 */
                if (!existsCacheBucket(meta.getBucketId()))
                    throw new IllegalArgumentException("bucket does not exist -> " + meta.getBucketId().toString());

                bucket = getBucketCache().get(meta.getBucketId());

                /**
                 * backup metadata, there is no need to backup data because 
                 * existing data files are not touched.
                 **/
                backupMetadata(bucket, meta);

                operation = getJournalService().syncObject(bucket, objectName);

                /** HEAD VERSION --------------------------------------------------------- */

                {
                    /** Data (head) */
                    RAIDSixDecoder decoder = new RAIDSixDecoder(getDriver());
                    File file = decoder.decodeHead(meta, bucket);

                    RAIDSixSDriveSyncEncoder driveInitEncoder = new RAIDSixSDriveSyncEncoder(getDriver(), getDrives());

                    try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {
                        driveInitEncoder.encodeHead(in, bucket, objectName);
                    } catch (FileNotFoundException e) {
                        throw new InternalCriticalException(e, getDriver().objectInfo(meta));
                    } catch (IOException e) {
                        throw new InternalCriticalException(e, getDriver().objectInfo(meta));
                    }

                    /** MetaData (head) */
                    meta.setDateSynced(OffsetDateTime.now());

                    List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
                    getDrivesToSync().forEach(d -> list.add(meta));
                    saveRAIDOneObjectMetadataToDisk(getDrivesToSync(), list, true);
                }

                /** PREVIOUS VERSIONS ---------------------------------------------------- */

                if (getDriver().getVirtualFileSystemService().getServerSettings().isVersionControl()) {

                    for (int version = 0; version < meta.getVersion(); version++) {

                        ObjectMetadata versionMeta = getDriver().getObjectMetadataReadDrive(bucket, objectName)
                                .getObjectMetadataVersion(bucket, objectName, version);

                        if (versionMeta != null) {

                            /** Data (version) */
                            RAIDSixDecoder decoder = new RAIDSixDecoder(getDriver());
                            File file = decoder.decodeVersion(versionMeta, bucket);

                            RAIDSixSDriveSyncEncoder driveEncoder = new RAIDSixSDriveSyncEncoder(getDriver(), getDrives());

                            try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {

                                /**
                                 * encodes version without saving existing blocks, 
                                 * only the ones that go to the new drive/s
                                 */
                                driveEncoder.encodeVersion(in, bucket, objectName, versionMeta.getVersion());

                            } catch (FileNotFoundException e) {
                                throw new InternalCriticalException(e, getDriver().objectInfo(meta));
                            } catch (IOException e) {
                                throw new InternalCriticalException(e, getDriver().objectInfo(meta));
                            }

                            /** Metadata (version) */
                            /**
                             * changes the date of sync in order to prevent this object's sync if the
                             * process is re run
                             */
                            versionMeta.setDateSynced(OffsetDateTime.now());

                            List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
                            getDrives().forEach(d -> list.add(versionMeta));
                            saveRAIDOneObjectMetadataToDisk(getDrives(), list, false);

                        } else {
                            logger.warn("previous version was deleted for Object -> " + String.valueOf(version) + " |  head "
                                    + objectInfo(meta) + "  head version:" + String.valueOf(meta.getVersion()));
                        }
                    }
                }
                done = operation.commit();
            } finally {
                try {
                    if ((!done) && (operation != null)) {
                        try {
                            rollbackJournal(operation, false);
                        } catch (Exception e) {
                            throw new InternalCriticalException(e, objectInfo(meta));
                        }
                    }
                } finally {
                    bucketReadUnLock(meta.getBucketId());
                }
            }
        } finally {
            objectWriteUnLock(meta.getBucketId(), objectName);
        }
    }

    @Override
    public void rollbackJournal(VirtualFileSystemOperation operation, boolean recoveryMode) {
        Check.requireNonNullArgument(operation, "operation is null");
        Check.requireTrue(operation.getOperationCode() == OperationCode.SYNC_OBJECT_NEW_DRIVE, 
                "operation can not be  ->  op: " + operation.getOperationCode().getName());

        switch (operation.getOperationCode()) {
        case SYNC_OBJECT_NEW_DRIVE: {
            execRollback(operation, recoveryMode);
            break;
        }
        default: {
            break;
        }
        }
    }

    protected synchronized List<Drive> getDrives() {

        if (this.drives != null)
            return this.drives;

        this.drives = new ArrayList<Drive>();

        getDriver().getDrivesAll().forEach(d -> drives.add(d));
        this.drives.sort(new Comparator<Drive>() {
            @Override
            public int compare(Drive o1, Drive o2) {
                try {
                    return o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder() ? -1 : 1;
                } catch (Exception e) {
                    return 0;
                }
            }
        });
        return this.drives;
    }

    private void execRollback(VirtualFileSystemOperation operation, boolean recoveryMode) {

        boolean done = false;

        String objectName = operation.getObjectName();

        ServerBucket bucket = null;

        getLockService().getObjectLock(operation.getBucketId(), objectName).writeLock().lock();
        try {

            getLockService().getBucketLock(operation.getBucketId()).readLock().lock();
            try {
                bucket = getBucketCache().get(operation.getBucketId());
                restoreMetadata(bucket, objectName);
                done = true;

            } catch (InternalCriticalException e) {
                if (!recoveryMode)
                    throw (e);
                else
                    logger.error(opInfo(operation), SharedConstant.NOT_THROWN);
            } catch (Exception e) {
                if (!recoveryMode)
                    throw new InternalCriticalException(e, opInfo(operation));
                else
                    logger.error(e, opInfo(operation), SharedConstant.NOT_THROWN);
            } finally {
                try {
                    if (done || recoveryMode) {
                        operation.cancel();
                    }
                } finally {
                    getLockService().getBucketLock(bucket).readLock().unlock();
                }
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
        }
    }

    protected synchronized List<Drive> getDrivesToSync() {
        if (this.drivesToSync != null)
            return this.drivesToSync;
        drivesToSync = new ArrayList<Drive>();
        getDrives().forEach(d -> {
            if (d.getDriveInfo().getStatus() == DriveStatus.NOTSYNC)
                drivesToSync.add(d);
        });

        return drivesToSync;
    }

    /**
     * <p>
     * copy metadata directory <br/>
     * . back up the full metadata directory (ie. ObjectMetadata for all versions)
     * </p>
     * 
     * @param bucket
     * @param objectName
     */
    private void backupMetadata(ServerBucket bucket, ObjectMetadata meta) {
        try {

            for (Drive drive : getDriver().getDrivesEnabled()) {
                File src = new File(drive.getObjectMetadataDirPath(bucket, meta.getObjectName()));
                File dest = new File(drive.getBucketWorkDirPath(bucket) + File.separator + meta.getObjectName());
                if (src.exists())
                    FileUtils.copyDirectory(src, dest);
            }
        } catch (IOException e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(meta));
        }
    }

    private void restoreMetadata(ServerBucket bucket, String objectName) {
        try {
            for (Drive drive : getDriver().getDrivesEnabled()) {
                File dest = new File(drive.getObjectMetadataDirPath(bucket, objectName));
                File src = new File(drive.getBucketWorkDirPath(bucket) + File.separator + objectName);
                if (src.exists())
                    FileUtils.copyDirectory(src, dest);
                else
                    throw new InternalCriticalException("backup dir does not exist " + getDriver().objectInfo(bucket, objectName)
                            + "dir:" + src.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }
}
