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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.OdilonVersion;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <b>RAID 6 Object creation</b>
 * <p>
 * Auxiliary class used by {@link RaidSixHandler}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@ThreadSafe
public class RAIDSixCreateObjectHandler extends RAIDSixHandler {

    private static Logger logger = Logger.getLogger(RAIDSixCreateObjectHandler.class.getName());

    /**
     * <p>
     * Instances of this class are used internally by {@link RAIDSixDriver}
     * <p>
     * 
     * @param driver
     */
    protected RAIDSixCreateObjectHandler(RAIDSixDriver driver) {
        super(driver);
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * @param contentType
     * @param customTags
     */
    protected void create(ServerBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType,
            Optional<List<String>> customTags) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

        VirtualFileSystemOperation op = null;
        boolean done = false;
        boolean isMainException = false;

        try {

            objectWriteLock(bucket, objectName);

            try (stream) {

                bucketReadLock(bucket);

                if (getDriver().getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucket, objectName))
                    throw new IllegalArgumentException("Object already exist -> " + getDriver().objectInfo(bucket, objectName));

                int version = 0;

                op = getJournalService().createObject(bucket, objectName);

                RAIDSixBlocks raidSixBlocks = saveObjectDataFile(bucket, objectName, stream);
                saveObjectMetadata(bucket, objectName, raidSixBlocks, srcFileName, contentType, version, customTags);
                done = op.commit();

            } catch (InternalCriticalException e) {
                done = false;
                isMainException = true;
                throw e;
            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName, srcFileName));
            } finally {
                try {
                    if ((!done) && (op != null)) {
                        try {
                            rollbackJournal(op, false);
                        } catch (Exception e) {
                            if (isMainException)
                                logger.error(objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                            else
                                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
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
     * VFSop.CREATE_OBJECT
     * </p>
     */
    @Override
    protected void rollbackJournal(VirtualFileSystemOperation op, boolean recoveryMode) {

        Check.requireNonNullArgument(op, "op is null");
        Check.checkTrue(op.getOperationCode() == OperationCode.CREATE_OBJECT, "Invalid op ->  " + op.getOperationCode().getName());

        String objectName = op.getObjectName();

        ServerBucket bucket = getBucketCache().get(op.getBucketId());

        boolean done = false;

        try {
            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(op);

            ObjectMetadata meta = null;

            /** remove metadata dir on all drives */
            for (Drive drive : getDriver().getDrivesAll()) {
                File f_meta = drive.getObjectMetadataFile(bucket, objectName);
                if ((meta == null) && (f_meta != null)) {
                    try {
                        meta = drive.getObjectMetadata(bucket, objectName);
                    } catch (Exception e) {
                        logger.warn("can not load meta -> d: " + drive.getName() + SharedConstant.NOT_THROWN);
                    }
                }
                FileUtils.deleteQuietly(new File(drive.getObjectMetadataDirPath(bucket, objectName)));
            }

            /** remove data dir on all drives */
            if (meta != null)
                getDriver().getObjectDataFiles(meta, bucket, Optional.empty()).forEach(file -> {
                    FileUtils.deleteQuietly(file);
                });

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(e, op.toString() + SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, "Rollback: " + op.toString() + SharedConstant.NOT_THROWN);
            else
                logger.error(e, op.toString() + SharedConstant.NOT_THROWN);
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
     */
    private RAIDSixBlocks saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        try (InputStream sourceStream = isEncrypt() ? (getVirtualFileSystemService().getEncryptionService().encryptStream(stream))
                : stream) {
            return (new RAIDSixEncoder(getDriver())).encodeHead(sourceStream, bucket, objectName);
        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * 
     * 
     *                    todo en el object metadata o cada file por separado
     * 
     */
    private void saveObjectMetadata(ServerBucket bucket, String objectName, RAIDSixBlocks ei, String srcFileName,
            String contentType, int version, Optional<List<String>> customTags) {

        List<String> shaBlocks = new ArrayList<String>();
        StringBuilder etag_b = new StringBuilder();

        ei.getEncodedBlocks().forEach(item -> {
            try {
                shaBlocks.add(OdilonFileUtils.calculateSHA256String(item));
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName) + ", f:"
                        + (Optional.ofNullable(item).isPresent() ? (item.getName()) : "null"));
            }
        });

        shaBlocks.forEach(item -> etag_b.append(item));

        String etag = null;

        try {
            etag = OdilonFileUtils.calculateSHA256String(etag_b.toString());
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
        }

        OffsetDateTime creationDate = OffsetDateTime.now();

        final List<Drive> drives = getDriver().getDrivesAll();
        final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

        for (Drive drive : getDriver().getDrivesAll()) {
            try {
                ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
                meta.setFileName(srcFileName);
                meta.setAppVersion(OdilonVersion.VERSION);
                meta.setContentType(contentType);
                meta.setCreationDate(creationDate);
                meta.setVersion(version);
                meta.setVersioncreationDate(meta.getCreationDate());
                meta.setLength(ei.getFileSize());
                meta.setSha256Blocks(shaBlocks);
                meta.setTotalBlocks(ei.getEncodedBlocks().size());
                meta.setEtag(etag);
                meta.setEncrypt(getVirtualFileSystemService().isEncrypt());
                meta.setIntegrityCheck(meta.getCreationDate());
                meta.setStatus(ObjectStatus.ENABLED);
                meta.setDrive(drive.getName());
                meta.setRaid(String.valueOf(getRedundancyLevel().getCode()).trim());
                if (customTags.isPresent())
                    meta.setCustomTags(customTags.get());
                list.add(meta);
            } catch (Exception e) {
                throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName, srcFileName));
            }
        }

        /** save in parallel */
        getDriver().saveObjectMetadataToDisk(drives, list, true);
    }

}
