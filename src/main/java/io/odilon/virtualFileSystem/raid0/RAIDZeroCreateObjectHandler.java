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
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import io.odilon.OdilonVersion;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.ObjectDataPathBuilder;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VFSOp;
import io.odilon.virtualFileSystem.model.VFSOperation;

/**
 * <p>
 * RAID 0 Handler <br/>
 * Creates new Objects ({@link VFSOp.CREATE_OBJECT})
 * </p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDZeroCreateObjectHandler extends RAIDZeroHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroCreateObjectHandler.class.getName());

    /**
     * <p>
     * Created and used only from {@link RAIDZeroDriver}
     * </p>
     */
    protected RAIDZeroCreateObjectHandler(RAIDZeroDriver driver) {
        super(driver);
    }

    /**
     * <p>
     * The procedure is the same whether version control is enabled or not
     * </p>
     * 
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * @param contentType
     * @param customTags
     */
    protected void create(@NonNull ServerBucket bucket, @NonNull String objectName, @NonNull InputStream stream, String srcFileName,
            String contentType, Optional<List<String>> customTags) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(bucket.getName(), "bucketName is null");
        Check.requireNonNullArgument(bucket.getId(), "bucket id is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty " + objectInfo(bucket));

        VFSOperation op = null;
        boolean done = false;
        boolean isMainException = false;

        objectWriteLock(bucket, objectName);

        try {
            bucketReadLock(bucket);

            try (stream) {
                if (getDriver().getWriteDrive(bucket, objectName).existsObjectMetadata(bucket, objectName))
                    throw new IllegalArgumentException("Object already exist -> " + objectInfo(bucket, objectName));

                int version = 0;

                op = getJournalService().createObject(bucket, objectName);
                
                saveObjectDataFile(bucket, objectName, stream, srcFileName);
                saveObjectMetadata(bucket, objectName, srcFileName, contentType, version, customTags);
                
                done = op.commit();

            } catch (InternalCriticalException e1) {
                done = false;
                isMainException = true;
                throw e1;

            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
            } finally {
                try {
                    if ((!done) && (op != null)) {
                        try {
                            rollbackJournal(op, false);
                        } catch (InternalCriticalException e) {
                            if (!isMainException)
                                throw e;
                            else
                                logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
                            else
                                logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
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
     * This method is <b>not</b> ThreadSafe, callers must ensure proper concurrency
     * control
     * </p>
     * 
     */
    @Override
    protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {

        Check.requireNonNullArgument(op, "op is null");
        Check.checkTrue(op.getOp() == VFSOp.CREATE_OBJECT, "Invalid op ->  " + op.getOp().getName());

        String objectName = op.getObjectName();

        boolean done = false;

        try {
            if (getVirtualFileSystemService().getServerSettings().isStandByEnabled())
                getVirtualFileSystemService().getReplicationService().cancel(op);

            getWriteDrive(this.getVirtualFileSystemService().getBucketById(op.getBucketId()), objectName)
                    .deleteObjectMetadata(op.getBucketId(), objectName);

            FileUtils.deleteQuietly(new File(
                    getWriteDrive(this.getVirtualFileSystemService().getBucketById(op.getBucketId()), objectName).getRootDirPath(),
                    op.getBucketId().toString() + File.separator + objectName));
            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(e, getDriver().opInfo(op), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, getDriver().opInfo(op));
            else
                logger.error(e, getDriver().opInfo(op), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode)
                op.cancel();
        }
    }

    /**
     * <p>
     * This method is <b>not</b> ThreadSafe, callers must ensure proper concurrency
     * control
     * </p>
     * 
     * @param bucket      can not be null
     * @param objectName  can not be null
     * @param stream      can not be null
     * @param srcFileName can not be null
     */
    private void saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream, String srcFileName) {

        byte[] buf = new byte[ServerConstant.BUFFER_SIZE];

        BufferedOutputStream out = null;
        boolean isMainException = false;

        SimpleDrive drive = (SimpleDrive) getWriteDrive(bucket, objectName);

        ObjectDataPathBuilder pathBuilder = new ObjectDataPathBuilder(drive, bucket, objectName);

        try (InputStream sourceStream = isEncrypt() ? getVirtualFileSystemService().getEncryptionService().encryptStream(stream)
                : stream) {
            out = new BufferedOutputStream(new FileOutputStream(pathBuilder.build()), ServerConstant.BUFFER_SIZE);
            int bytesRead;
            while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0) {
                out.write(buf, 0, bytesRead);
            }
        } catch (Exception e) {
            isMainException = true;
            throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));

        } finally {
            IOException secEx = null;
            try {
                if (out != null)
                    out.close();
            } catch (IOException e) {
                if (isMainException)
                    logger.error(e,objectInfo(bucket, objectName, srcFileName) + (isMainException ? SharedConstant.NOT_THROWN : ""));
                secEx = e;
            }
            if ((!isMainException) && (secEx != null))
                throw new InternalCriticalException(secEx);
        }
    }

    /**
     * <p>
     * This method is <b>not</b> ThreadSafe, callers must ensure proper concurrency
     * control
     * </p>
     * <p>
     * note that sha256 (meta.etag) is calculated on the encrypted file
     * </p>
     * 
     * @param bucket      can not be null
     * @param objectName  can not be null
     * @param stream      can not be null
     * @param srcFileName can not be null
     * @param customTags
     */
    private void saveObjectMetadata(ServerBucket bucket, String objectName, String srcFileName, String contentType, int version,
            Optional<List<String>> customTags) {

        Check.requireNonNullArgument(bucket, "bucket is null");

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
            meta.setEtag(sha256); /** note that -> sha256 is calculated on the encrypted file **/
            meta.setIntegrityCheck(now);
            meta.setSha256(sha256);
            meta.setStatus(ObjectStatus.ENABLED);
            meta.setDrive(drive.getName());
            if (customTags.isPresent())
                meta.setCustomTags(customTags.get());
            meta.setRaid(String.valueOf(getRedundancyLevel().getCode()).trim());

            drive.saveObjectMetadata(meta);

        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
        }
    }

}
