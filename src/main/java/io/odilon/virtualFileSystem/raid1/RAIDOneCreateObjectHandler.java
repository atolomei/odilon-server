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
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 1 Handler <br/>
 * Creates new Objects ({@link OperationCode.CREATE_OBJECT})
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneCreateObjectHandler extends RAIDOneHandler {

    private static Logger logger = Logger.getLogger(RAIDOneCreateObjectHandler.class.getName());

    /**
     * <p>
     * Instances of this class are used internally by {@link RAIDOneDriver}
     * <p>
     * 
     * @param driver
     */
    protected RAIDOneCreateObjectHandler(RAIDOneDriver driver) {
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
        Long bucket_id = bucket.getId();
        Check.requireNonNullArgument(bucket_id, "bucket_id is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket_id.toString());

        Check.requireNonNullArgument(stream, "stream is null");

        VirtualFileSystemOperation operation = null;
        boolean done = false;
        boolean isMainException = false;

        objectWriteLock(bucket, objectName);
        try {
            bucketReadLock(bucket);
            try (stream) {
            
                /** must be executed inside the critical zone. */
                checkExistsBucket(bucket);

                /** must be executed inside the critical zone. */
                checkNotExistObject(bucket, objectName);
                
                //if (getDriver().getReadDrive(bucket, objectName).existsObjectMetadata(bucket, objectName))
                //    throw new IllegalArgumentException("object already exist -> b:" + getDriver().objectInfo(bucket, objectName));

                int version = 0;
                operation = createObject(bucket, objectName);
                saveObjectDataFile(bucket, objectName, stream, srcFileName);
                saveObjectMetadata(bucket, objectName, srcFileName, contentType, version, customTags);
                done = operation.commit();
                
            } catch (Exception e) {
                done = false;
                isMainException = true;
                throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName, srcFileName));
            } finally {
                try {
                    if (!done) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName, srcFileName));
                            else
                                logger.error(e, getDriver().objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
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
     * 
     * <p>
     * The only operation that should be called here by {@link RAIDSixDriver} is
     * {@link OperationCode.CREATE_OBJECT}
     * </p>
     * 
     */
    @Override
    protected void rollbackJournal(VirtualFileSystemOperation operation, boolean recoveryMode) {

        Check.requireNonNullArgument(operation, "operation is null");
        Check.checkTrue(operation.getOperationCode() == OperationCode.CREATE_OBJECT, "Invalid op ->  " + operation.getOperationCode().getName());

        String objectName = operation.getObjectName();
        Long bucket_id = operation.getBucketId();

        ServerBucket bucket = getBucketCache().get(operation.getBucketId());

        Check.requireNonNullArgument(bucket_id, "bucket_id is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket_id.toString());

        boolean done = false;

        try {
            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(operation);

            for (Drive drive : getDriver().getDrivesAll()) {
                drive.deleteObjectMetadata(bucket, objectName);
                ObjectPath path = new ObjectPath(drive, bucket_id, objectName);
                FileUtils.deleteQuietly(path.dataFilePath().toFile());
            }
            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(e, getDriver().opInfo(operation), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, getDriver().opInfo(operation));
            else
                logger.error(e, getDriver().opInfo(operation), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode)
                operation.cancel();
        }
    }

    /**
     * <p>
     * This method is not ThreadSafe. Locks must be applied by the caller
     * </p>
     * 
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     */
    private void saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream, String srcFileName) {

        int total_drives = getDriver().getDrivesAll().size();

        byte[] buf = new byte[ServerConstant.BUFFER_SIZE];

        BufferedOutputStream out[] = new BufferedOutputStream[total_drives];
        boolean isMainException = false;
        try (InputStream sourceStream = isEncrypt() ? (getVirtualFileSystemService().getEncryptionService().encryptStream(stream))
                : stream) {
            int n_d = 0;
            for (Drive drive : getDriver().getDrivesAll()) {

                ObjectPath path = new ObjectPath(drive, bucket.getId(), objectName);
                String sPath = path.dataFilePath().toString();
                out[n_d++] = new BufferedOutputStream(new FileOutputStream(sPath), ServerConstant.BUFFER_SIZE);
            }

            int bytes_read = 0;
            
            /** if there is just 1 disk copy directly */
            if (getDriver().getDrivesAll().size() < 2) {
                while ((bytes_read = sourceStream.read(buf, 0, buf.length)) >= 0) {
                    for (int bytes = 0; bytes < total_drives; bytes++) {
                        out[bytes].write(buf, 0, bytes_read);
                    }
                }
            } else {
                /** for 2 or more disks copy in parallel */
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
                        List<Future<Boolean>> future = executor.invokeAll(tasks, 10, TimeUnit.MINUTES);
                        Iterator<Future<Boolean>> it = future.iterator();
                        while (it.hasNext()) {
                            if (!it.next().get())
                                throw new InternalCriticalException(objectInfo(bucket, objectName, srcFileName));
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new InternalCriticalException(e);
                    }

                }
            } // else

        } catch (InternalCriticalException e) {
            isMainException = true;
            throw e;
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
            if (!isMainException && (secEx != null))
                throw new InternalCriticalException(secEx);
        }
    }

    /**
     * <p>
     * This method is not ThreadSafe. Locks must be applied by the caller
     * </p>
     * <p>
     * Note that metadata is saved in parallel to all available disks
     * </p>
     * 
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     */
    private void saveObjectMetadata(ServerBucket bucket, String objectName, String srcFileName, String contentType, int version,
            Optional<List<String>> customTags) {

        String sha = null;
        String baseDrive = null;

        OffsetDateTime now = OffsetDateTime.now();

        final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

        for (Drive drive : getDriver().getDrivesAll()) {
            ObjectPath path = new ObjectPath(drive, bucket, objectName);
            File file = path.dataFilePath().toFile();
            try {
                String sha256 = OdilonFileUtils.calculateSHA256String(file);
                if (sha == null) {
                    sha = sha256;
                    baseDrive = drive.getName();
                } else {
                    if (!sha256.equals(sha))
                        throw new InternalCriticalException("SHA 256 are not equal for -> d:" + baseDrive + " ->" + sha + " vs d:"
                                + drive.getName() + " -> " + sha256);
                }
                ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
                meta.setFileName(srcFileName);
                meta.setAppVersion(OdilonVersion.VERSION);
                meta.setContentType(contentType);
                meta.setCreationDate(now);
                meta.setVersion(version);
                meta.setVersioncreationDate(now);
                meta.setLength(file.length());
                meta.setEtag(sha256);
                meta.setEncrypt(getVirtualFileSystemService().isEncrypt());
                meta.setSha256(sha256);
                meta.setIntegrityCheck(now);
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
        saveRAIDOneObjectMetadataToDisk(getDriver().getDrivesAll(), list, true);
    }

    

}
