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

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import io.odilon.OdilonVersion;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.SharedConstant;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
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
public class RAIDSixCreateObjectHandler extends RAIDSixTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDSixCreateObjectHandler.class.getName());

    /**
     * <p>
     * Instances of this class are used internally by {@link RAIDSixDriver}
     * <p>
     * 
     * @param driver
     */
    protected RAIDSixCreateObjectHandler(RAIDSixDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    
    protected void create(InputStream stream, String srcFileName, String contentType, Optional<List<String>> customTags) {

        VirtualFileSystemOperation operation = null;
        boolean commitOK = false;
        boolean isMainException = false;

        objectWriteLock();
        try {

            bucketReadLock();
            try (stream) {

                checkExistsBucket();
                checkNotExistObject();

                /** start operation */
                operation = createObject();

                /** save data and metadata */
                RAIDSixBlocks blocks = saveData(stream);
                saveMetadata(blocks, srcFileName, contentType, customTags);

                /** commit */
                commitOK = operation.commit();

            } catch (InternalCriticalException e) {
                isMainException = true;
                throw e;
            } catch (Exception e) {
                isMainException = true;
                throw new InternalCriticalException(e, info(srcFileName));
            } finally {
                try {
                    if (!commitOK) {
                        try {
                            rollback(operation);
                        } catch (Exception e) {
                            if (isMainException)
                                logger.error(info(srcFileName), SharedConstant.NOT_THROWN);
                            else
                                throw new InternalCriticalException(e, info());
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

    private VirtualFileSystemOperation createObject() {
        return createObject(getBucket(), getObjectName());
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     */
    private RAIDSixBlocks saveData(InputStream stream) {
        try (InputStream sourceStream = isEncrypt() ? (getVirtualFileSystemService().getEncryptionService().encryptStream(stream))
                : stream) {
            return (new RAIDSixEncoder(getDriver())).encodeHead(sourceStream, getBucket(), getObjectName());
        } catch (Exception e) {
            throw new InternalCriticalException(e, info());
        }
    }

    /**
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     */
    private void saveMetadata(RAIDSixBlocks ei, String srcFileName, String contentType, Optional<List<String>> customTags) {

        List<String> shaBlocks = new ArrayList<String>();
        StringBuilder etag_b = new StringBuilder();

        ei.getEncodedBlocks().forEach(item -> {
            try {
                shaBlocks.add(OdilonFileUtils.calculateSHA256String(item));
            } catch (Exception e) {
                throw new InternalCriticalException(e,info());
            }
        });

        shaBlocks.forEach(item -> etag_b.append(item));

        String etag = null;

        try {
            etag = OdilonFileUtils.calculateSHA256String(etag_b.toString());
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new InternalCriticalException(e, info(srcFileName));
        }

        OffsetDateTime creationDate = OffsetDateTime.now();

        final List<Drive> drives = getDriver().getDrivesAll();
        final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

        for (Drive drive : getDriver().getDrivesAll()) {
            try {
                ObjectMetadata meta = new ObjectMetadata(getBucket().getId(), getObjectName());
                meta.setFileName(srcFileName);
                meta.setAppVersion(OdilonVersion.VERSION);
                meta.setContentType(contentType);
                meta.setCreationDate(creationDate);
                meta.setVersion(VERSION_ZERO);
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
                throw new InternalCriticalException(e, info(srcFileName));
            }
        }
        /** save in parallel */
        saveRAIDSixObjectMetadataToDisk(drives, list, true);
    }
}
