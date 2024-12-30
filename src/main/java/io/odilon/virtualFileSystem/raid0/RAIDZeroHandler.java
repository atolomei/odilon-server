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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.OdilonVersion;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.ServerConstant;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * <p>
 * Base class for {@link RAIDZeroDriver} operations
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public abstract class RAIDZeroHandler extends BaseRAIDHandler implements RAIDHandler {

    @JsonIgnore
    private final RAIDZeroDriver driver;

    @Override
    protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
        return getDriver().getReadDrive(bucket, objectName);
    }

    public RAIDZeroHandler(RAIDZeroDriver driver) {
        this.driver = driver;
    }

    @Override
    public RAIDZeroDriver getDriver() {
        return this.driver;
    }

    public Drive getWriteDrive(ServerBucket bucket, String objectName) {
        return getDriver().getWriteDrive(bucket, objectName);
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
    protected void saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream, String srcFileName) {
        byte[] buf = new byte[ServerConstant.BUFFER_SIZE];
        try (InputStream sourceStream = isEncrypt() ? getEncryptionService().encryptStream(stream) : stream) {
            ObjectPath path = new ObjectPath(getWriteDrive(bucket, objectName), bucket, objectName);
            try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(path.dataFilePath().toFile()),
                    ServerConstant.BUFFER_SIZE)) {
                int bytesRead;
                while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0)
                    out.write(buf, 0, bytesRead);
            }
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
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
    protected void saveObjectMetadata(ServerBucket bucket, String objectName, String srcFileName, String contentType, int version,
            Optional<List<String>> customTags) {

        OffsetDateTime now = OffsetDateTime.now();
        Drive drive = getWriteDrive(bucket, objectName);
        ObjectPath path = new ObjectPath(drive, bucket, objectName);

        try {
            String sha256 = OdilonFileUtils.calculateSHA256String(path.dataFilePath().toFile());
            ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
            meta.setFileName(srcFileName);
            meta.setAppVersion(OdilonVersion.VERSION);
            meta.setContentType(contentType);
            meta.setEncrypt(isEncrypt());
            meta.setVault(isUseVaultNewFiles());
            meta.setCreationDate(now);
            meta.setLastModified(now);
            meta.setVersioncreationDate(now);
            meta.setVersion(version);
            meta.setLength(path.dataFilePath().toFile().length());
            meta.setEtag(sha256);
            meta.setIntegrityCheck(now);
            meta.setSha256(sha256);
            meta.setStatus(ObjectStatus.ENABLED);
            meta.setDrive(drive.getName());
            if (customTags.isPresent())
                meta.setCustomTags(customTags.get());
            meta.setRaid(String.valueOf(getRedundancyLevel().getCode()).trim());
            if (!path.metadataDirPath().toFile().exists())
                FileUtils.forceMkdir(path.metadataDirPath().toFile());
            Files.writeString(path.metadataFilePath(), getObjectMapper().writeValueAsString(meta));
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
        }
    }

    /**
     * must be executed inside the critical zone.
     */
    protected void checkNotExistObject(ServerBucket bucket, String objectName) {
        if (existsObjectMetadata(bucket, objectName))
            throw new IllegalArgumentException("Object already exist -> " + objectInfo(bucket, objectName));
    }

    /**
     * must be executed inside the critical zone.
     */
    protected void checkExistObject(ServerBucket bucket, String objectName) {
        if (!existsObjectMetadata(bucket, objectName))
            throw new OdilonObjectNotFoundException("Object does not exist -> " + objectInfo(bucket, objectName));
    }

    /**
     * This check must be executed inside the critical section
     */
    protected boolean existsObjectMetadata(ServerBucket bucket, String objectName) {
        if (existsCacheObject(bucket, objectName))
            return true;
        return getDriver().getWriteDrive(bucket, objectName).existsObjectMetadata(bucket, objectName);
    }

}
