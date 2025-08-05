/*
 * Odilon Object Storage
 * (c) kbee 
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
package io.odilon.service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.encryption.EncryptionService;
import io.odilon.error.OdilonInternalErrorException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.model.SystemInfo;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.monitor.SystemInfoService;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Implementation of the Object Storage interface
 * </p>
 * <p>
 * The Object Storage is essentially an intermediary that downloads the
 * requirements into the Virtual File System
 * </p>
 * 
 * Checks VFS is in state enabled WORM or Read Only
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class OdilonObjectStorageService extends BaseService implements ObjectStorageService, ApplicationContextAware {

    static private Logger startuplogger = Logger.getLogger("StartupLogger");
    static private Logger logger = Logger.getLogger(OdilonObjectStorageService.class.getName());

    @JsonIgnore
    @Autowired
    private ServerSettings serverSettings;

    @JsonIgnore
    @Autowired
    private EncryptionService encrpytionService;

    @JsonIgnore
    @Autowired
    private VirtualFileSystemService virtualFileSystemService;

    @JsonIgnore
    private ApplicationContext applicationContext;

    @JsonIgnore
    SystemInfoService systemInfoService;

    @JsonProperty("port")
    private String port;

    @JsonProperty("accessKey")
    private String accessKey;

    @JsonProperty("secretKey")
    private String secretKey;

    @JsonIgnore
    private OdilonServerInfo odilonServer;

    /**
     * Services provided by Spring
     * 
     * @param serverSettings
     * @param montoringService
     * @param encrpytionService
     * @param vfs
     */
    public OdilonObjectStorageService(ServerSettings serverSettings, EncryptionService encrpytionService,
            VirtualFileSystemService vfs, SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
        this.serverSettings = serverSettings;
        this.encrpytionService = encrpytionService;
        this.virtualFileSystemService = vfs;
    }

    @Override
    public void wipeAllPreviousVersions() {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        getVirtualFileSystemService().wipeAllPreviousVersions();
    }

    @Override
    public void deleteBucketAllPreviousVersions(String bucketName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        getVirtualFileSystemService().deleteBucketAllPreviousVersions(bucketName);
    }

    @Override
    public List<ObjectMetadata> getObjectMetadataAllPreviousVersions(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().getObjectMetadataAllVersions(bucketName, objectName);
    }

    @Override
    public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName, int version) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().getObjectMetadataVersion(bucketName, objectName, version);
    }

    @Override
    public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().getObjectMetadataPreviousVersion(bucketName, objectName);
    }

    @Override
    public InputStream getObjectPreviousVersionStream(String bucketName, String objectName, int version) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().getObjectVersion(bucketName, objectName, version);
    }

    @Override
    public ObjectMetadata restorePreviousVersion(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().restorePreviousVersion(bucketName, objectName);
    }

    @Override
    public void deleteObjectAllPreviousVersions(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        getVirtualFileSystemService().deleteObjectAllPreviousVersions(bucketName, objectName);
    }

    @Override
    public boolean hasVersions(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().hasVersions(bucketName, objectName);
    }

    @Override
    public boolean existsObject(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().existsObject(bucketName, objectName);
    }

    @Override
    public boolean isEmptyBucket(String bucketName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().isEmptyBucket(bucketName);
    }

    @Override
    public void deleteObject(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        if (getServerSettings().isReadOnly() || getServerSettings().isWORM())
            throw new IllegalStateException(dataStorageModeMsg(bucketName, objectName));
        getVirtualFileSystemService().deleteObject(getVirtualFileSystemService().getBucketByName(bucketName), objectName);
    }

    @Override
    public DataList<Item<ObjectMetadata>> listObjects(String bucketName, Optional<Long> offset, Optional<Integer> pageSize,
            Optional<String> prefix, Optional<String> serverAgentId) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().listObjects(bucketName, offset, pageSize, prefix, serverAgentId);
    }

    @Override
    public void putObject(String bucketName, String objectName, File file) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
        Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
        Check.requireNonNullArgument(file, "file is null | b: " + bucketName + " o: " + objectName);

        if (getServerSettings().isReadOnly())
            throw new IllegalStateException(dataStorageModeMsg(bucketName, objectName));

        if (getServerSettings().isWORM()) {
            if (existsObject(bucketName, objectName))
                throw new IllegalStateException(dataStorageModeMsg(bucketName, objectName));
        }

        Path filePath = file.toPath();

        if (!Files.isRegularFile(filePath))
            throw new IllegalArgumentException("'" + file.getName() + "': not a regular file");
        String contentType = null;
        FileInputStream fis = null;

        try {
            contentType = Files.probeContentType(filePath);
            fis = new FileInputStream(file);
        } catch (IOException e) {
            throw new OdilonInternalErrorException(e);
        }
        putObject(bucketName, objectName, new BufferedInputStream(fis), file.getName(), contentType);
    }

    @Override
    public void putObject(String bucketName, String objectName, InputStream stream, String fileName, String contentType) {
        putObject(bucketName, objectName, stream, fileName, contentType, Optional.empty());
    }

    @Override
    public void putObject(String bucketName, String objectName, InputStream is, String fileName, String contentType,
            Optional<List<String>> customTags) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
        Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
        Check.requireNonNullStringArgument(fileName, "file is null | b: " + bucketName + " o:" + objectName);
        Check.requireNonNullArgument(is, "InpuStream can not null -> b:" + bucketName + " | o:" + objectName);

        if (getServerSettings().isReadOnly())
            throw new IllegalStateException(dataStorageModeMsg(bucketName, objectName));

        if (getServerSettings().isWORM()) {
            if (existsObject(bucketName, objectName))
                throw new IllegalStateException(dataStorageModeMsg(bucketName, objectName));
        }

        if (objectName.length() < 1 || objectName.length() > SharedConstant.MAX_OBJECT_CHARS)
            throw new IllegalArgumentException("objectName must be >0 and <" + String.valueOf(SharedConstant.MAX_OBJECT_CHARS)
                    + ", and Name must match the java regex ->  " + SharedConstant.object_valid_regex + " | o:" + objectName);

        if (!objectName.matches(SharedConstant.object_valid_regex))
            throw new IllegalArgumentException("objectName must be >0 and <" + String.valueOf(SharedConstant.MAX_OBJECT_CHARS)
                    + ", and Name must match the java regex ->  " + SharedConstant.object_valid_regex + " | o:" + objectName);
        try {
            getVirtualFileSystemService().putObject(bucketName, objectName, is, fileName, contentType, customTags);
        } catch (Exception e) {
            throw new OdilonInternalErrorException(e);
        }
    }

    @Override
    public InputStream getObjectStream(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        InputStream is = null;
        try {
            is = getVirtualFileSystemService().getObjectStream(bucketName, objectName);
            return is;
        } catch (Exception e1) {
            logger.error(e1, SharedConstant.NOT_THROWN);
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                }
            }
            throw new InternalCriticalException(e1);
        }
    }

    @Override
    public VirtualFileSystemObject getObject(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().getObject(bucketName, objectName);
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String objectName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().getObjectMetadata(getVirtualFileSystemService().getBucketByName(bucketName),
                objectName);
    }

    @Override
    public boolean existsBucket(String bucketName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().existsBucket(bucketName);
    }

    @Override
    public ServerBucket findBucketName(String bucketName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().getBucketByName(bucketName);
    }

    @Override
    public ServerBucket createBucket(String bucketName) {

        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");

        if (getServerSettings().isReadOnly())
            throw new IllegalStateException(dataStorageModeMsg(bucketName));

        if (getServerSettings().isWORM()) {
            if (existsBucket(bucketName))
                throw new IllegalStateException(dataStorageModeMsg(bucketName));
        }

        if (bucketName.length() < 1 || bucketName.length() > SharedConstant.MAX_BUCKET_CHARS)
            throw new IllegalArgumentException("bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS)
                    + " and must contain just lowercase letters and numbers, java regex = '" + SharedConstant.bucket_valid_regex
                    + "' | b:" + bucketName);

        if (!bucketName.matches(SharedConstant.bucket_valid_regex))
            throw new IllegalArgumentException("bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS)
                    + " and must contain just lowercase letters and numbers, java regex = '" + SharedConstant.bucket_valid_regex
                    + "' | b:" + bucketName);
        try {
            return getVirtualFileSystemService().createBucket(bucketName);

        } catch (Exception e) {
            throw (new OdilonInternalErrorException(e));
        }
    }

    @Override
    public ServerBucket updateBucketName(ServerBucket bucket, String newBucketName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        Check.requireNonNullArgument(bucket, "bucket can not be null or empty");
        Check.requireNonNullStringArgument(newBucketName, "newbucketName can not be null or empty");

        if (getServerSettings().isReadOnly())
            throw new IllegalStateException(dataStorageModeMsg(bucket.getName()));
        if (getServerSettings().isWORM())
            throw new IllegalStateException(dataStorageModeMsg(bucket.getName()));

        if (newBucketName.length() < 1 || newBucketName.length() > SharedConstant.MAX_BUCKET_CHARS)
            throw new IllegalArgumentException("bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS)
                    + " and must contain just lowercase letters and numbers, java regex = '" + SharedConstant.bucket_valid_regex
                    + "' | b:" + newBucketName);

        if (!newBucketName.matches(SharedConstant.bucket_valid_regex))
            throw new IllegalArgumentException("bucketName must be >0 and <" + String.valueOf(SharedConstant.MAX_BUCKET_CHARS)
                    + " and must contain just lowercase letters and numbers, java regex = '" + SharedConstant.bucket_valid_regex
                    + "' | b:" + newBucketName);

        if (this.existsBucket(newBucketName))
            throw new IllegalArgumentException("bucketName already used " + newBucketName);

        try {
            return getVirtualFileSystemService().renameBucketName(bucket.getName(), newBucketName);
        } catch (Exception e) {
            throw (new OdilonInternalErrorException(e));
        }
    }

    /**
     * delete all DriveBuckets
     */
    public void deleteBucketByName(String bucketName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        if (getServerSettings().isReadOnly())
            throw new IllegalStateException(dataStorageModeMsg(bucketName));

        if (getServerSettings().isWORM()) {
            if (existsBucket(bucketName))
                throw new IllegalStateException(dataStorageModeMsg(bucketName));
            else
                return;
        }
        getVirtualFileSystemService().removeBucket(bucketName);
    }

    @Override
    public void forceDeleteBucket(String bucketName) {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        if (getServerSettings().isReadOnly())
            throw new IllegalStateException(dataStorageModeMsg(bucketName));
        if (getServerSettings().isWORM()) {
            if (existsBucket(bucketName))
                throw new IllegalStateException(dataStorageModeMsg(bucketName));
            else
                return;
        }
        getVirtualFileSystemService().forceRemoveBucket(bucketName);
    }

    @Override
    public List<ServerBucket> findAllBuckets() {
        Check.requireTrue(isVirtualFileSystemServiceEnabled(), invalidStateMsg());
        return getVirtualFileSystemService().listAllBuckets();
    }

    @Override
    public String ping() {
        if (!isVirtualFileSystemServiceEnabled())
            return (VirtualFileSystemService.class.getSimpleName() + " not enabled -> "
                    + getVirtualFileSystemService().getStatus().toString());
        return getVirtualFileSystemService().ping();
    }

    @Override
    public SystemInfo getSystemInfo() {
        return this.systemInfoService.getSystemInfo();
    }

    @Override
    public ServerSettings getServerSettings() {
        return serverSettings;
    }

    @Override
    public EncryptionService getEncryptionService() {
        return encrpytionService;
    }

    @Override
    public boolean isEncrypt() {
        return getServerSettings().isEncryptionEnabled();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public VirtualFileSystemService getVirtualFileSystemService() {
        return virtualFileSystemService;
    }

    @PostConstruct
    protected void onInitialize() {
        synchronized (this) {
            try {
                setStatus(ServiceStatus.STARTING);
                this.port = String.valueOf(getServerSettings().getPort());
                this.accessKey = getServerSettings().getAccessKey();
                this.secretKey = getServerSettings().getSecretKey();
                startuplogger.debug("Started -> " + ObjectStorageService.class.getSimpleName());
                setStatus(ServiceStatus.RUNNING);
            } catch (Exception e) {
                setStatus(ServiceStatus.STOPPED);
                throw (e);
            }
        }
    }

    private boolean isVirtualFileSystemServiceEnabled() {
        return (getVirtualFileSystemService().getStatus() == ServiceStatus.RUNNING);
    }

    private String invalidStateMsg() {
        return VirtualFileSystemService.class.getSimpleName() + " invalid state -> "
                + getVirtualFileSystemService().getStatus().toString();
    }

    private String dataStorageModeMsg(String bucketName) {
        return "Illegal operation for Data Storage Mode -> " + getServerSettings().getDataStorage().getName() + " | b: "
                + bucketName;
    }

    private String dataStorageModeMsg(String bucketName, String objectName) {
        return "Illegal operation for Data Storage Mode -> " + getServerSettings().getDataStorage().getName() + " | b: "
                + bucketName + " o: " + objectName;
    }
}
