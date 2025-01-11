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

package io.odilon.virtualFileSystem.model;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;

import io.odilon.cache.FileCacheService;
import io.odilon.cache.ObjectMetadataCacheService;
import io.odilon.encryption.EncryptionService;
import io.odilon.encryption.MasterKeyService;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.query.BucketIteratorService;
import io.odilon.replication.ReplicationService;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;
import io.odilon.virtualFileSystem.BucketCache;

/**
 * <p>
 * The virtual file system layer manages the Objects repository on top of the OS
 * File System. <br/>
 * It implements
 * <a href="https://en.wikipedia.org/wiki/Standard_RAID_levels">software
 * RAID</a>, which depending on the configuration can be RAID 0, RAID 1,
 * <a href="https://en.wikipedia.org/wiki/Erasure_code">RAID 6/Erasure
 * Coding</a>. <br/>
 * Odilon uses <a href=
 * "https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction"> Reed
 * Solomon encoding</a> for
 * <a href="https://en.wikipedia.org/wiki/Erasure_code">Erasure Codes</a>.
 * </p>
 * 
 * <p>
 * Implementations of this interface are expected to be thread-safe, and can be
 * safely accessed by multiple concurrent threads.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public interface VirtualFileSystemService extends SystemService {

    public static final int BUFFER_SIZE = 8192;

    /**
     * sys / buckets sys / config sys / tmp
     */
    public static final String DATA = "data";
    public static final String METADATA = "metadata";

    public static final String DRIVE_INFO = "driveInfo.json";
    public static final String SERVER_METADATA_FILE = "odilon.json";
    public static final String ENCRYPTION_KEY_FILE = "key.enc";

    public static final String SYS = ".odilon.sys";
    public static final String BUCKETS = "buckets";
    public static final String WORK = "work";

    public static final String CACHE = "cache";

    public static final String SCHEDULER = "scheduler";
    public static final String JOURNAL = "journal";
    public static final String TEMP = "tmp";
    public static final String VERSION_DIR = "version";

    static final public String VERSION_EXTENSION = ".v";

    static final public int BITS_PER_BYTE = 8;

    /**
     * Create RAID driver
     */
    public IODriver createVFSIODriver();

    /**
     * Odilon Server info
     */
    public OdilonServerInfo getOdilonServerInfo();

    public void setOdilonServerInfo(OdilonServerInfo serverInfo);

    /**
     * Drives and VFS Buckets
     */
    public Map<String, Drive> getMapDrivesAll();

    public Map<String, Drive> getMapDrivesEnabled();

    public Map<Integer, Drive> getMapDrivesRSDecode();

    /** used to add a new disk enabled after a Drive sync process */
    public void updateDriveStatus(Drive drive);

    public List<ServerBucket> listAllBuckets();

    /**
     * Bucket
     */
    public ServerBucket createBucket(String bucketName) throws IOException;

    public void removeBucket(String bucketName);

    public ServerBucket renameBucketName(String currentBucketName, String newBucketName);

    public ServerBucket getBucketByName(String bucketName);

    // public ServerBucket getBucketById(Long bucketId);

    public boolean existsBucket(String bucketName);

    public boolean isEmpty(ServerBucket bucket);

    public void forceRemoveBucket(String bucketName);

    public Long getNextBucketId();

    public String ping();

    /**
     * Objects
     */
    public void putObject(ServerBucket bucket, String objectName, File file);

    public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType);

    public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType,
            Optional<List<String>> customTags);

    public void putObject(String bucketName, String objectName, InputStream is, String fileName, String contentType,
            Optional<List<String>> customTags);

    public VirtualFileSystemObject getObject(ServerBucket bucket, String objectName);

    public VirtualFileSystemObject getObject(String bucketName, String objectName);

    public ObjectMetadata getObjectMetadata(ServerBucket bucket, String objectName);

    public boolean existsObject(ServerBucket bucket, String objectName);

    public boolean existsObject(String bucketName, String objectName);

    // public boolean existsObject(Long bucketId, String objectName);

    public void deleteObject(ServerBucket bucket, String objectName);

    // public void deleteObject(String bucketName, String objectName);

    public InputStream getObjectStream(ServerBucket bucket, String objectName) throws IOException;

    public InputStream getObjectStream(String bucketName, String objectName) throws IOException;

    /**
     * Object Version
     */
    public boolean hasVersions(String bucketName, String objectName);

    public List<ObjectMetadata> getObjectMetadataAllVersions(String bucketName, String objectName);

    public ObjectMetadata getObjectMetadataVersion(String bucketName, String objectName, int version);

    public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName);

    public InputStream getObjectVersion(String bucketName, String ObjectName, int version);

    public ObjectMetadata restorePreviousVersion(String bucketName, String objectName);

    //public void deleteObjectAllPreviousVersions(ObjectMetadata meta);
    public void deleteObjectAllPreviousVersions(String bucketName, String objectName);
    
    public void deleteBucketAllPreviousVersions(String bucketName);

    public void wipeAllPreviousVersions();

    /**
     * Query
     */
    public DataList<Item<ObjectMetadata>> listObjects(String bucketName, Optional<Long> offset, Optional<Integer> pageSize,
            Optional<String> prefix, Optional<String> serverAgentId);

    public DataList<Item<ObjectMetadata>> listObjects(String bucketName);

    /**
     * Journal
     */
    public void saveJournal(VirtualFileSystemOperation op);

    public void removeJournal(String id);

    public List<VirtualFileSystemOperation> getJournalPendingOperations();

    /**
     * Scheduler
     */
    public void saveScheduler(ServiceRequest request, String queueId);

    public void removeScheduler(ServiceRequest request, String queueId);

    public List<ServiceRequest> getSchedulerPendingRequests(String queueId);

    /**
     * Status Info
     */
    public boolean isEncrypt();

    public boolean isUseVaultNewFiles();

    public OffsetDateTime getStarted();

    public EncryptionService getEncryptionService();

    public JournalService getJournalService();

    public SchedulerService getSchedulerService();

    public ServerSettings getServerSettings();

    public RedundancyLevel getRedundancyLevel();

    public boolean isEmptyBucket(String bucketName);

    public BucketIteratorService getBucketIteratorService();

    public boolean checkIntegrity(String bucketName, String objectName, boolean forceCheck);

    /**
     * Query
     */

    public ReplicationService getReplicationService();

    public ObjectMetadataCacheService getObjectMetadataCacheService();

    public FileCacheService getFileCacheService();

    public SystemMonitorService getSystemMonitorService();

    public LockService getLockService();

    /**
     * Security
     **/
    public MasterKeyService getMasterKeyEncryptorService();

    public byte[] HMAC(byte[] data, byte[] key) throws NoSuchAlgorithmException, InvalidKeyException;

    public ApplicationContext getApplicationContext();

    public BucketCache getBucketCache();


    public ApplicationEventPublisher getApplicationEventPublisher();

    public ExecutorService getExecutorService();


}
