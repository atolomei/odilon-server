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
package io.odilon.virtualFileSystem;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.OdilonVersion;
import io.odilon.encryption.EncryptionService;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AbstractServiceRequest;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.service.util.ByteToString;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveBucket;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VFSOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Base class for VirtualFileSystemDrivers ({@link IODriver}): <br/>
 * RAID 0: {@link RAIDZeroDriver}, <br/>
 * RAID 1: {@link RAIDOneDriver}, <br/>
 * RAID 6: {@link RAIDSixDriver} <br/>
 * </p>
 *
 * @see {@link RAIDZeroDriver} {@link RAIDOneDriver} {@link RAIDSixDriver}
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class BaseIODriver implements IODriver, ApplicationContextAware {

    private static Logger logger = Logger.getLogger(BaseIODriver.class.getName());
    static private Logger std_logger = Logger.getLogger("StartupLogger");

    @JsonIgnore
    static final public int MAX_CACHE_SIZE = 4000000;

    @JsonIgnore
    static private ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jdk8Module());
    }

    @JsonIgnore
    private VirtualFileSystemService virtualFileSystem;

    @JsonIgnore
    private LockService lockService;

    @JsonIgnore
    private List<Drive> drivesEnabled;

    @JsonIgnore
    private List<Drive> drivesAll;

    @JsonIgnore
    private ApplicationContext applicationContext;

    /**
     * @param vfs
     * @param vfsLockService
     */
    public BaseIODriver(VirtualFileSystemService vfs, LockService vfsLockService) {
        this.virtualFileSystem = vfs;
        this.lockService = vfsLockService;
    }

    
    public void saveObjectMetadataToDisk(final List<Drive> drives, final List<ObjectMetadata> list,
            final boolean isHead) {

        Check.requireNonNullArgument(drives, "drives is null");
        Check.requireNonNullArgument(list, "list is null");
        Check.requireTrue(drives.size() == list.size(), "must have the same number of elements. " + " drives -> "
                + String.valueOf(drives.size()) + " - list -> " + String.valueOf(list.size()));

        final int size = drives.size();

        ExecutorService executor = Executors.newFixedThreadPool(size);
        List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(size);

        for (int index = 0; index < size; index++) {
            final int val = index;
            tasks.add(() -> {
                try {
                    ObjectMetadata meta = list.get(val);
                    if (isHead)
                        drives.get(val).saveObjectMetadata(meta);
                    else
                        drives.get(val).saveObjectMetadataVersion(meta);
                    return Boolean.valueOf(true);

                } catch (Exception e) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                    return Boolean.valueOf(false);
                } finally {

                }
            });
        }

        try {
            List<Future<Boolean>> future = executor.invokeAll(tasks, 5, TimeUnit.MINUTES);
            Iterator<Future<Boolean>> it = future.iterator();
            while (it.hasNext()) {
                if (!it.next().get())
                    throw new InternalCriticalException("could not copy " + ObjectMetadata.class.getSimpleName());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalCriticalException(e, "could not copy " + ObjectMetadata.class.getSimpleName());
        }
    }

    /**
     * <p>
     * Shared by RAID 0, RAID 1, RAID 6
     * </p>
     */
    @Override
    public ServerBucket createBucket(String bucketName) {

        Check.requireNonNullArgument(bucketName, "bucketName is null");

        BucketMetadata meta = new BucketMetadata(bucketName);

        meta.setStatus(BucketStatus.ENABLED);
        meta.setAppVersion(OdilonVersion.VERSION);
        meta.setId(getVirtualFileSystemService().getNextBucketId());

        ServerBucket bucket = new OdilonBucket(meta);

        boolean done = false;
        boolean isMainException = false;

        VFSOperation op = null;

        bucketWriteLock(bucket);

        try {

            if (getVirtualFileSystemService().existsBucket(bucketName))
                throw new IllegalArgumentException(
                        ServerBucket.class.getSimpleName() + " already exist | b: " + bucketName);

            op = getJournalService().createBucket(meta);

            OffsetDateTime now = OffsetDateTime.now();

            meta.setCreationDate(now);
            meta.setLastModified(now);

            for (Drive drive : getDrivesAll()) {
                try {
                    drive.createBucket(meta);
                } catch (Exception e) {
                    done = false;
                    isMainException = true;
                    throw new InternalCriticalException(e, objectInfo(drive));
                }
            }
            done = op.commit();
            return bucket;

        } finally {
            try {
                if (done) {
                    getVirtualFileSystemService().addBucketCache(bucket);
                } else
                    rollbackJournal(op);

            } catch (Exception e) {
                if (!isMainException)
                    throw new InternalCriticalException(e, objectInfo(meta));
                else
                    logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                bucketWriteUnLock(bucket);
            }
        }
    }

    /**
     * 
     */
    @Override
    public ServerBucket renameBucket(ServerBucket bucket, String newBucketName) {

        Check.requireNonNullArgument(bucket, ServerBucket.class.getSimpleName() + " is null");

        VFSOperation op = null;
        boolean done = false;
        BucketMetadata meta = null;

        OffsetDateTime now = OffsetDateTime.now();

        getLockService().getBucketLock(bucket).writeLock().lock();

        String oldName = bucket.getName();

        try {

            if (getVirtualFileSystemService().existsBucket(newBucketName))
                throw new IllegalArgumentException("bucketName already used -> " + newBucketName);

            op = getJournalService().updateBucket(bucket);

            backupBucketMetadata(bucket);

            meta = bucket.getBucketMetadata();
            meta.setLastModified(now);
            meta.setBucketName(newBucketName);

            for (Drive drive : getDrivesAll()) {
                try {
                    drive.updateBucket(meta);
                } catch (Exception e) {
                    done = false;
                    throw new InternalCriticalException(e, objectInfo(drive));
                }
            }

            done = op.commit();
            return bucket;
        } finally {
            try {
                if (done) {
                    getVirtualFileSystemService().updateBucketCache(oldName, new OdilonBucket(meta));
                } else {
                    if (op != null)
                        rollbackJournal(op);
                }

            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                getLockService().getBucketLock(bucket).writeLock().unlock();
            }
        }
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */

    @Override
    public void deleteBucket(ServerBucket bucket) {
        getVirtualFileSystemService().removeBucket(bucket);
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public boolean isEmpty(ServerBucket bucket) {

        Check.requireNonNullArgument(bucket, ServerBucket.class.getSimpleName() + " is null");
        Check.requireTrue(existsBucketInDrives(bucket.getId()),
                "bucket does not exist in all drives -> b: " + bucket.getName());

        try {
            getLockService().getBucketLock(bucket).readLock().lock();
            for (Drive drive : getDrivesEnabled()) {
                if (!drive.isEmpty(bucket))
                    return false;
            }
            return true;
        } catch (Exception e) {
            String msg = "b:" + (Optional.ofNullable(bucket).isPresent() ? (bucket.getName()) : "null");
            logger.error(e, msg);
            throw new InternalCriticalException(e, msg);

        } finally {
            getLockService().getBucketLock(bucket).readLock().unlock();
        }
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    public void rollbackJournal(VFSOperation op) {
        rollbackJournal(op, false);
    }

    /**
     * <p>
     * ObjectMetadata is copied to all drives as regular files. Shared by RAID 1 and
     * RAID 6
     * </p>
     */
    @Override
    public ObjectMetadata getObjectMetadataPreviousVersion(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, ServerBucket.class.getSimpleName() + " is null");
        Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());

        getLockService().getObjectLock(bucket, objectName).readLock().lock();

        try {
            getLockService().getBucketLock(bucket).readLock().lock();

            try {
                List<ObjectMetadata> list = getObjectMetadataVersionAll(bucket, objectName);
                if (list != null && !list.isEmpty())
                    return list.get(list.size() - 1);
                return null;
            } catch (Exception e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            } finally {
                getLockService().getBucketLock(bucket).readLock().unlock();
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).readLock().unlock();
        }

    }

    public abstract RedundancyLevel getRedundancyLevel();

    public ObjectMapper getObjectMapper() {
        return mapper;
    }

    public LockService getLockService() {
        return this.lockService;
    }

    public JournalService getJournalService() {
        return this.virtualFileSystem.getJournalService();
    }

    public SchedulerService getSchedulerService() {
        return this.virtualFileSystem.getSchedulerService();
    }

    public VirtualFileSystemService getVirtualFileSystemService() {
        return virtualFileSystem;
    }

    public void setVFS(VirtualFileSystemService vfs) {
        this.virtualFileSystem = vfs;
    }

    /**
     * 
     * Save metadata Save stream
     * 
     * @param folderName
     * @param objectName
     * @param file
     */
    @Override
    public void putObject(ServerBucket bucket, String objectName, File file) {

        Check.requireNonNullArgument(bucket, ServerBucket.class.getSimpleName() + " is null");
        Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
        Check.requireNonNullArgument(file, "file is null | b:" + bucket.getName());

        Path filePath = file.toPath();

        if (!Files.isRegularFile(filePath))
            throw new IllegalArgumentException("'" + file.getName() + "' -> not a regular file");

        String contentType = null;

        try {
            contentType = Files.probeContentType(filePath);
        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
        try {

            putObject(bucket, objectName, new BufferedInputStream(new FileInputStream(file)), file.getName(),
                    contentType, Optional.empty());

        } catch (FileNotFoundException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public synchronized List<ServiceRequest> getSchedulerPendingRequests(String queueId) {

        List<ServiceRequest> list = new ArrayList<ServiceRequest>();
        Map<String, File> useful = new HashMap<String, File>();
        Map<String, File> useless = new HashMap<String, File>();

        Map<Drive, Map<String, File>> allDriveFiles = new HashMap<Drive, Map<String, File>>();

        for (Drive drive : getDrivesEnabled()) {
            allDriveFiles.put(drive, new HashMap<String, File>());
            for (File file : drive.getSchedulerRequests(queueId)) {
                allDriveFiles.get(drive).put(file.getName(), file);
            }
        }

        final Drive referenceDrive = getDrivesEnabled().get(0);

        allDriveFiles.get(referenceDrive).forEach((k, file) -> {
            boolean isOk = true;
            for (Drive drive : getDrivesEnabled()) {
                if (!drive.equals(referenceDrive)) {
                    if (!allDriveFiles.get(drive).containsKey(k)) {
                        isOk = false;
                        break;
                    }
                }
            }
            if (isOk)
                useful.put(k, file);
            else
                useless.put(k, file);
        });

        useful.forEach((k, file) -> {
            try {
                AbstractServiceRequest request = getObjectMapper().readValue(file, AbstractServiceRequest.class);
                list.add((ServiceRequest) request);

            } catch (IOException e) {
                logger.debug(e, "f:" + (Optional.ofNullable(file).isPresent() ? (file.getName()) : "null"));
                try {
                    Files.delete(file.toPath());
                } catch (IOException e1) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                }
            }
        });

        getDrivesEnabled().forEach(drive -> {
            allDriveFiles.get(drive).forEach((k, file) -> {
                if (!useful.containsKey(k)) {
                    try {
                        Files.delete(file.toPath());
                    } catch (Exception e1) {
                        logger.error(e1, SharedConstant.NOT_THROWN);
                    }
                }
            });
        });
        return list;
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public List<VFSOperation> getJournalPending(JournalService journalService) {

        List<VFSOperation> list = new ArrayList<VFSOperation>();

        for (Drive drive : getDrivesEnabled()) {
            File dir = new File(drive.getJournalDirPath());
            if (!dir.exists())
                return list;
            if (!dir.isDirectory())
                return list;
            File[] files = dir.listFiles();
            for (File file : files) {
                if (!file.isDirectory()) {
                    Path pa = Paths.get(file.getAbsolutePath());
                    try {
                        String str = Files.readString(pa);
                        OdilonVFSperation op = getObjectMapper().readValue(str, OdilonVFSperation.class);
                        op.setJournalService(getJournalService());
                        if (!list.contains(op))
                            list.add(op);

                    } catch (IOException e) {
                        logger.debug(e, "f:" + (Optional.ofNullable(file).isPresent() ? (file.getName()) : "null"));
                        try {
                            Files.delete(file.toPath());
                        } catch (IOException e1) {
                            logger.error(e, SharedConstant.NOT_THROWN);
                        }
                    }
                }
            }
        }

        std_logger.debug("Total operations that will rollback -> " + String.valueOf(list.size()));
        return list;
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public OdilonServerInfo getServerInfo() {
        File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
        if (file == null || !file.exists())
            return null;
        try {
            getLockService().getServerLock().readLock().lock();
            return getObjectMapper().readValue(file, OdilonServerInfo.class);
        } catch (IOException e) {
            throw new InternalCriticalException(e);
        } finally {
            getLockService().getServerLock().readLock().unlock();
        }
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public void setServerInfo(OdilonServerInfo serverInfo) {
        Check.requireNonNullArgument(serverInfo, "serverInfo is null");
        if (getServerInfo() == null)
            saveNewServerInfo(serverInfo);
        else
            updateServerInfo(serverInfo);
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public byte[] getServerMasterKey() {

        getLockService().getServerLock().readLock().lock();

        try {

            File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);

            if (file == null || !file.exists())
                return null;

            byte[] bDataEnc = FileUtils.readFileToByteArray(file);

            String encryptionKey = getVirtualFileSystemService().getServerSettings().getEncryptionKey();
            String encryptionIV = getVirtualFileSystemService().getServerSettings().getEncryptionIV();

            if (encryptionKey == null || encryptionIV == null)
                throw new InternalCriticalException(" encryption Key or IV is null");

            byte[] b_encryptionKey = ByteToString.hexStringToByte(encryptionKey);
            byte[] b_encryptionKeyIV = ByteToString.hexStringToByte(encryptionKey + encryptionIV);

            byte[] b_hmacOriginal;

            try {
                b_hmacOriginal = getVirtualFileSystemService().HMAC(b_encryptionKeyIV, b_encryptionKey);
            } catch (InvalidKeyException | NoSuchAlgorithmException e) {
                throw new InternalCriticalException(e, "can not calculate HMAC for 'odilon.properties' encryption key");
            }

            /** HMAC(32) + Master Key (16) + IV(12) + Salt (64) */
            byte[] bdataDec = getVirtualFileSystemService().getMasterKeyEncryptorService().decryptKey(bDataEnc);

            byte[] b_hmacNew = new byte[EncryptionService.HMAC_SIZE];
            System.arraycopy(bdataDec, 0, b_hmacNew, 0, b_hmacNew.length);

            if (!Arrays.equals(b_hmacOriginal, b_hmacNew)) {
                logger.error(
                        "HMAC of 'encryption.key' in 'odilon.properties' does not match with HMAC in 'key.enc'  -> encryption.key="
                                + encryptionKey + encryptionIV);
                throw new InternalCriticalException(
                        "HMAC of 'encryption.key' in 'odilon.properties' does not match with HMAC in 'key.enc'  -> encryption.key="
                                + encryptionKey + encryptionIV);
            }

            /** HMAC is correct */
            byte[] key = new byte[EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
            System.arraycopy(bdataDec, b_hmacNew.length, key, 0, key.length);

            return key;

        } catch (InternalCriticalException e) {
            if ((e.getCause() != null) && (e.getCause() instanceof javax.crypto.BadPaddingException))
                logger.error("possible cause -> the value of 'encryption.key' in 'odilon.properties' is incorrect");
            throw e;

        } catch (IOException e) {
            throw new InternalCriticalException(e, "getServerMasterKey");

        } finally {
            getLockService().getServerLock().readLock().unlock();
        }
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */

    @Override
    public void saveServerMasterKey(byte[] key, byte[] hmac, byte[] iv, byte[] salt) {

        Check.requireNonNullArgument(key, "key is null");
        Check.requireNonNullArgument(salt, "salt is null");

        boolean done = false;
        boolean reqRestoreBackup = false;

        VFSOperation op = null;

        getLockService().getServerLock().writeLock().lock();

        try {
            /** backup */
            for (Drive drive : getDrivesAll()) {
                try {
                    // drive.putSysFile(ServerConstant.ODILON_SERVER_METADATA_FILE, jsonString);
                    // backup
                } catch (Exception e) {
                    // isError = true;
                    reqRestoreBackup = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }

            op = getJournalService().saveServerKey();

            reqRestoreBackup = true;

            Exception eThrow = null;

            byte[] data = new byte[hmac.length + iv.length + key.length + salt.length];

            /** HMAC(32) + Master Key (16) + IV(12) + Salt (64) */
            System.arraycopy(hmac, 0, data, 0, hmac.length);
            System.arraycopy(key, 0, data, hmac.length, key.length);
            System.arraycopy(iv, 0, data, (hmac.length + key.length), iv.length);
            System.arraycopy(salt, 0, data, (hmac.length + iv.length + key.length), salt.length);

            byte[] dataEnc = getVirtualFileSystemService().getMasterKeyEncryptorService().encryptKey(data, iv);

            /** save */
            for (Drive drive : getDrivesAll()) {
                try {
                    File file = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
                    FileUtils.writeByteArrayToFile(file, dataEnc);

                } catch (Exception e) {
                    eThrow = new InternalCriticalException(e, objectInfo(drive));
                    break;
                }
            }

            if (eThrow != null)
                throw eThrow;

            done = op.commit();

        } catch (InternalCriticalException e) {
            throw e;

        } catch (Exception e) {
            if (logger.isDebugEnabled())
                logger.error(e, SharedConstant.NOT_THROWN);
            throw new InternalCriticalException(e, "saveServerMasterKey");

        } finally {
            try {
                if (!done) {
                    if (!reqRestoreBackup)
                        op.cancel();
                    else
                        rollbackJournal(op);
                }

            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                getLockService().getServerLock().writeLock().unlock();
            }
        }

    }

    /**
     * 
     * 
     */
    @Override
    public synchronized List<Drive> getDrivesEnabled() {

        if (this.drivesEnabled != null)
            return this.drivesEnabled;

        this.drivesEnabled = new ArrayList<Drive>();

        getVirtualFileSystemService().getMapDrivesEnabled().forEach((K, V) -> this.drivesEnabled.add(V));

        this.drivesEnabled.sort(new Comparator<Drive>() {
            @Override
            public int compare(Drive o1, Drive o2) {
                try {

                    if ((o1.getDriveInfo() == null))
                        if (o2.getDriveInfo() != null)
                            return 1;

                    if ((o2.getDriveInfo() == null))
                        if (o1.getDriveInfo() != null)
                            return -1;

                    if ((o1.getDriveInfo() == null) && o2.getDriveInfo() == null)
                        return 0;

                    if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
                        return -1;

                    if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
                        return 1;

                    return 0;
                } catch (Exception e) {
                    return 0;
                }
            }
        });

        return this.drivesEnabled;
    }

    /**
     * 
     */
    public synchronized List<Drive> getDrivesAll() {

        if (drivesAll != null)
            return drivesAll;

        this.drivesAll = new ArrayList<Drive>();

        getVirtualFileSystemService().getMapDrivesAll().forEach((K, V) -> drivesAll.add(V));

        this.drivesAll.sort(new Comparator<Drive>() {
            @Override
            public int compare(Drive o1, Drive o2) {
                try {
                    if ((o1.getDriveInfo() == null))
                        if (o2.getDriveInfo() != null)
                            return 1;

                    if ((o2.getDriveInfo() == null))
                        if (o1.getDriveInfo() != null)
                            return -1;

                    if ((o1.getDriveInfo() == null) && o2.getDriveInfo() == null)
                        return 0;

                    if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
                        return -1;

                    if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
                        return 1;

                    return 0;
                } catch (Exception e) {
                    return 0;
                }
            }
        });

        return this.drivesAll;
    }

    /**
     * 
     * @return
     */
    public boolean isEncrypt() {
        return getVirtualFileSystemService().isEncrypt();
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public void saveScheduler(ServiceRequest request, String queueId) {
        for (Drive drive : getDrivesEnabled()) {
            drive.saveScheduler(request, queueId);
        }
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public void removeScheduler(ServiceRequest request, String queueId) {
        for (Drive drive : getDrivesEnabled())
            drive.removeScheduler(request, queueId);
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public void saveJournal(VFSOperation op) {
        for (Drive drive : getDrivesEnabled())
            drive.saveJournal(op);
    }

    /**
     * <p>
     * Shared by RAID 1 and RAID 6
     * </p>
     */
    @Override
    public void removeJournal(String id) {
        for (Drive drive : getDrivesEnabled())
            drive.removeJournal(id);
    }

    protected abstract Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName);

    /**
     * <p>
     * Note that bucketName is not stored on disk, we must set the bucketName
     * explicitly. Disks identify Buckets by id, the name is stored in the
     * BucketMetadata file
     * </p>
     */
    public ObjectMetadata getObjectMetadataInternal(ServerBucket bucket, String objectName, boolean addToCacheIfmiss) {

        if ((!getVirtualFileSystemService().getServerSettings().isUseObjectCache())
                || (getVirtualFileSystemService().getObjectMetadataCacheService().size() >= MAX_CACHE_SIZE))
            return getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket.getId(), objectName);

        if (getVirtualFileSystemService().getObjectMetadataCacheService().containsKey(bucket.getId(), objectName)) {
            getVirtualFileSystemService().getSystemMonitorService().getCacheObjectHitCounter().inc();

            ObjectMetadata meta = getVirtualFileSystemService().getObjectMetadataCacheService().get(bucket.getId(),
                    objectName);
            meta.setBucketName(bucket.getName());
            return meta;
        }

        ObjectMetadata meta = getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket.getId(),
                objectName);
        meta.setBucketName(bucket.getName());

        getVirtualFileSystemService().getSystemMonitorService().getCacheObjectMissCounter().inc();

        if (addToCacheIfmiss) {
            getVirtualFileSystemService().getObjectMetadataCacheService().put(bucket.getId(), objectName, meta);
        }
        return meta;
    }

    /**
     * 
     */
    protected boolean existsBucketInDrives(Long bucketId) {

        for (Drive drive : getDrivesEnabled()) {
            if (!drive.existsBucket(bucketId)) {
                logger.error(("b: " + (Optional.of(bucketId).isPresent() ? bucketId.toString() : "null"))
                        + " -> not in d:" + drive.getName());
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * all drives have all buckets
     * </p>
     */
    protected Map<String, ServerBucket> getBucketsMap() {

        Map<String, ServerBucket> map = new HashMap<String, ServerBucket>();
        Map<String, Integer> control = new HashMap<String, Integer>();

        int totalDrives = getDrivesEnabled().size();

        for (Drive drive : getDrivesEnabled()) {
            for (DriveBucket bucket : drive.getBuckets()) {
                if (bucket.getStatus().isAccesible()) {
                    String name = bucket.getName();
                    Integer count;
                    if (control.containsKey(name))
                        count = control.get(name) + 1;
                    else
                        count = Integer.valueOf(1);
                    control.put(name, count);
                }
            }
        }

        /**
         * any drive is ok because all have all the buckets
         */

        Drive drive = getDrivesEnabled()
                .get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % getDrivesEnabled().size());
        for (DriveBucket bucket : drive.getBuckets()) {
            String name = bucket.getName();
            if (control.containsKey(name)) {
                Integer count = control.get(name);
                if (count == totalDrives) {
                    ServerBucket vfsbucket = new OdilonBucket(bucket);
                    map.put(vfsbucket.getName(), vfsbucket);
                }
            }
        }
        return map;
    }

    public String opInfo(VFSOperation op) {
        return "op:" + (op != null ? op.toString() : "null");
    }

    public String objectInfo(BucketMetadata bucket) {
        if (bucket == null)
            return "b: null";
        return "b_id:" + (bucket.getId() != null ? bucket.getId().toString() : "null") + " bn:"
                + (bucket.getBucketName() != null ? bucket.getBucketName() : "null");
    }

    public String objectInfo(Drive drive) {
        if (drive == null)
            return "d: null";
        return "d:" + drive.getName();
    }

    public String objectInfo(ServerBucket bucket) {
        if (bucket == null)
            return "b: null";
        return "b_id:" + (bucket.getId() != null ? bucket.getId().toString() : "null") + " bn:"
                + (bucket.getName() != null ? bucket.getName() : "null");
    }

    public String objectInfo(Long bucket_id, String objectName) {
        return "b_id:" + (bucket_id != null ? bucket_id.toString() : "null") + " o:"
                + (objectName != null ? objectName : "null");
    }

    public String objectInfo(String bucketName, String objectName) {
        return "bn:" + (bucketName != null ? bucketName : "null") + " o:" + (objectName != null ? objectName : "null");
    }

    public String objectInfo(ServerBucket bucket, String objectName) {
        if (bucket == null)
            return objectInfo("null", objectName);
        return objectInfo(bucket.getName(), objectName);
    }

    public String objectInfo(ServerBucket bucket, String objectName, String fileName) {
        if (bucket == null)
            return objectInfo("null", objectName, fileName);
        return objectInfo(bucket.getName(), objectName, fileName);
    }

    public String objectInfo(ObjectMetadata meta) {
        if (meta == null)
            return "om: null";

        if (meta.bucketName != null)
            return objectInfo(meta.bucketName, meta.objectName);
        else
            return objectInfo(meta.bucketId, meta.objectName);
    }

    public String objectInfo(String bucketName, String objectName, String fileName) {
        return "bn:" + (bucketName != null ? bucketName.toString() : "null") + " o:"
                + (objectName != null ? objectName : "null") + (fileName != null ? (" f:" + fileName) : "");
    }

    /**
     * @param serverInfo
     */
    private void saveNewServerInfo(OdilonServerInfo serverInfo) {

        boolean done = false;
        VFSOperation op = null;

        try {
            getLockService().getServerLock().writeLock().lock();
            op = getJournalService().createServerMetadata();
            String jsonString = getObjectMapper().writeValueAsString(serverInfo);

            for (Drive drive : getDrivesAll()) {
                try {
                    drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
                } catch (Exception e) {
                    done = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }
            done = op.commit();

        } catch (Exception e) {
            throw new InternalCriticalException(e, serverInfo.toString());

        } finally {

            try {
                if (!done) {
                    rollbackJournal(op);
                }
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                getLockService().getServerLock().writeLock().unlock();
            }
        }
    }

    protected void objectReadLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).readLock().lock();
    }

    protected void objectReadUnLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).readLock().unlock();
    }

    protected void objectWriteLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).writeLock().lock();
    }

    protected void objectWriteUnLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
    }

    protected void bucketReadLock(ServerBucket bucket) {
        getLockService().getBucketLock(bucket).readLock().lock();
    }

    protected void bucketReadUnLock(ServerBucket bucket) {
        getLockService().getBucketLock(bucket).readLock().unlock();
    }

    protected void bucketWriteLock(ServerBucket bucket) {
        getLockService().getBucketLock(bucket).writeLock().lock();
    }

    protected void bucketWriteUnLock(ServerBucket bucket) {
        getLockService().getBucketLock(bucket).writeLock().unlock();
    }

    protected void bucketWriteLock(BucketMetadata meta) {
        ServerBucket bucket = getVirtualFileSystemService().getBucketById(meta.getId());
        Check.requireNonNullArgument(bucket,
                ServerBucket.class.getSimpleName() + " does not exist for id -> " + meta.getId().toString());
        getLockService().getBucketLock(bucket).writeLock().lock();
    }

    protected void bucketWriteUnLock(BucketMetadata meta) {
        ServerBucket bucket = getVirtualFileSystemService().getBucketById(meta.getId());
        Check.requireNonNullArgument(bucket,
                ServerBucket.class.getSimpleName() + " does not exist for id -> " + meta.getId().toString());
        getLockService().getBucketLock(bucket).writeLock().unlock();
    }

    /**
     * @param bucket
     */
    protected void restoreBucketMetadata(ServerBucket bucket) {
        try {
            for (Drive drive : getDrivesAll()) {
                String path = drive.getBucketWorkDirPath(bucket.getId()) + File.separator + "bucketmetadata-"
                        + bucket.getId().toString() + ServerConstant.JSON;
                BucketMetadata meta = getObjectMapper().readValue(Paths.get(path).toFile(), BucketMetadata.class);
                drive.updateBucket(meta);
            }
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket));
        }
    }

    /**
     * @param bucket
     */
    protected void backupBucketMetadata(ServerBucket bucket) {
        try {
            for (Drive drive : getDrivesAll()) {
                BucketMetadata meta = drive.getBucketMetadata(bucket.getId());
                String path = drive.getBucketWorkDirPath(bucket.getId()) + File.separator + "bucketmetadata-"
                        + bucket.getId().toString() + ServerConstant.JSON;
                Files.writeString(Paths.get(path), getObjectMapper().writeValueAsString(meta));
            }
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket));
        }
    }

    private void updateServerInfo(OdilonServerInfo serverInfo) {

        boolean done = false;
        boolean mayReqRestoreBackup = false;
        VFSOperation op = null;

        getLockService().getServerLock().writeLock().lock();

        try {

            op = getJournalService().updateServerMetadata();
            String jsonString = getObjectMapper().writeValueAsString(serverInfo);

            for (Drive drive : getDrivesAll()) {
                try {
                    // drive.putSysFile(ServerConstant.ODILON_SERVER_METADATA_FILE, jsonString);
                    // backup
                } catch (Exception e) {
                    done = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }

            mayReqRestoreBackup = true;

            for (Drive drive : getDrivesAll()) {
                try {
                    drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
                } catch (Exception e) {
                    done = false;
                    throw new InternalCriticalException(e, "Drive -> " + drive.getName());
                }
            }
            done = op.commit();

        } catch (Exception e) {
            throw new InternalCriticalException(e, serverInfo.toString());

        } finally {
            try {
                if (!mayReqRestoreBackup) {
                    op.cancel();
                } else if (!done) {
                    rollbackJournal(op);
                }
            } catch (Exception e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            } finally {
                getLockService().getServerLock().writeLock().unlock();
            }
        }
    }

}
