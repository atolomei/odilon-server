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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseObject;
import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;
import io.odilon.model.ServerConstant;

import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveBucket;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;

/**
 * <p>
 * A <@link Drive} is a File System Directory.<br/>
 * There are multiple drives that the server uses to store objects. A drive can
 * only see regular files and only one level of folders, called {@link buckets}
 * </p>
 * <p>
 * Drives are <b>NOT Thread safe</b>, they do not apply concurrency control
 * mechanisms. It is up to the {@link IODriver} to ensure concurrency control.
 * </p>
 * Path -> String, the File may not exist
 * 
 * Dir -> File File -> File
 * 
 * [0-n][0.6] objectName.chunk.block.v[Version] objectName (there is one chunk
 * and one block) objectName
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public class OdilonDrive extends BaseObject implements Drive {

    static private Logger startuplogger = Logger.getLogger("StartupLogger");
    static private Logger logger = Logger.getLogger(OdilonDrive.class.getName());

    @JsonIgnore
    private ServiceStatus status = ServiceStatus.STOPPED;;

    @JsonIgnore
    private ReadWriteLock drive_lock = new ReentrantReadWriteLock();

    /**
     * <p>
     * {@link DriveBucket} may contain buckets marked as
     * {@code BucketStatus.DELETED}
     * </p>
     */
    @JsonIgnore
    private Map<Long, DriveBucket> driveBuckets = new ConcurrentHashMap<Long, DriveBucket>();

    @JsonProperty("name")
    private String name;

    @JsonProperty("rootDir")
    private String rootDir;

    @JsonProperty("configOrder")
    private int configOrder;

    @JsonProperty("driveInfo")
    DriveInfo driveInfo;

    @Autowired
    protected OdilonDrive(String rootDir) {
        this.name = rootDir;
        this.rootDir = rootDir;
    }

    /**
     * <p>
     * Constructor to call when creating a Dir with {@code new Drive}. <br/>
     * it calls method {@link onInitialize()}
     * </p>
     * 
     * @param name
     * @param rootDir
     */
    protected OdilonDrive(String name, String rootDir, int configOrder) {
        this.name = name;
        this.rootDir = rootDir;
        this.configOrder = configOrder;
        onInitialize();
    }

    @JsonIgnore
    @Override
    public BucketMetadata getBucketMetadata(ServerBucket bucket) throws IOException {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return getBucketMetadataById(bucket.getId());
    }

    /**
     * @param bucketId
     * @return
     */
    @JsonIgnore
    @Override
    public BucketMetadata getBucketMetadataById(Long bucketId) {
        Check.requireNonNullArgument(bucketId, "bucketId is null");
        try {
            return getObjectMapper().readValue(Paths.get(this.getBucketsDirPath() + File.separator + bucketId.toString()
                    + File.separator + bucketId.toString() + ServerConstant.JSON).toFile(), BucketMetadata.class);
        } catch (IOException e) {
            throw new InternalCriticalException(e, "b:" + bucketId.toString() + ", d:" + getName());
        }
    }

    @JsonIgnore
    @Override
    public File getObjectMetadataFile(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null " + objectInfo(bucket));
        return getObjectMetadataFileById(bucket.getId(), objectName);
    }

    @JsonIgnore
    @Override
    public String getBucketMetadataDirPath(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return getBucketMetadataDirPathById(bucket.getId());
    }

    @JsonIgnore
    @Override
    public String getObjectMetadataDirPath(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null " + objectInfo(bucket));
        return getObjectMetadataDirPathById(bucket.getId(), objectName);
    }

    @JsonIgnore
    @Override
    public DriveInfo getDriveInfo() {
        return this.driveInfo;
    }

    @Override
    public synchronized void setDriveInfo(DriveInfo info) {
        this.driveInfo = info;
        saveDriveMetadata(info);
    }

    @JsonIgnore
    @Override
    public String getBucketWorkDirPath(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return getBucketWorkDirPathById(bucket.getId());
    }

    @JsonIgnore
    @Override
    public String getBucketCacheDirPath(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return getBucketCacheDirPathById(bucket.getId());
    }

    private String getBucketCacheDirPathById(Long id) {
        return this.getCacheDirPath() + File.separator + id.toString();
    }

    @JsonIgnore
    @Override
    public String getBucketObjectDataDirPath(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return this.getRootDirPath() + File.separator + bucket.getId().toString();
    }

    @Override
    public void putObjectMetadataVersionFile(ServerBucket bucket, String objectName, int version, File metaFile)
            throws IOException {
        try (InputStream is = new BufferedInputStream(new FileInputStream(metaFile))) {
            putObjectMetadataVersionStream(bucket, objectName, version, is);
        }
    }

    @Override
    public void putObjectMetadataFile(ServerBucket bucket, String objectName, File metaFile) throws IOException {
        Check.requireNonNullArgument(bucket, "bucket is null");
        try (InputStream is = new BufferedInputStream(new FileInputStream(metaFile))) {
            putObjectMetadataStream(bucket, objectName, is);
        }
    }

    /**
     * @param bucketName
     * @return
     * @throws IOException
     */
    @Override
    public File createBucket(BucketMetadata meta) throws IOException {

        Check.requireNonNullArgument(meta, "meta is null");
        Check.requireNonNullStringArgument(meta.bucketName, "bucketName is null");
        Check.requireNonNullArgument(meta.id, "Id is null");

        File metadata_dir = new File(this.getBucketsDirPath() + File.separator + meta.id.toString());
        File data_dir = new File(this.getRootDirPath() + File.separator + meta.id.toString());
        File cache_dir = new File(this.getCacheDirPath() + File.separator + meta.id.toString());
        File data_version_dir = new File(this.getRootDirPath() + File.separator + meta.id.toString() + File.separator
                + VirtualFileSystemService.VERSION_DIR);
        File work_dir = new File(this.getWorkDirPath() + File.separator + meta.id.toString());

        try {

            this.drive_lock.writeLock().lock();

            if (metadata_dir.exists() && metadata_dir.isDirectory())
                throw new IllegalArgumentException("Bucket already exist -> b: " + meta.id.toString());

            FileUtils.forceMkdir(metadata_dir);

            if ((!data_dir.exists()) || (!data_dir.isDirectory()))
                FileUtils.forceMkdir(data_dir);

            if ((!data_version_dir.exists()) || (!data_version_dir.isDirectory()))
                FileUtils.forceMkdir(data_version_dir);

            if ((!work_dir.exists()) || (!work_dir.isDirectory()))
                FileUtils.forceMkdir(work_dir);

            if ((!cache_dir.exists()) || (!cache_dir.isDirectory()))
                FileUtils.forceMkdir(cache_dir);

            saveBucketMetadata(meta);

            return data_dir;

        } catch (Exception e) {
            throw new InternalCriticalException(e, "b:" + meta.id.toString() + ", d:" + getName());
        }

        finally {
            this.drive_lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteBucket(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        if (isEmpty(bucket))
            deleteBucketInternal(bucket);
    }

    public void forceDeleteBucketById(Long bucketId) {
        Check.requireNonNullArgument(bucketId, "bucketId");
        deleteBucketInternal(bucketId);
    }

    /**
     * <p>
     * If the control directory contains the object directory -> the file is
     * present<br/>
     * Checks whether the File exists, regardless of the status of the object
     * </p>
     * 
     * @param bucketName
     * @param fileName
     * @return
     */
    @Override
    public boolean existsBucketById(Long bucketId) {
        return new File(getBucketMetadataDirPathById(bucketId)).exists();
    }

    @Override
    public boolean existsObjectMetadata(ObjectMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        File objectMetadataDir = new File(this.getObjectMetadataDirPathById(meta.getBucketId(), meta.getObjectName()));
        if (!objectMetadataDir.exists())
            return false;
        return (getObjectMetadataById(meta.getBucketId(), meta.getObjectName()).status != ObjectStatus.DELETED);
    }

    /**
     * <p>
     * Checks whether the File exists, regardless of the status of the object It is
     * on the upper layers to return false if the object owner of this file is in
     * state DELETED
     * </p>
     */
    @Override
    public boolean existsObjectMetadata(ServerBucket bucket, String objectName) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null " + objectInfo(bucket));

        ObjectPath path = new ObjectPath(this, bucket, objectName);

        File objectMetadataDir = path.metadataDirPath().toFile();

        // File objectMetadataDir = new File(this.getObjectMetadataDirPath(bucket,
        // objectName));

        if (!objectMetadataDir.exists())
            return false;

        return (getObjectMetadata(bucket, objectName).status != ObjectStatus.DELETED);
    }

    @Override
    public void markAsDeletedBucket(ServerBucket bucket) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(this.driveBuckets.containsKey(bucket.getId()), "bucket does not exist");

        try {
            this.drive_lock.writeLock().lock();
            DriveBucket db = this.driveBuckets.get(bucket.getId());
            BucketMetadata meta = db.getBucketMetadata();
            meta.status = BucketStatus.DELETED;
            meta.lastModified = OffsetDateTime.now();
            saveBucketMetadata(meta);
        } finally {
            this.drive_lock.writeLock().unlock();
        }
    }

    @Override
    public void markAsEnabledBucket(ServerBucket bucket) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(this.driveBuckets.containsKey(bucket.getId()), "bucket does not exist");
        try {
            this.drive_lock.writeLock().lock();
            DriveBucket db = this.driveBuckets.get(bucket.getId());
            BucketMetadata meta = db.getBucketMetadata();
            meta.status = BucketStatus.ENABLED;
            meta.lastModified = OffsetDateTime.now();
            saveBucketMetadata(meta);
        } finally {
            this.drive_lock.writeLock().unlock();
        }
    }

    /**
     * 
     */
    @Override
    public void markAsDeletedObject(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null " + objectInfo(bucket));

        ObjectMetadata meta = getObjectMetadata(bucket, objectName);
        meta.status = ObjectStatus.DELETED;
        saveObjectMetadata(meta, true);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    @JsonIgnore
    public List<DriveBucket> getBuckets() {
        try {
            this.drive_lock.readLock().lock();
            List<DriveBucket> list = new ArrayList<DriveBucket>();
            for (Entry<Long, DriveBucket> entry : this.driveBuckets.entrySet())
                list.add(entry.getValue());
            list.sort(new Comparator<DriveBucket>() {
                @Override
                public int compare(DriveBucket o1, DriveBucket o2) {
                    if (o1.getName() == null)
                        return 1;
                    if (o2.getName() == null)
                        return -1;
                    return o1.getName().compareToIgnoreCase(o2.getName());
                }
            });

            return list;

        } catch (Exception e) {
            throw new InternalCriticalException(e, "getBuckets");
        } finally {
            this.drive_lock.readLock().unlock();
        }
    }

    @JsonIgnore
    @Override
    public String getName() {
        return name;
    }

    /**
     */
    @JsonIgnore
    @Override
    public String getRootDirPath() {
        return rootDir;
    }

    @JsonIgnore
    @Override
    public String getSysDirPath() {
        return getRootDirPath() + File.separator + VirtualFileSystemService.SYS;
    }

    @JsonIgnore
    @Override
    public String getWorkDirPath() {
        return getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + VirtualFileSystemService.WORK;
    }

    @JsonIgnore
    @Override
    public String getCacheDirPath() {
        return getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + VirtualFileSystemService.CACHE;
    }

    @JsonIgnore
    @Override
    public String getSchedulerDirPath() {
        return getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator
                + VirtualFileSystemService.SCHEDULER;
    }

    @JsonIgnore
    @Override
    public String getJournalDirPath() {
        return getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + VirtualFileSystemService.JOURNAL;
    }

    @JsonIgnore
    @Override
    public String getBucketsDirPath() {
        return getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + VirtualFileSystemService.BUCKETS;
    }

    @JsonIgnore
    @Override
    public String getTempDirPath() {
        return getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + VirtualFileSystemService.TEMP;
    }

    /**
     * <p>
     * ObjectMetadata
     * </p>
     */
    @Override
    public ObjectMetadata getObjectMetadata(ServerBucket bucket, String objectName) {
        try {
            return getObjectMapper().readValue(getObjectMetadataFileById(bucket.getId(), objectName), ObjectMetadata.class);
        } catch (FileNotFoundException e) {
            throw new InternalCriticalException(e);
        } catch (Exception e) {
            throw new InternalCriticalException(e);
        }
    }

    private ObjectMetadata getObjectMetadataById(Long bucketId, String objectName) {
        try {
            return getObjectMapper().readValue(getObjectMetadataFileById(bucketId, objectName), ObjectMetadata.class);
        } catch (Exception e) {
            throw new InternalCriticalException(e);
        }
    }

    /**
     * ObjectMetadata Version
     * 
     * If the version was removed by a {@code deleteObjectAllPreviousVersions} the
     * file will not exist anymore. In that case returns null
     * 
     */
    @Override
    public ObjectMetadata getObjectMetadataVersion(ServerBucket bucket, String objectName, int version) {
        try {

            Check.requireNonNullArgument(bucket, "bucked is null");
            Check.requireNonNullStringArgument(objectName, "objectName is null " + objectInfo(bucket));

            File file = getObjectMetadataVersionFile(bucket, objectName, version);

            if (!file.exists())
                return null;

            return getObjectMapper().readValue(file, ObjectMetadata.class);

        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    /**
     * Head Version
     */
    public void saveObjectMetadata(ObjectMetadata meta) {
        saveObjectMetadata(meta, true);
    }

    public void saveObjectMetadataVersion(ObjectMetadata meta) {
        saveObjectMetadata(meta, false);
    }

    protected void saveObjectMetadata(ObjectMetadata meta, boolean isHead) {

        Check.requireNonNullArgument(meta, "meta");
        Check.requireNonNullArgument(meta.bucketId, "meta.bucketId is null");
        Check.requireNonNullStringArgument(meta.objectName, "meta.objectName is null");

        Optional<Integer> version = isHead ? Optional.empty() : Optional.of(meta.getVersion());

        try {
            File dir = new File(getObjectMetadataDirPathById(meta.getBucketId(), meta.getObjectName()));

            if (!dir.exists())
                FileUtils.forceMkdir(dir);

            meta.setLastModified(OffsetDateTime.now());
            String jsonString = getObjectMapper().writeValueAsString(meta);

            ObjectPath path = new ObjectPath(this, meta.getBucketId(), meta.getObjectName());

            if (isHead) {
                Files.writeString(path.metadataFilePath(), jsonString);
                // Files.writeString(
                // Paths.get(getObjectMetadataDirPathById(meta.getBucketId(),
                // meta.getObjectName()) + File.separator + meta.getObjectName() +
                // ServerConstant.JSON), jsonString);
            } else {

                Files.writeString(Paths.get(
                        getObjectMetadataVersionFilePathById(meta.getBucketId(), meta.getObjectName(), version.get().intValue())),
                        jsonString);
            }

        } catch (Exception e) {
            throw new InternalCriticalException(e, "b:" + meta.getBucketId().toString() + "o: + " + meta.getObjectName() + " v: "
                    + (version.isPresent() ? String.valueOf(version.get()) : "head"));
        }
    }

    @Override
    public void removeScheduler(ServiceRequest serviceRequest, String queueId) {

        Check.requireNonNullStringArgument(queueId, "queueId is null");

        try {
            String name = getSchedulerDirPath() + File.separator + queueId + File.separator + serviceRequest.getId()
                    + ServerConstant.JSON;
            File file = new File(name);
            if (file.exists())
                Files.delete(Paths.get(name));
            else
                logger.debug("Remove file not found -> d: " + getName() + " | f:" + (file != null ? file.getName() : "null"));

        } catch (Exception e) {
            throw new InternalCriticalException(e,
                    "op: " + (Optional.ofNullable(serviceRequest).isPresent() ? (serviceRequest.toString()) : "null"));
        }
    }

    @Override
    public void saveScheduler(ServiceRequest serviceRequest, String queueId) {

        Check.requireNonNullStringArgument(queueId, "queueId is null");
        Check.requireNonNullArgument(serviceRequest, "serviceRequest is null");

        try {

            String jsonString = getObjectMapper().writeValueAsString(serviceRequest);

            File dir = new File(getSchedulerDirPath() + File.separator + queueId);
            if (!dir.exists() || !dir.isDirectory()) {
                try {
                    io.odilon.util.OdilonFileUtils.forceMkdir(dir);
                } catch (IOException e) {
                    throw new InternalCriticalException(e, "Can not create dir -> d: " + getName() + " dir:" + dir.getName());
                }
            }
            String name = getSchedulerDirPath() + File.separator + queueId + File.separator + serviceRequest.getId()
                    + ServerConstant.JSON;
            Files.writeString(Paths.get(name), jsonString);

        } catch (Exception e) {
            throw new InternalCriticalException(e,
                    "op: " + (Optional.ofNullable(serviceRequest).isPresent() ? (serviceRequest.toString()) : "null"));
        }
    }

    @Override
    public synchronized List<File> getSchedulerRequests(String queueId) {

        Check.requireNonNullStringArgument(queueId, "queueId is null");
        try {
            List<File> ret = new ArrayList<File>();
            File schedulerDir = new File(getSchedulerDirPath() + File.separator + queueId);
            if ((!schedulerDir.exists()) || (!schedulerDir.isDirectory()) || (!schedulerDir.canRead()))
                return ret;
            for (File file : schedulerDir.listFiles()) {
                if (file.exists() && (!file.isDirectory())) {
                    ret.add(file);
                }
            }
            return ret;

        } catch (Exception e) {
            throw new InternalCriticalException(e, "getSchedulerRequests " + queueId);
        }
    }

    @Override
    public void saveJournal(VirtualFileSystemOperation op) {

        Check.requireNonNullArgument(op, "op is null");
        try {
            String jsonString = getObjectMapper().writeValueAsString(op);
            Files.writeString(Paths.get(getJournalDirPath() + File.separator + op.getId() + ServerConstant.JSON), jsonString);
        } catch (Exception e) {
            throw new InternalCriticalException(e, "op: " + (Optional.ofNullable(op).isPresent() ? (op.toString()) : "null"));
        }
    }

    @Override
    public void removeJournal(String id) {
        Check.requireNonNullArgument(id, "id is null");
        try {
            Files.delete(Paths.get(getJournalDirPath() + File.separator + id + ServerConstant.JSON));
        } catch (Exception e) {
            throw new InternalCriticalException(e, "id: " + (Optional.ofNullable(id).isPresent() ? id : "null"));
        }
    }

    @Override
    public File getObjectMetadataVersionFile(ServerBucket bucket, String objectName, int version) {
        return new File(getObjectMetadataVersionFilePath(bucket, objectName, version));
    }

    /**
     * @param bucketName
     * @param objectName
     * @param version
     * @param stream
     */
    private void putObjectMetadataVersionStream(ServerBucket bucket, String objectName, int version, InputStream stream) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullArgument(stream, "stream is null");
        try {
            transferTo(stream, this.getObjectMetadataVersionFilePath(bucket, objectName, version));
        } catch (Exception e) {
            throw new InternalCriticalException(e,
                    objectInfo(bucket, objectName) + ", version:" + String.valueOf(version) + ", d:" + getName());
        }
    }

    /**
     * 
     */
    @Override
    public void removeSysFile(String fileName) {
        Check.requireNonNullStringArgument(fileName, "fileName is null");
        try {
            Files.delete(
                    Paths.get(this.getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + fileName));
        } catch (Exception e) {
            throw new InternalCriticalException(e, fileInfo(fileName));
        }
    }

    /**
     * 
     * 
     */
    @Override
    public void putSysFile(String fileName, String content) {
        Check.requireNonNullStringArgument(fileName, "fileName is null");
        Check.requireNonNullStringArgument(content, "content can not be null | f:" + fileName);
        try {
            Files.writeString(
                    Paths.get(this.getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + fileName),
                    content);
        } catch (Exception e) {
            throw new InternalCriticalException(e, "f:" + fileName + " d:" + getName());
        }
    }

    @Override
    public File getSysFile(String fileName) {
        Check.requireNonNullStringArgument(fileName, "fileName is null");
        return new File(this.getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + fileName);
    }

    @Override
    public boolean isEmpty(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return isEmpty(bucket.getId());
    }

    private boolean isEmpty(Long bucketId) {
        Check.requireNonNullArgument(bucketId, "bucketId is null");
        File file = new File(this.getBucketMetadataDirPathById(bucketId));
        Path path = file.toPath();

        // TBA -> ver metadata ObjectStatus
        if (Files.isDirectory(path)) {
            try (Stream<Path> entries = Files.list(path)) {
                return !entries.filter(pa -> pa.toFile().isDirectory()).findFirst().isPresent();
            } catch (IOException e) {
                throw new InternalCriticalException(e, "b:" + bucketId.toString());
            }
        }
        return true;
    }

    public void setStatus(ServiceStatus status) {
        this.status = status;
    }

    @JsonIgnore
    public ServiceStatus getStatus() {
        return this.status;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof Drive))
            return false;

        if (((Drive) o).getName() == null || ((Drive) o).getRootDirPath() == null)
            return false;

        if (name == null || rootDir == null)
            return false;

        return name.equals(((Drive) o).getName()) && rootDir.equals(((Drive) o).getRootDirPath());
    }

    @Override
    public String ping() {
        return "ok";
    }

    /**
     * @param stream
     * @param destFileName
     * @throws IOException
     */
    protected void transferTo(InputStream stream, String destFileName) throws IOException {
        byte[] buf = new byte[ServerConstant.BUFFER_SIZE];
        int bytesRead;
        BufferedOutputStream out = null;
        try {
            out = new BufferedOutputStream(new FileOutputStream(destFileName), ServerConstant.BUFFER_SIZE);
            while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0)
                out.write(buf, 0, bytesRead);
        } catch (IOException e) {
            throw (e);

        } finally {

            if (stream != null)
                stream.close();

            if (out != null)
                out.close();
        }
    }

    @JsonIgnore
    @Override
    public long getAvailableSpace() {
        File file = new File(getRootDirPath());
        return file.getUsableSpace();
    }

    @JsonIgnore
    @Override
    public long getTotalSpace() {
        File file = new File(getRootDirPath());
        return file.getTotalSpace();
    }

    public void setConfigOrder(int order) {
        this.configOrder = order;
    }

    @JsonIgnore
    @Override
    public int getConfigOrder() {
        return this.configOrder;
    }

    @Override
    public synchronized void cleanUpCacheDir(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        try {
            int n = 0;
            File bucketDir = new File(getBucketCacheDirPath(bucket));
            if (bucketDir.exists()) {
                File files[] = bucketDir.listFiles();
                for (File fi : files) {
                    FileUtils.deleteQuietly(fi);
                    if (n++ > 50000)
                        break;
                }
            }
            if (n > 0)
                logger.debug("Removed temp files from  dir:" + getBucketCacheDirPath(bucket) + " | b:" + bucket.getName()
                        + " total:" + String.valueOf(n));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    @Override
    public synchronized void cleanUpWorkDir(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        try {
            int n = 0;
            File bucketDir = new File(getBucketWorkDirPath(bucket));
            if (bucketDir.exists()) {
                File files[] = bucketDir.listFiles();
                for (File fi : files) {
                    FileUtils.deleteQuietly(fi);
                    if (n++ > 50000)
                        break;
                }
            }
            if (n > 0)
                logger.debug("Removed temp files from dir: " + getBucketWorkDirPath(bucket) + " | b:" + bucket.getName() + " total:"
                        + String.valueOf(n));
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    /**
     * 
     */
    @PostConstruct
    protected void onInitialize() {

        try {

            setStatus(ServiceStatus.STARTING);

            File base = new File(getRootDirPath());

            this.drive_lock.writeLock().lock();

            /** rootdir */
            if (!base.exists() || !base.isDirectory())
                createRootDirectory();

            /** sys */
            File sys = new File(getSysDirPath());
            if (!sys.exists() || !sys.isDirectory())
                createSysDirectory();

            /** buckets */
            File buckets = new File(getBucketsDirPath());
            if (!buckets.exists() || !buckets.isDirectory())
                createBucketsDirectory();

            /** version directory for data */
            checkBucketDataVersionDirs();

            /** journal */
            File journal = new File(getJournalDirPath());
            if (!journal.exists() || !journal.isDirectory())
                createJournalDirectory();

            /** temp */
            File temp = new File(getTempDirPath());
            if (!temp.exists() || !temp.isDirectory())
                createTempDirectory();

            /** work */
            File work = new File(getWorkDirPath());
            if (!work.exists() || !work.isDirectory())
                createWorkDirectory();

            /** file cache */
            File cache = new File(getCacheDirPath());
            if (!cache.exists() || !cache.isDirectory())
                createCacheDirectory();

            /** version directory for data */
            checkWorkBucketDirs();

            /** scheduler */
            File scheduler = new File(getSchedulerDirPath());
            if (!scheduler.exists() || !scheduler.isDirectory())
                createSchedulerDirectory();

            DriveInfo info = readDriveMetadata();

            if (readDriveMetadata() == null) {
                info = new DriveInfo(getName(), randomString(12), OffsetDateTime.now(), DriveStatus.NOTSYNC, getConfigOrder());
                saveDriveMetadata(info);
            }

            this.driveInfo = readDriveMetadata();

            loadBuckets();

            setStatus(ServiceStatus.RUNNING);

        } catch (Exception e) {
            setStatus(ServiceStatus.STOPPED);
            throw new InternalCriticalException(e, "Drive Startup | d:" + getName());
        }

        finally {
            this.drive_lock.writeLock().unlock();
            startuplogger.debug("Started -> Drive: " + this.name + " | rootDir: " + this.rootDir);
        }
    }

    /**
     * 
     */
    protected void deleteBucketInternal(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireTrue(isEmpty(bucket), "bucket is not empty -> " + objectInfo(bucket));
        deleteBucketInternal(bucket.getId());
    }

    protected void deleteBucketInternal(Long bucketId) {
        Check.requireNonNullArgument(bucketId, "bucketId is null");

        File metadata_dir = new File(this.getBucketsDirPath() + File.separator + bucketId.toString());

        File data_dir = new File(this.getRootDirPath() + File.separator + bucketId.toString());
        File data_version_dir = new File(this.getRootDirPath() + File.separator + bucketId.toString() + File.separator
                + VirtualFileSystemService.VERSION_DIR);
        File work_dir = new File(this.getBucketWorkDirPathById(bucketId));

        boolean done = false;

        try {

            this.drive_lock.writeLock().lock();

            if (metadata_dir.exists() && metadata_dir.isDirectory()) {
                FileUtils.deleteQuietly(metadata_dir);
                this.driveBuckets.remove(bucketId);
                done = true;
            }

            if (data_dir.exists() && data_dir.isDirectory()) {
                FileUtils.deleteQuietly(data_dir);
            }

            if (data_version_dir.exists() && data_version_dir.isDirectory()) {
                FileUtils.deleteQuietly(data_version_dir);
            }

            if (work_dir.exists() && work_dir.isDirectory()) {
                FileUtils.deleteQuietly(work_dir);
            }

        } catch (Exception e) {
            if (done) {
                return;
            }
            throw e;
        } finally {
            this.drive_lock.writeLock().unlock();
        }
    }

    @Override
    public void updateBucket(BucketMetadata meta) throws IOException {
        Check.requireNonNullArgument(meta, "meta is null");
        saveBucketMetadata(meta);
    }

    protected void saveBucketMetadata(BucketMetadata meta) {
        Check.requireNonNullArgument(meta, "meta is null");
        Check.requireNonNullArgument(meta.id, "meta.id is null");
        try {
            String jsonString = getObjectMapper().writeValueAsString(meta);
            Files.writeString(Paths.get(this.getBucketsDirPath() + File.separator + meta.id.toString() + File.separator
                    + meta.id.toString() + ServerConstant.JSON), jsonString);
            this.driveBuckets.put(meta.id, new DriveBucket(this, meta));

        } catch (Exception e) {
            throw new InternalCriticalException(e, "b:" + meta.bucketName + ", d:" + getName());
        }
    }

    /**
     * @param bucketName
     */
    protected void createDataBucketDirIfNotExists(Long bucketId) {

        Check.requireNonNullArgument(bucketId, "bucketId is null");

        try {

            this.drive_lock.writeLock().lock();

            /** data */
            if (!Files.exists(Paths.get(getRootDirPath() + File.separator + bucketId.toString()))) {
                logger.debug("Creating Data Bucket Directory for  -> b:" + bucketId.toString());
                io.odilon.util.OdilonFileUtils.forceMkdir(new File(getRootDirPath() + File.separator + bucketId.toString()));
            }

            /** data version */
            if (!Files.exists(Paths.get(getRootDirPath() + File.separator + bucketId.toString() + File.separator
                    + VirtualFileSystemService.VERSION_DIR))) {
                io.odilon.util.OdilonFileUtils.forceMkdir(new File(getRootDirPath() + File.separator + bucketId.toString()
                        + File.separator + VirtualFileSystemService.VERSION_DIR));
            }

        } catch (IOException e) {
            throw new InternalCriticalException(e, "Can not create Bucket Data Directory -> d:" + getName() + " b:"
                    + (Optional.ofNullable(bucketId.toString()).orElse("null")));
        } finally {
            this.drive_lock.writeLock().unlock();
        }
    }

    protected boolean existBucketMetadataDir(ServerBucket bucket) {
        return Files.exists(Paths.get(getBucketMetadataDirPath(bucket)));
    }

    protected void createRootDirectory() {
        try {
            logger.debug("Creating Data Directory -> " + getRootDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getRootDirPath()));

        } catch (IOException e) {
            throw new InternalCriticalException(e, "Can not create root Directory -> dir:" + rootDir + "  | d:" + name);
        }
    }

    protected void createSysDirectory() {
        try {
            logger.debug("Creating Directory -> " + getSysDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getSysDirPath()));

        } catch (IOException e) {
            String msg = "Can not create sys Directory ->  dir:" + getSysDirPath() + "  d:" + name;
            throw new InternalCriticalException(e, msg);
        }
    }

    private void createWorkDirectory() {
        try {

            logger.debug("Creating Directory -> " + getWorkDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getWorkDirPath()));

        } catch (IOException e) {
            String msg = "Can not create work Directory ->  dir:" + getWorkDirPath() + "  d:" + name;
            throw new InternalCriticalException(e, msg);
        }
    }

    private void createCacheDirectory() {
        try {

            logger.debug("Creating Directory -> " + getCacheDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getCacheDirPath()));

        } catch (IOException e) {
            String msg = "Can not create cache Directory ->  dir:" + getCacheDirPath() + "  d:" + name;
            throw new InternalCriticalException(e, msg);
        }
    }

    private void createSchedulerDirectory() {
        try {
            logger.debug("Creating Directory -> " + getSchedulerDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getSchedulerDirPath()));

        } catch (IOException e) {
            String msg = "Can not create scheduler Directory ->  dir:" + getSchedulerDirPath() + "  d:" + name;
            throw new InternalCriticalException(e, msg);
        }
    }

    private void createBucketsDirectory() {
        try {
            logger.debug("Creating Directory -> " + getBucketsDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getBucketsDirPath()));

        } catch (IOException e) {
            String msg = "Can not create Buckets Metadata Directory ->  dir:" + getBucketsDirPath() + "  d:" + name;
            throw new InternalCriticalException(e, msg);
        }
    }

    private void createJournalDirectory() {
        try {

            logger.debug("Creating Directory -> " + getJournalDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getJournalDirPath()));

        } catch (IOException e) {
            String msg = "Can not create Journal Directory ->  dir:" + getJournalDirPath() + "  d:" + name;
            throw new InternalCriticalException(e, msg);

        }
    }

    private void createTempDirectory() {
        try {
            logger.debug("Creating Directory -> " + getTempDirPath());
            io.odilon.util.OdilonFileUtils.forceMkdir(new File(getTempDirPath()));

        } catch (IOException e) {
            String msg = "Can not create temp Directory ->  dir:" + getTempDirPath() + "  d:" + name;
            throw new InternalCriticalException(e, msg);
        }
    }

    private void checkWorkBucketDirs() {

        Path start = new File(this.getBucketsDirPath()).toPath();
        Stream<Path> stream = null;
        try {
            stream = Files.walk(start, 1).skip(1).filter(file -> Files.isDirectory(file));
        } catch (IOException e) {
            throw new InternalCriticalException(e, "checkWorkBucketDirs");
        }

        try {

            Iterator<Path> it = stream.iterator();

            while (it.hasNext()) {

                Path pbucket = it.next();

                Long bucketId = Long.valueOf(pbucket.toFile().getName());

                File workDir = new File(getBucketWorkDirPathById(bucketId));
                if (!workDir.exists()) {
                    logger.debug("Creating Directory -> " + workDir.getName());
                    try {
                        io.odilon.util.OdilonFileUtils.forceMkdir(workDir);
                    } catch (IOException e) {
                        throw new InternalCriticalException(e, "Can not create -> " + workDir.getName());
                    }
                }

                File cacheDir = new File(getBucketCacheDirPathById(bucketId));

                if (!cacheDir.exists()) {
                    logger.debug("Creating Directory -> " + cacheDir.getName());
                    try {
                        io.odilon.util.OdilonFileUtils.forceMkdir(cacheDir);
                    } catch (IOException e) {
                        throw new InternalCriticalException(e, "Can not create -> " + cacheDir.getName());
                    }
                }
            }
        } finally {
            if (stream != null)
                stream.close();
        }
    }

    private void checkBucketDataVersionDirs() {

        Path start = new File(this.getRootDirPath()).toPath();
        Stream<Path> stream = null;
        try {
            stream = Files.walk(start, 1).skip(1).filter(file -> Files.isDirectory(file))
                    .filter(file -> (!file.getFileName().toString().equals(VirtualFileSystemService.SYS)));
        } catch (IOException e) {
            throw new InternalCriticalException(e, "checkBucketDataVersionDirs");
        }

        try {
            Iterator<Path> it = stream.iterator();
            while (it.hasNext()) {
                Path bucket = it.next();
                String version = bucket.toFile().getAbsolutePath() + File.separator + VirtualFileSystemService.VERSION_DIR;
                boolean ex = (new File(version)).exists();
                if (!ex) {
                    logger.debug("Creating Directory -> " + version);
                    try {
                        io.odilon.util.OdilonFileUtils.forceMkdir(new File(version));
                    } catch (IOException e) {
                        throw new InternalCriticalException(e, "Can not create -> " + version);
                    }
                }
            }
        } finally {
            if (stream != null)
                stream.close();
        }
    }

    /**
     * 
     */
    private void loadBuckets() {

        try {
            this.drive_lock.readLock().lock();
            try {
                List<Path> subBucket = Files.walk(Paths.get(getBucketsDirPath()), 1).filter(new Predicate<Path>() {
                    public boolean test(Path o) {
                        return (Files.isDirectory(o) && (!VirtualFileSystemService.BUCKETS.equals(o.toFile().getName())));
                    }
                }).collect(Collectors.toList());

                subBucket.forEach(item -> {
                    try {
                        driveBuckets.put(Long.valueOf(item.toFile().getName()),
                                new DriveBucket(OdilonDrive.this, getBucketMetadataById(Long.valueOf(item.toFile().getName()))));
                    } catch (IOException e) {
                        throw new InternalCriticalException(e, "loadbuckets");
                    }
                });
            } catch (IOException e) {
                throw new InternalCriticalException(e, "loadbuckets");
            }
        } finally {
            this.drive_lock.readLock().unlock();
        }
    }

    private synchronized void saveDriveMetadata(DriveInfo info) {
        Check.requireNonNullArgument(info, "info is null");
        String jsonString = null;
        try {

            jsonString = getObjectMapper().writeValueAsString(info);
            Files.writeString(Paths.get(getSysDirPath() + File.separator + VirtualFileSystemService.DRIVE_INFO), jsonString);

        } catch (Exception e) {
            throw new InternalCriticalException(e, "json:" + (Optional.ofNullable(jsonString).isPresent() ? jsonString : "null"));
        }
    }

    private synchronized DriveInfo readDriveMetadata() {

        File file = null;
        try {
            file = new File(getSysDirPath() + File.separator + VirtualFileSystemService.DRIVE_INFO);

            if (!file.exists())
                return null;

            return getObjectMapper().readValue(file, DriveInfo.class);

        } catch (Exception e) {
            throw new InternalCriticalException(e, "f:" + (Optional.ofNullable(file).isPresent() ? file.getName() : "null"));
        }
    }

    private String getObjectMetadataFilePathById(Long bucketId, String objectName) {
        return getObjectMetadataDirPathById(bucketId, objectName) + File.separator + objectName + ServerConstant.JSON;
    }

    // private String getObjectMetadataFilePath(ServerBucket bucket, String
    // objectName) {
    // return getObjectMetadataDirPath(bucket, objectName) + File.separator +
    // objectName + ServerConstant.JSON;
    // }

    private String getObjectMetadataVersionFilePath(ServerBucket bucket, String objectName, int version) {
        return getObjectMetadataDirPath(bucket, objectName) + File.separator + objectName
                + VirtualFileSystemService.VERSION_EXTENSION + String.valueOf(version) + ServerConstant.JSON;
    }

    private String getObjectMetadataVersionFilePathById(Long bucketId, String objectName, int version) {
        return getObjectMetadataDirPathById(bucketId, objectName) + File.separator + objectName
                + VirtualFileSystemService.VERSION_EXTENSION + String.valueOf(version) + ServerConstant.JSON;
    }

    /**
     * @param bucketName
     * @param objectName
     * @param version
     * @param stream
     */
    private void putObjectMetadataStream(ServerBucket bucket, String objectName, InputStream stream) {

        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null " + objectInfo(bucket));
        Check.requireNonNullArgument(stream, "stream is null");

        if (!existBucketMetadataDir(bucket))
            throw new IllegalArgumentException(
                    "Bucket Metadata Directory must exist -> d:" + getName() + " | b:" + objectInfo(bucket));
        try {
            transferTo(stream, this.getBucketMetadataDirPath(bucket) + File.separator + objectName + File.separator + objectName
                    + ServerConstant.JSON);
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    /** Delete Metadata directory */
    @Override
    public void deleteObjectMetadata(ServerBucket bucket, String objectName) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        Check.requireNonNullStringArgument(objectName, "objectName is null " + objectInfo(bucket));
        FileUtils.deleteQuietly(new File(this.getObjectMetadataDirPath(bucket, objectName)));
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

    private String fileInfo(String fileName) {
        return "f:" + (fileName != null ? fileName : "null") + " d:" + getName();
    }

    public String objectInfo(ServerBucket bucket, String objectName) {
        return "bn:" + (bucket != null ? bucket.getId().toString() : "null") + " o:" + (objectName != null ? objectName : "null");
    }

    private String getBucketWorkDirPathById(Long id) {
        return this.getWorkDirPath() + File.separator + id.toString();
    }

    private File getObjectMetadataFileById(Long bucketId, String objectName) {
        return new File(getObjectMetadataFilePathById(bucketId, objectName));
    }

    private String getBucketMetadataDirPathById(Long id) {
        return this.getBucketsDirPath() + File.separator + id.toString();
    }

    private String getObjectMetadataDirPathById(Long bucketId, String objectName) {
        return getBucketsDirPath() + File.separator + bucketId.toString() + File.separator + objectName;
    }

}
