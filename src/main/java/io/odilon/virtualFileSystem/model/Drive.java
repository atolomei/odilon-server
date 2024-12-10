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
import java.util.List;

import io.odilon.model.BucketMetadata;
import io.odilon.model.ObjectMetadata;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.virtualFileSystem.DriveInfo;

/**
 * <p>
 * A Drive or Volume is File System directory that acts as a storage unit
 * </p>
 * 
 * <p>
 * a Drive is <b>not</b> ThreadSafe, concurrency control is the responsibility
 * of whoever uses its methods.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface Drive {

    /**
     * ----------------- Drive Info ------------------
     */
    public DriveInfo getDriveInfo();

    public void setDriveInfo(DriveInfo info);

    public void cleanUpWorkDir(Long bucketId);

    public void cleanUpCacheDir(Long bucketId);

    /**
     * ----------------- Journal ------------------
     */
    public void saveJournal(VFSOperation op);

    public void removeJournal(String id);

    /**
     * ----------------- Sys ------------------
     */
    public void putSysFile(String fileName, String content);

    public File getSysFile(String fileName);

    public void removeSysFile(String fileName);

    /**
     * ----------------- Info ------------------
     */
    public String getRootDirPath();

    public String getSysDirPath();

    public String getBucketsDirPath();

    public String getCacheDirPath();

    public String getBucketCacheDirPath(Long bucketId);

    public String getWorkDirPath();

    public String getBucketWorkDirPath(Long bucketId);

    String getJournalDirPath();

    String getTempDirPath();

    public String getSchedulerDirPath();

    public String ping();

    public long getTotalSpace();

    public long getAvailableSpace();

    public String getName();

    /** order in the rootDirs variable in odilon.properties */
    public int getConfigOrder();

    /**
     * ----------------- Scheduler ------------------
     */
    public void saveScheduler(ServiceRequest serviceRequest, String queueId);

    public void removeScheduler(ServiceRequest serviceRequest, String queueId);

    public List<File> getSchedulerRequests(String queueId);

    /**
     * ----------------- Bucket ------------------
     */
    public File createBucket(BucketMetadata meta) throws IOException;

    public void updateBucket(BucketMetadata meta) throws IOException;

    public BucketMetadata getBucketMetadata(Long bucketId) throws IOException;

    public boolean existsBucket(Long bucketId);

    public void deleteBucket(Long bucketId);

    public List<DriveBucket> getBuckets();

    public void markAsDeletedBucket(Long bucketId);

    public void markAsEnabledBucket(Long bucketId);

    public boolean isEmpty(ServerBucket bucket);
    // public boolean isEmpty(Long bucketId);

    public String getBucketMetadataDirPath(Long bucketId);

    public String getBucketObjectDataDirPath(Long bucketId);

    /**
     * ---------------------- ObjectMetadata (head) ----------------------
     */

    public boolean existsObjectMetadata(ServerBucket bucket, String objectName);

    public boolean existsObjectMetadata(ObjectMetadata meta);

    public void markAsDeletedObject(Long bucketId, String objectName);

    public String getObjectMetadataDirPath(Long bucketId, String objectName);

    public ObjectMetadata getObjectMetadata(Long bucketId, String objectName);

    public void deleteObjectMetadata(Long bucketId, String objectName);

    public void saveObjectMetadata(ObjectMetadata meta);

    public File getObjectMetadataFile(Long bucketId, String objectName);

    public void putObjectMetadataFile(Long bucketId, String objectName, File metaFile) throws IOException;;

    /** ObjectMetadata. Version --- */
    public void saveObjectMetadataVersion(ObjectMetadata meta);

    public ObjectMetadata getObjectMetadataVersion(Long bucketId, String objectName, int version);

    public File getObjectMetadataVersionFile(Long bucketId, String objectName, int version);

    public void putObjectMetadataVersionFile(Long bucketId, String objectName, int version, File metaFile) throws IOException;

}
