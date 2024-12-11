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
    
    /** order in the rootDirs variable in odilon.properties */
    public int getConfigOrder();
    
    public String getRootDirPath();

    public String getSysDirPath();

    public String getBucketsDirPath();

    public String getCacheDirPath();

    public String getWorkDirPath();

    public String getJournalDirPath();

    public String getTempDirPath();

    public String getSchedulerDirPath();

    public String ping();

    public long getTotalSpace();

    public long getAvailableSpace();

    public String getName();

    

    /**
     * ----------------- Scheduler ------------------
     */
    public void saveScheduler(ServiceRequest serviceRequest, String queueId);

    public void removeScheduler(ServiceRequest serviceRequest, String queueId);

    public List<File> getSchedulerRequests(String queueId);

    /**
     * ----------------- Bucket ------------------
     */
    
    /** by id */
    
    
    public BucketMetadata getBucketMetadataById(Long bucketId) throws IOException;
    
    public boolean existsBucketById(Long bucketId);

    /** interface **/
    
    public BucketMetadata getBucketMetadata(ServerBucket bucket) throws IOException;
    
    public File createBucket(BucketMetadata meta) throws IOException;

    public void updateBucket(BucketMetadata meta) throws IOException;
    
    public void deleteBucket(ServerBucket bucket);

    public List<DriveBucket> getBuckets();

    public void markAsDeletedBucket(ServerBucket bucket);

    public void markAsEnabledBucket(ServerBucket bucket);

    public boolean isEmpty(ServerBucket bucket);

    public String getBucketMetadataDirPath(ServerBucket bucket);
    
    public String getBucketObjectDataDirPath(ServerBucket bucket);

    public String getBucketWorkDirPath(ServerBucket bucket); 

    public String getBucketCacheDirPath(ServerBucket bucket);
    
    public void cleanUpWorkDir(ServerBucket bucket);

    public void cleanUpCacheDir(ServerBucket bucket);

    
    
    /**
     * ---------------------- ObjectMetadata (head) ----------------------
     */

    public boolean existsObjectMetadata(ServerBucket bucket, String objectName);

    public boolean existsObjectMetadata(ObjectMetadata meta);

    public void markAsDeletedObject(ServerBucket bucket, String objectName);

    public String getObjectMetadataDirPath(ServerBucket bucket, String objectName);

    public ObjectMetadata getObjectMetadata(ServerBucket bucket, String objectName);

    public void deleteObjectMetadata(ServerBucket bucket, String objectName);

    public void saveObjectMetadata(ObjectMetadata meta);

    public File getObjectMetadataFile(ServerBucket bucket, String objectName);

    public void putObjectMetadataFile(ServerBucket bucket, String objectName, File metaFile) throws IOException;;


    /** ObjectMetadata. Version --- */
    
    public void saveObjectMetadataVersion(ObjectMetadata meta);

    public ObjectMetadata getObjectMetadataVersion(ServerBucket bucket, String objectName, int version);

    public File getObjectMetadataVersionFile(ServerBucket bucket, String objectName, int version);

    public void putObjectMetadataVersionFile(ServerBucket bucket, String objectName, int version, File metaFile) throws IOException;

}
