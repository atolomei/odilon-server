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
package io.odilon.vfs.model;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import io.odilon.model.BucketMetadata;
import io.odilon.model.ObjectMetadata;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.vfs.DriveInfo;

/**
 * <p>A Drive or Volume is File System directory that acts 
 * as a storage unit</p>
 * 
 * <p>a Drive is <b>not</b> ThreadSafe, 
 * concurrency control is the responsibility of whoever uses its methods.</p> 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface Drive {

	/** -----------------
	* Drive Info  
	------------------*/
	public DriveInfo getDriveInfo();
	public void setDriveInfo(DriveInfo info);

	 public void cleanUpWorkDir(String name);
	 public void cleanUpCacheDir(String name);
	 

	/** -----------------
	 *  Journal 
	------------------*/				
	public void saveJournal(VFSOperation op);
	public void removeJournal(String id);

	/** -----------------
	 *  Sys 
	 ------------------*/
	 public void putSysFile(String fileName, String content);
	 public File getSysFile(String fileName);
	 public void removeSysFile(String fileName);
	 
	 /** -----------------
	  *  Info
	  ------------------*/
	public String getRootDirPath();
	public String getSysDirPath();
	public String getBucketsDirPath();

	public String getWorkDirPath();
	String getJournalDirPath();
	String getTempDirPath();
	public String getSchedulerDirPath();
	
	public String ping();
	public long getTotalSpace();
	public long getAvailableSpace();
	public String getName();
	
	/** order in the rootDirs variable in odilon.properties */
	public int getConfigOrder();
	
	/** -----------------
	  *  Scheduler
   ------------------*/
	public void saveScheduler(ServiceRequest serviceRequest, String queueId);
	public void removeScheduler(ServiceRequest serviceRequest, String queueId);
	public List<File> getSchedulerRequests(String queueId);

	
	/** -----------------
	 * Bucket
	 ------------------*/
	public File createBucket(String bucketName, BucketMetadata meta) throws IOException; 	
	public boolean existsBucket(String bucketName);
	public void deleteBucket(String bucketName); 											
	public List<DriveBucket> getBuckets();
	public void markAsDeletedBucket(String bucketName);
	public void markAsEnabledBucket(String bucketName);
	public boolean isEmpty(String bucketName); 												
	public String getBucketMetadataDirPath(String bucketName);
	public String getBucketObjectDataDirPath(String bucketName);
	public String getBucketWorkDirPath(String bucketName);

	/** -----------------
	 *  ObjectMetadata 
		------------------*/
    public boolean existsObjectMetadata(String bucketName, String objectName);
    public void markAsDeletedObject(String name, String objectName);
    public String getObjectMetadataDirPath(String bucketName, String objectName); 
    public ObjectMetadata getObjectMetadata(String bucketName, String objectName);
	public void saveObjectMetadata(ObjectMetadata meta, boolean isHead);
	public void deleteObjectMetadata(String bucketName, String objectName);
	
	public File getObjectMetadataFile(String bucketName, String objectName);
	public void putObjectMetadataFile(String bucketName, String objectName, File metaFile) throws IOException;;
	 
	 /** ObjectMetadata. Version ---*/
	 public ObjectMetadata getObjectMetadataVersion(String bucketName, String objectName, int version);
	 public File getObjectMetadataVersionFile(String bucketName, String objectName, int version);
	 public void putObjectMetadataVersionFile(String bucketName, String objectName, int version, File metaFile) throws IOException;
	 public String getCacheDirPath();
	 public String getBucketCacheDirPath(String bucketName);
	
	
	
	
	/** Object. Data File Version */
	/** ----------------
  	* Object. Data File
     public void deleteObject(String bucketName, String objectName); // TBA
     public InputStream getObjectInputStream	(String bucketName, String objectName);
     public File putObjectStream				(String bucketName, String objectName, InputStream stream) throws IOException;
     public void putObjectDataFile				(String bucketName, String objectName, File objectFile) throws IOException;
	 public File getObjectDataFile				(String bucketName, String objectName);
	 public File getObjectDataVersionFile		(String bucketName, String objectName, int version);
     public void putObjectDataVersionFile		(String bucketName, String objectName, int version, File objectFile) throws IOException;
     ------------------*/
	
	
}	
	
	
