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
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.scheduler.ServiceRequest;

/**
 * 
 * <p>Implementations of this interface are expected to be thread-safe, 
 * and can be safely accessed by multiple concurrent threads.</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface IODriver {

	/**
	 * Bucket
	 */
	public ODBucket 	createBucket(String bucketName);
	public ODBucket 	renameBucket(ODBucket bucket, String newBucketName);
	public void 		deleteBucket(ODBucket bucket);
	public boolean 		isEmpty(ODBucket bucket);
	
	/**
	 * Object get/ put / delete
	 */
	public ObjectMetadata 	getObjectMetadata(ODBucket bucket, String objectName);
	public void 			putObjectMetadata(ObjectMetadata meta);
	public void 			putObject(ODBucket bucket, String objectName, InputStream stream, String fileName, String contentType);
	public void 			putObject(ODBucket bucket, String objectName, File file);
	
	public VFSObject 		getObject(ODBucket bucket, String objectName);
	public boolean 			exists(ODBucket bucket, String objectName);
	public InputStream 		getInputStream(ODBucket bucket, String objectName) throws IOException;
	public void 			delete(ODBucket bucket, String objectName);
	
	/**
	 * Object List
	 */
	public DataList<Item<ObjectMetadata>> listObjects(ODBucket bucket, Optional<Long> offset, Optional<Integer> pageSize,	Optional<String> prefix, Optional<String> serverAgentId);

	/** 
	 * Post Transaction (Async)
	 */
	public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion);
	public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta, int headVersion);
	
	
	/** 
	 * Journal
	 */
	public void saveJournal(VFSOperation op);
	public void removeJournal(String id);
	public void rollbackJournal(VFSOperation op, boolean recoveryMode);
	public List<VFSOperation> getJournalPending(JournalService journalService);

	/** 
	 * Scheduler
	 */
	public void saveScheduler(ServiceRequest request, String queueId);
	public void removeScheduler(ServiceRequest request,  String queueId);
	

	/**
	 * 
	 */
	boolean checkIntegrity(ODBucket bucket, String objectName, boolean forceCheck); // [A]
	public boolean setUpDrives();
	
	/**
	 * ServerInfo
	 */
	public OdilonServerInfo getServerInfo();
	public void setServerInfo(OdilonServerInfo serverInfo);
	
	
	/**
	 * Key
	 */
	public void saveServerMasterKey(byte[] masterKey, byte[] hmac, byte[] salt);
	public byte[] getServerMasterKey();
	
	
	
	public LockService getLockService();
	public VirtualFileSystemService getVFS();
	public List<Drive> getDrivesEnabled();
	public List<ServiceRequest> getSchedulerPendingRequests(String queueId);
	
	
	/**
	 * VERSION CONTROL
	 */
	
	public ObjectMetadata getObjectMetadataPreviousVersion(ODBucket bucket, String objectName);
	public ObjectMetadata getObjectMetadataVersion(ODBucket bucket, String objectName, int version); 
	
	public List<ObjectMetadata> getObjectMetadataVersionAll(ODBucket bucket, String objectName); 

	public InputStream getObjectVersionInputStream(ODBucket bucket, String objectName, int version); 
	
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta);
	public void deleteBucketAllPreviousVersions(ODBucket bucket); 

	public void wipeAllPreviousVersions();
	public ObjectMetadata restorePreviousVersion(ODBucket bucket, String objectName);
	public boolean hasVersions(ODBucket bucket, String objectName);

	public void syncObject(ObjectMetadata meta);
	
	

	
	
	
	
	
 
}