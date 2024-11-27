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
import io.odilon.model.RedundancyLevel;
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
	public ServerBucket 	createBucket(String bucketName);
	public ServerBucket 	renameBucket(ServerBucket bucket, String newBucketName);
	public void 			deleteBucket(ServerBucket bucket);
	public boolean 			isEmpty(ServerBucket bucket);
	
	/**
	 * Object get/ put / delete
	 */
	public ObjectMetadata 	getObjectMetadata(ServerBucket bucket, String objectName);
	public void 			putObjectMetadata(ObjectMetadata meta);
	public void 			putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType, Optional<List<String>> customTags);
	public void 			putObject(ServerBucket bucket, String objectName, File file);
	
	public VirtualFileSystemObject 		getObject(ServerBucket bucket, String objectName);
	public boolean 			exists(ServerBucket bucket, String objectName);
	public InputStream 		getInputStream(ServerBucket bucket, String objectName) throws IOException;
	public void 			delete(ServerBucket bucket, String objectName);
	
	/**
	 * Object List
	 */
	public DataList<Item<ObjectMetadata>> listObjects(ServerBucket bucket, Optional<Long> offset, Optional<Integer> pageSize,	Optional<String> prefix, Optional<String> serverAgentId);

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
	public JournalService getJournalService();
	

	
	/** 
	 * Scheduler
	 */
	public void saveScheduler(ServiceRequest request, String queueId);
	public void removeScheduler(ServiceRequest request,  String queueId);
	

	/**
	 * 
	 */
	boolean checkIntegrity(ServerBucket bucket, String objectName, boolean forceCheck); // [A]
	public boolean setUpDrives();
	
	/**
	 * ServerInfo
	 */
	public OdilonServerInfo getServerInfo();
	public void setServerInfo(OdilonServerInfo serverInfo);
	public boolean isEncrypt();
	public RedundancyLevel getRedundancyLevel();
	
	
	
	/**
	 * Key
	 */
	public void saveServerMasterKey(byte[] masterKey, byte[] hmac, byte[] iv, byte[] salt);
	public byte[] getServerMasterKey();
	
	// public byte[] getServerAESIV();
	
	
	public LockService getLockService();
	public VirtualFileSystemService getVirtualFileSystemService();
	public List<Drive> getDrivesEnabled();
	public List<ServiceRequest> getSchedulerPendingRequests(String queueId);
	
	
	/**
	 * VERSION CONTROL
	 */
	
	public ObjectMetadata getObjectMetadataPreviousVersion(ServerBucket bucket, String objectName);
	public ObjectMetadata getObjectMetadataVersion(ServerBucket bucket, String objectName, int version); 
	
	public List<ObjectMetadata> getObjectMetadataVersionAll(ServerBucket bucket, String objectName); 

	public InputStream getObjectVersionInputStream(ServerBucket bucket, String objectName, int version); 
	
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta);
	public void deleteBucketAllPreviousVersions(ServerBucket bucket); 

	public void wipeAllPreviousVersions();
	public ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName);
	public boolean hasVersions(ServerBucket bucket, String objectName);

	public void syncObject(ObjectMetadata meta);

	
	
	/**
	 * ERROR 
	 */
	public String objectInfo(ServerBucket bucket);
	
	
 
}