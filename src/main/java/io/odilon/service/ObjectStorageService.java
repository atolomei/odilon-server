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
package io.odilon.service;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import io.odilon.encryption.EncryptionService;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SystemInfo;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VFSObject;

/**
 *
 *
 *  @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface ObjectStorageService extends SystemService {

	/** -------------------
	* Bucket
	* -------------------*/
	public ServerBucket findBucketName(String bucketName);
	public void deleteBucketByName(String bucketName);
	public List<ServerBucket> findAllBuckets();
	
	public ServerBucket createBucket(String bucketName);
	public ServerBucket updateBucketName(ServerBucket bucket, String newBucketName);
	
	
	public boolean existsBucket(String bucketName);
	public void forceDeleteBucket(String bucketName);
	public boolean isEmptyBucket(String bucketName);
	
	/** Object version - delete (normally to free disk) */
	public void deleteBucketAllPreviousVersions(String bucketName);
	
	/** -------------------
	* Object
	* -------------------*/
	public void putObject(String bucketName, String objectName, File file);
	public void putObject(String bucketName, String objectName, InputStream stream, String fileName, String contentType);
	public void putObject(String bucketName, String objectName, InputStream stream, String fileName, String contentType, Optional<List<String>> customTags);
	
	public VFSObject getObject(String bucketName, String objectName);
	public ObjectMetadata getObjectMetadata(String bucketName, String objectName);
	
	/** Object version - get */
	
	public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName);
	public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName, int version); 
	public List<ObjectMetadata> getObjectMetadataAllPreviousVersions(String bucketName, String objectName);
	
	public InputStream getObjectPreviousVersionStream(String bucketName, String ObjectName, int version);
	public InputStream getObjectStream(String bucketName, String objectName);
	
	
	/** Object version - delete/restore */
	
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta);
	public ObjectMetadata restorePreviousVersion(String bucketName, String objectName); 
	
	/** -------------------
	* Query
	* -------------------*/
	public DataList<Item<ObjectMetadata>> listObjects(String bucketName, Optional<Long> offset, Optional<Integer> pageSize, Optional<String> prefix, Optional<String> serverAgentId);
	
	public boolean existsObject(String bucketName, String objectName);
	public void deleteObject(String bucketName, String objectName);
	
	/** -------------------
	* System
	* -------------------*/
	public void wipeAllPreviousVersions();
	
	
	/** -------------------
	* Settings
	* -------------------*/
	public ServerSettings getServerSettings();
	public EncryptionService getEncryptionService();

	public String ping();
	public boolean isEncrypt();
	public SystemInfo getSystemInfo();
	public boolean hasVersions(String bucketName, String objectName);
	
}
