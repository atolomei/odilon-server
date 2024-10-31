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
package io.odilon.vfs;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jakarta.annotation.PostConstruct;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.OdilonApplication;
import io.odilon.cache.FileCacheService;
import io.odilon.cache.ObjectMetadataCacheService;
import io.odilon.encryption.EncryptionService;
import io.odilon.encryption.MasterKeyService;
import io.odilon.encryption.OdilonKeyEncryptorService;
import io.odilon.error.OdilonServerAPIException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.query.BucketIteratorService;
import io.odilon.replication.ReplicationService;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.service.util.ByteToString;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveBucket;
import io.odilon.vfs.model.DriveStatus;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.VFSOperation;

import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.IODriver;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VirtualFileSystemService;
import io.odilon.vfs.raid0.RAIDZeroDriver;
import io.odilon.vfs.raid1.RAIDOneDriver;
import io.odilon.vfs.raid6.RAIDSixDriver;
				
/**
 * <p>
 * This class is the implementation of the {@link VirtualFileSystemService}) interface. 
 * It manages the underlying layer that may be</p> 
 * <ul>
 * <li><b>RAID 0</b> {@link RAIDZeroDriver}</li>
 * <li><b>RAID 1</b> {@link RAIDOneDriver}</li>
 * <li><b>RAID 6 / Erasure Coding</b>{@link RAIDSixDriver}</li> 
 * </ul>
 * 
 * <p>Buckets are maintained in a RAM cache by the {@link VirtualFileSystemService}.
 *  INVARIANT: All {@link Drive} must have all buckets</p>
 * 
 * <p>Status: Stopped ->  Starting -> Running -> Stopping</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
@Service
public class OdilonVirtualFileSystemService extends BaseService implements VirtualFileSystemService, ApplicationContextAware {
		
	static private Logger logger = Logger.getLogger(OdilonVirtualFileSystemService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
					
	@JsonIgnore
	@Autowired
	private final SchedulerService schedulerService;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	@JsonIgnore
	@Autowired
	private final EncryptionService encrpytionService;

	@JsonIgnore
	@Autowired
	private final SystemMonitorService monitoringService;

	@JsonIgnore
	@Autowired
	private final ObjectMetadataCacheService objectCacheService;
	
	@JsonIgnore
	@Autowired
	private final LockService vfsLockService;
	
	@JsonIgnore
	@Autowired
	private final JournalService journalService;

	@JsonIgnore
	@Autowired
	private final BucketIteratorService bucketIteratorService;
	
	@JsonIgnore
	@Autowired
	private final ReplicationService replicationService;
	
	@JsonIgnore
	@Autowired							
	private final MasterKeyService masterKeyEncryptorService;

	@JsonIgnore
	@Autowired							
	private final OdilonKeyEncryptorService odilonKeyEncryptorService;
	
	@Autowired
	@JsonIgnore
    private final ApplicationEventPublisher applicationEventPublisher;
	
	
	/** 
	 * <p>Cache of decoded {@link File} in File System, used only in <b>RAID 6</b></p>
	 * <p>If the server has encryption.enabled the Files in cache are decoded but encrypted.</p> 
	 * */
	@JsonIgnore
	@Autowired
	private final FileCacheService fileCacheService;

	@JsonIgnore
	ReentrantReadWriteLock bucketCacheLock = new ReentrantReadWriteLock();
	
	@JsonIgnore
    private AtomicLong bucket_id;

	
	/** All Drives, either {@link DriveStatus.ENABLED} or 
	 * {@link DriveStatus.NOT_SYNC}(ie. in the sync process 
	 * to become {@link DriveStatus.ENABLED}*/
	@JsonIgnore
	private Map<String, Drive> drivesAll = new ConcurrentHashMap<String, Drive>();
	
	/** Drives in status {@link DriveStatus.ENABLED} */
	@JsonIgnore						
	private Map<String, Drive> drivesEnabled = new ConcurrentHashMap<String, Drive>();

	/** Drives available to be used to decode by {@RAIDSixDecoder} in RAID 6*/
	@JsonIgnore
	private final Map<Integer, Drive> drivesRSDecode = new ConcurrentHashMap<Integer, Drive>();
	
	@JsonIgnore
	private ApplicationContext applicationContext;
	
	@JsonProperty("started")
	private final OffsetDateTime started  = OffsetDateTime.now();
	
	@JsonProperty("raid")
	private final RedundancyLevel raid;

	/**
	 * <p>this map contains accessible buckets, not  
	 * marked as {@code BucketState.DELETED} or {@code BucketState.DRAFT}
	 * </p>
	 */
	@JsonIgnore						
	private Map<String, ServerBucket> buckets_by_name = new ConcurrentHashMap<String, ServerBucket>();

	@JsonIgnore
	private Map<Long, ServerBucket> buckets_by_id = new ConcurrentHashMap<Long, ServerBucket>();
	
	
	private Map<String, ServerBucket> getBucketsByNameMap() {
		return buckets_by_name;
	}
	
	private Map<Long, ServerBucket> getBucketsByIdMap() {
		return buckets_by_id;
	}
	
	
	/**
	 * 
	 * @param serverSettings
	 * @param montoringService
	 * @param encrpytionService
	 * @param vfsLockService
	 * @param journalService
	 * @param schedulerService
	 * @param walkerService
	 * @param replicationService
	 * 
	 */
	@Autowired
	public OdilonVirtualFileSystemService(  ServerSettings serverSettings, 
										SystemMonitorService montoringService,
										EncryptionService encrpytionService,
										LockService  vfsLockService,
										JournalService journalService,
										SchedulerService schedulerService,
										BucketIteratorService walkerService,
										ReplicationService replicationService,
										ObjectMetadataCacheService objectCacheService,
										MasterKeyService masterKeyEncryptorService,
										OdilonKeyEncryptorService odilonKeyEncryptorService,
										FileCacheService fileCacheService,
										ApplicationEventPublisher applicationEventPublisher) {
		
		this.fileCacheService=fileCacheService;
		this.objectCacheService=objectCacheService;
		this.vfsLockService=vfsLockService;
		this.serverSettings=serverSettings;
		this.monitoringService=montoringService;
		this.encrpytionService=encrpytionService;
		this.journalService=journalService;
		this.schedulerService=schedulerService;
		this.bucketIteratorService=walkerService;
		this.raid=serverSettings.getRedundancyLevel();
		this.replicationService=replicationService;
		this.masterKeyEncryptorService = masterKeyEncryptorService;
		this.odilonKeyEncryptorService=odilonKeyEncryptorService;
		this.applicationEventPublisher = applicationEventPublisher;
		
	}
	
	
	@Override
	public Long getNextBucketId() {
		return Long.valueOf(bucket_id.addAndGet(1));
	}

	
	@Override
	public byte [] HMAC(byte [] data, byte [] key)  throws NoSuchAlgorithmException, InvalidKeyException {
		final String algorithm = "HmacSHA256";
		SecretKeySpec secretKeySpec = new SecretKeySpec(key, algorithm);
			    Mac mac = Mac.getInstance(algorithm);
			    mac.init(secretKeySpec);
			    return mac.doFinal(data);
	}
	
	
	@Override
	public List<ObjectMetadata> getObjectMetadataAllVersions(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		
		return createVFSIODriver().getObjectMetadataVersionAll(getBucketsByNameMap().get(bucketName), objectName); 
		
	}
	
	@Override
	public ObjectMetadata getObjectMetadataVersion(String bucketName, String objectName, int version) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);

		return createVFSIODriver().getObjectMetadataVersion(getBucketsByNameMap().get(bucketName), objectName, version);
	}
	
	@Override
	public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		
		return createVFSIODriver().getObjectMetadataPreviousVersion(getBucketsByNameMap().get(bucketName), objectName);
	}
	
	@Override
	public InputStream getObjectVersion(String bucketName, String objectName, int version) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(version>=0, "version must be >=0");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		
		return createVFSIODriver().getObjectVersionInputStream(getBucketsByNameMap().get(bucketName), objectName, version);
	}
	
	
	@Override
	public boolean hasVersions(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		
		return createVFSIODriver().hasVersions(getBucketsByNameMap().get(bucketName), objectName);
	}
				
	@Override
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta can not be null or empty");
		createVFSIODriver().deleteObjectAllPreviousVersions(meta);
	}
	
	@Override
	public void deleteBucketAllPreviousVersions(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		
		createVFSIODriver().deleteBucketAllPreviousVersions(getBucketsByNameMap().get(bucketName));
	}
	
	@Override
	public void wipeAllPreviousVersions() {
		createVFSIODriver().wipeAllPreviousVersions();
	}

	@Override
	public ObjectMetadata restorePreviousVersion(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		
		ServerBucket bucket = getBucketsByNameMap().get(bucketName);
		Check.requireNonNullArgument(bucket, "bucket can not be null or empty");
		
		return createVFSIODriver().restorePreviousVersion(bucket, objectName);
	}

	/**
	 * <p>Creates the bucket folder in every {@link Drive}</p>
	 * <p>if the bucket does not exist:
	 * creates the bucket
	 * rollback -> delete the bucket</p>
	 * <p>if the bucket exists mark as deleted</p>
	 */
	@Override
	public ServerBucket createBucket(String bucketName) throws IOException {
	
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(!getBucketsByNameMap().containsKey(bucketName), "bucket already exist | b: " + bucketName);

		if (!bucketName.matches(SharedConstant.bucket_valid_regex)) 
			throw new IllegalArgumentException("bucketName contains invalid character | regular expression is -> " + SharedConstant.bucket_valid_regex +" |  b:" + bucketName);

		return createVFSIODriver().createBucket(bucketName);
	}
	
	
	
	@Override
	public ServerBucket renameBucketName(String bucketName, String newBucketName) {
		
											
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(newBucketName, "newBucketName can not be null or empty");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		
		if (!newBucketName.matches(SharedConstant.bucket_valid_regex)) 
			throw new IllegalArgumentException("bucketName contains invalid character | regular expression is -> " + SharedConstant.bucket_valid_regex +" |  b:" + newBucketName);
		
		if (this.existsBucket(newBucketName))
			throw new IllegalArgumentException( "bucketName already used " + newBucketName);
		
		return createVFSIODriver().renameBucket(getBucketByName(bucketName), newBucketName);
	}
	
	/**
	 * <p>Deletes the bucket folder in every Drive
	 * mark as deleted on all drives
	 * rollback -> mark as enabled
	 * </p>
	 */
	@Override
	public void removeBucket(ServerBucket bucket) {
		removeBucket(bucket, false);
	}

	/**
	 * 
	 */
	@Override
	public void removeBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		removeBucket(getBucketsByNameMap().get(bucketName), false);
	}
	
	/**
	 * 
	 */
	@Override
	public void forceRemoveBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		removeBucket(getBucketsByNameMap().get(bucketName), true);
	}
	
	/**
	 * 
	 */
	@Override
	public boolean existsBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		return getBucketsByNameMap().containsKey(bucketName);
	}
	
	/**
	 * 
	 */
	@Override
	public boolean isEmptyBucket(String bucketName) {
		Check.requireNonNullArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist or is not Accesible | b: " + bucketName);
		return isEmpty(getBucketsByNameMap().get(bucketName));
	}
	
	/**
	 * 
	 */
	@Override
	public boolean isEmpty(ServerBucket bucket) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist or is not Accesible | b: " + bucket.getName());
		return createVFSIODriver().isEmpty(bucket);
	}
	
	/**
	 * 
	 */
	@Override
	public void putObject(ServerBucket bucket, String objectName, File file) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist b: " + bucket.getName());
		createVFSIODriver().putObject(bucket, objectName, file);
	}

	@Override
	public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType) {
			putObject(bucket, objectName, stream, fileName, contentType, Optional.empty());
	}

	/**
	 * 
	 */
	@Override
	public void putObject(String bucketName, String objectName, InputStream is, String fileName, String contentType, Optional<List<String>> customTags) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullArgument(objectName, "objectName can not be null -> b:" + bucketName);
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		putObject(getBucketsByNameMap().get(bucketName), objectName, is, fileName, contentType, customTags);
	}

	/**
	 * 
	 */
	@Override
	public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType, Optional<List<String>> customTags) {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireNonNullArgument(objectName, "objectName can not be null -> b:" + bucket.getName());
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		createVFSIODriver().putObject(bucket, objectName, stream, fileName, contentType, customTags);
	}

	/**
	 * 
	 */
	@Override
	public VFSObject getObject(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireNonNullArgument(objectName, "objectName can not be null -> b:" + bucket.getName());
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		return createVFSIODriver().getObject(bucket,  objectName);
	}

	/**
	 * 
	 */
	@Override
	public VFSObject getObject(String bucketName, String objectName) {
		
		Check.requireNonNullArgument(bucketName, "bucketName can not be null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		
		ServerBucket bucket = getBucketsByNameMap().get(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist");
		
		return createVFSIODriver().getObject(bucket,  objectName);
	}
	
	/**
	 * <p>
	 * <pre>IMPORTANT</pre> 
	 * caller must close the {@link InputStream} returned
	 * </p>
	 * @throws IOException 
	 */
	@Override
	public InputStream getObjectStream(ServerBucket bucket, String objectName) throws IOException {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucket.getName());
		
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());

		return createVFSIODriver().getInputStream(bucket, objectName);
	}

	/**
	 * <p>
	 * <pre>IMPORTANT</pre>
	 * caller must close the {@link InputStream} returned
	 * </p>
	 * @throws IOException
	 */
	@Override		
	public InputStream getObjectStream(String bucketName, String objectName) throws IOException {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return getObjectStream(getBucketsByNameMap().get(bucketName), objectName);
	}
	
	/**
	 * 
	 */
	@Override																		
	public DataList<Item<ObjectMetadata>> listObjects(String bucketName) {
	return 	listObjects(bucketName,Optional.empty(),Optional.empty(),Optional.empty(),Optional.empty());
	}
	
	/**
	 */
	@Override
	public DataList<Item<ObjectMetadata>> listObjects(String bucketName, Optional<Long> offset, Optional<Integer> pageSize,	Optional<String> prefix, Optional<String> serverAgentId) {
		
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		ServerBucket bucket = getBucketsByNameMap().get(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist | b: " + bucketName);

		return createVFSIODriver().listObjects(bucket, offset, pageSize, prefix, serverAgentId);
	}
	
	/**
	 * <p>if the object does not exist or is in state DELETED -> not found</p>
	 */
	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String objectName) {

		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);

		ServerBucket bucket = getBucketsByNameMap().get(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist | b: " + bucketName);

		return createVFSIODriver().getObjectMetadata(bucket, objectName);

	}
	
	/**
	 * 
	 * 
	 */
	@Override					
	public boolean existsObject(String bucketName, String objectName) {
		
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		
		ServerBucket bucket = getBucketsByNameMap().get(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist | b: " + bucketName);
		
		return createVFSIODriver().exists(bucket, objectName);
	}


	@Override					
	public boolean existsObject(Long bucketId, String objectName) {
		
		Check.requireNonNullArgument(bucketId, "bucketId can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketId.toString());
		
		ServerBucket bucket = getBucketsByIdMap().get(bucketId);
		Check.requireNonNullArgument(bucket, "bucket does not exist | b: " + bucketId.toString());
		
		return createVFSIODriver().exists(bucket, objectName);
	}

	
	/**
	 * <p>returns true if the  object exist and is not in state DELETED</p>
	 */
	@Override
	public boolean existsObject(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucket.getName());
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		return createVFSIODriver().exists(bucket, objectName);
	}
	
	/**
	 * 
	 */
	@Override
	public ServerBucket getBucketByName(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return this.getBucketsByNameMap().get(bucketName);
	}
	
	@Override
	public ServerBucket getBucketById(Long id) {
		Check.requireNonNullArgument(id, "id can not be null");
		Check.requireTrue(getBucketsByIdMap().containsKey(id), "bucket does not exist | b: " + id.toString());
		return this.getBucketsByIdMap().get(id);
	}
	
	
	/**
	 * 
	 */
	@Override
	public void deleteObject(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		createVFSIODriver().delete(getBucketsByNameMap().get(bucketName), objectName);
	}
	
	/**
	 * 
	 */
	@Override
	public void deleteObject(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucket.getName());
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist -> b:"  + bucket.getName());
		createVFSIODriver().delete(bucket, objectName);
	}
	
	/**
	 * 
	 */
	@Override
	public Map<String, Drive> getMapDrivesEnabled() {
		return this.drivesEnabled;
	}
	
	/**
	 * 
	 */
	@Override
	public Map<String, Drive> getMapDrivesAll() {
		return this.drivesAll;
	}

	/**
	 * 
	 */
	@Override
	public Map<Integer, Drive> getMapDrivesRSDecode() {
		return this.drivesRSDecode;
	}
	
	public synchronized void updateDriveStatus(Drive drive) {
		
		if (drive.getDriveInfo().getStatus()==DriveStatus.ENABLED) {
			getMapDrivesEnabled().put(drive.getName(), drive);
			getMapDrivesRSDecode().put(Integer.valueOf(drive.getDriveInfo().getOrder()), drive);
		}
		else {
			getMapDrivesEnabled().remove(drive.getName(), drive);
			getMapDrivesRSDecode().remove(Integer.valueOf(drive.getDriveInfo().getOrder()), drive);
		}
	}
	
	/**
	 *  @return whether encryption is enabled
	 */
	@Override					
	public boolean isEncrypt() {
		return getServerSettings().isEncryptionEnabled();
	}
	
	/**
	 * @return whether new files should be encrypted using Vault. 
	 * If vault is not enabled this flag is not used
	 */
	@Override					
	public boolean isUseVaultNewFiles() {
		return getServerSettings().isUseVaultNewFiles();
	}
	
	@Override
	public EncryptionService getEncryptionService() {
		return  this.encrpytionService;
	}

	@Override
	public JournalService getJournalService() {
		return  this.journalService;
	}

	@Override
	public FileCacheService getFileCacheService() {
		return  this.fileCacheService;
	}

	@Override
	public ObjectMetadataCacheService getObjectMetadataCacheService() {
		return this.objectCacheService;
	}
	
	@Override
	public RedundancyLevel getRedundancyLevel() {
		return this.raid;
	}

	@Override
	public IODriver createVFSIODriver() {
												
		if (this.raid==RedundancyLevel.RAID_0) return getApplicationContext().getBean(RAIDZeroDriver.class, this, getLockService());
		if (this.raid==RedundancyLevel.RAID_1) return getApplicationContext().getBean(RAIDOneDriver.class, this, getLockService());
		if (this.raid==RedundancyLevel.RAID_6) return getApplicationContext().getBean(RAIDSixDriver.class, this, getLockService());

		throw new IllegalStateException("RAID not supported -> " + this.raid.toString());
	}

	@Override
	public List<VFSOperation> getJournalPendingOperations() {
		return createVFSIODriver().getJournalPending(getJournalService());
	}
	
	@Override
	public List<ServiceRequest> getSchedulerPendingRequests(String queueId) {
		return createVFSIODriver().getSchedulerPendingRequests(queueId);
	}
	
	@Override
	public void saveScheduler(ServiceRequest request, String queueId) {
		 Check.requireNonNullArgument(request, "request can not be null");
		 createVFSIODriver().saveScheduler(request, queueId);
	}
	
	@Override
	public void removeScheduler(ServiceRequest request, String queueId) {
		Check.requireNonNullArgument(request, "request can not be null");
		createVFSIODriver().removeScheduler(request, queueId);
	}
	
	@Override
	public void removeJournal(String id) {
		Check.requireNonNullArgument(id, "id can not be null");
		createVFSIODriver().removeJournal(id);
	}
	
	@Override
	public void saveJournal(VFSOperation op) {
		Check.requireNonNullArgument(op, "op is null");
		 createVFSIODriver().saveJournal(op);
	}
	@Override
	public OffsetDateTime getStarted() { 
		return started;
	}

	@Override
	public List<ServerBucket> listAllBuckets() {
			List<ServerBucket> list = new ArrayList<ServerBucket>();
			Map<String, ServerBucket> map = new HashMap<String, ServerBucket>();
			for (Entry<String, Drive> entry: getMapDrivesEnabled().entrySet()) {
				List<DriveBucket> db = entry.getValue().getBuckets();
				db.forEach( item -> map.put(item.getName(), new OdilonBucket(item)));
			}
			map.forEach( (k,v) -> list.add(v));
			return list;
	}

	@Override
	public boolean checkIntegrity(String bucketName, String objectName, boolean forceCheck) {
									
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		ServerBucket bucket = getBucketsByNameMap().get(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist b:" + bucketName);
		
		return createVFSIODriver().checkIntegrity(bucket, objectName, forceCheck);
	}
	
	@Override
	public BucketIteratorService getBucketIteratorService() {
		return bucketIteratorService;
	}

	@Override
	public String ping() {
		StringBuilder str = new StringBuilder();
		this.drivesEnabled.forEach( (k,d) -> 
			{ 	if (!d.ping().equals("ok")) {
						if (str.length()>0)
							str.append(" | ");	
						str.append(k+"->"+d.ping());
				}
			}
		);
		return str.length()>0?str.toString():"ok";
	}

	@Override
	public SchedulerService getSchedulerService() {
		return this.schedulerService;
	}
	
	@Override
	public ServerSettings getServerSettings() {
		return serverSettings;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName() +"{");
		str.append(toJSON());
		str.append("}");
		return str.toString();
	}

	@Override
	public String toJSON() {
		StringBuilder str = new StringBuilder();
			str.append("\"redundancy\":" + (Optional.ofNullable(raid).isPresent() ? ("\""+raid.getName()+"\"") :"null"));
			str.append(", \"drive\":[");
			int n=0;
			for (Entry<String, Drive> entry : getMapDrivesEnabled().entrySet()) {
					str.append( (n>0?", ":"") + "{\"name\":\"" + entry.getKey() + "\", \"mount\": \"" + entry.getValue().getRootDirPath() + "\"}");
					n++;
			}
			str.append("]");
		return str.toString();
	}
	

	@Override
	public ApplicationContext getApplicationContext()  {
		return this.applicationContext;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
	}
	
	/**
	 * <p>if the object does not exist or is in state {@link ObjectStatus#DELETE} -> not found</p>
	 */
	@Override
	public OdilonServerInfo getOdilonServerInfo() {
		return createVFSIODriver().getServerInfo();
	}
	
	@Override
	public void setOdilonServerInfo(OdilonServerInfo serverInfo) {
		Check.requireNonNullArgument(serverInfo, "serverInfo can not be null");
		createVFSIODriver().setServerInfo(serverInfo);
	}
	
	@Override
	public ReplicationService getReplicationService() {
		return replicationService;
	}


	@Override
	public MasterKeyService getMasterKeyEncryptorService() {
		return masterKeyEncryptorService;
	}
	
	@Override
	public SystemMonitorService getSystemMonitorService() {
		return  monitoringService;
	}
	
	@Override
	public LockService getLockService() {
		return this.vfsLockService;
	}

	
	public ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}
	
		
	/**
	 * 
	 * 
	 */
	@PostConstruct
	protected void onInitialize() {		
		
		synchronized (this) {
			setStatus(ServiceStatus.STARTING);
			try {
				
				lazyInjection();
				loadDrives();
				
				startuplogger.info(ServerConstant.SEPARATOR);
				
				/**	load master key	*/
				loadMasterKey();
				
				/** Starts up Scheduler */
				getSchedulerService().start();
				
				/** process unfinished transactions */
				processJournalQueue(true);

				/**
				*  CONSISTENCY
				*  -----------	
				*  after completing all unfinished trx ->
				*  buckets "marked as deleted" on a drive, must be purged from that drive
				**/
				deleteGhostsBuckets();
				loadBuckets();
				checkDriveBuckets();
				setUpNewDrives();
				checkServerInfo();
				cleanUpWorkDir();
				
				if (getSchedulerService().getStandardQueueSize()>0) {
					try {
						Thread.sleep(1000 * 3);
					} catch (Exception e) {
						logger.error(e, SharedConstant.NOT_THROWN);
					}
				}

				setStatus(ServiceStatus.RUNNING);

				
				} catch (Exception e) {
					setStatus(ServiceStatus.STOPPED);
					throw e;
				}
		}
	}

	/**
	 * 
	 */
	private void loadDrives() {
			
		List<Drive> baselist = new ArrayList<Drive>();
		
		
		
		/** load enabled drives and new drives */
		{	
			int configOrder=0;
			
			this.drivesAll.clear();
			this.drivesEnabled.clear();
			this.drivesRSDecode.clear();

			for (String dir: getServerSettings().getRootDirs()) {

				Drive drive = null;
				
				if (getServerSettings().getRedundancyLevel()==RedundancyLevel.RAID_6) {
					drive=new OdilonDrive(String.valueOf(configOrder), dir, configOrder);
					configOrder++;
				}
				else {
					drive=new OdilonSimpleDrive(String.valueOf(configOrder), dir, configOrder);
					configOrder++;
				}
				
				baselist.add(drive);
				
				drivesAll.put(drive.getName(), drive);
				
				if (drive.getDriveInfo().getStatus()==DriveStatus.ENABLED)
					drivesEnabled.put(drive.getName(), drive);
				
			}
		}	
		
		/** check if the installation is new, mark all drives as ENABLED */
		{
			boolean noneSync = true;
			for (Entry<String,Drive> entry: drivesAll.entrySet()) {
				Drive drive=entry.getValue();
				if (drive.getDriveInfo().getStatus()!=DriveStatus.NOTSYNC) {
					noneSync = false;
					break;
				}
			}
			if (noneSync) {
				drivesEnabled.clear();
				for (Drive drive: baselist) {
					DriveInfo info = drive.getDriveInfo();
					info.setOrder(drive.getConfigOrder());
					info.setStatus(DriveStatus.ENABLED);
					drive.setDriveInfo(info);
					drivesEnabled.put(drive.getName(), drive);
				}
			}
		}
		
		/** set up drives for RS Decoding */
		drivesEnabled.values().forEach( drive -> this.drivesRSDecode.put(Integer.valueOf(drive.getDriveInfo().getOrder()), drive));
	}
	
	/**
	 * 
	 * 
	 */
	private void deleteGhostsBuckets() {
		for (Entry<String,Drive> entry: getMapDrivesAll().entrySet()) {
			Drive drive=entry.getValue();
			List<DriveBucket> buckets = drive.getBuckets();
			for (DriveBucket driveBucket: buckets) {
				if (driveBucket.isDeleted()) {
					logger.debug("Deleting ghost bucket -> b:" + driveBucket.getName() + " d:" + drive.getName());
					((OdilonDrive) drive).forceDeleteBucket(driveBucket.getId());
				}
			}
		}
	}
	

	 
	@Override
	public void updateBucketCache(String oldBucketName, ServerBucket bucket) {
		
		this.bucketCacheLock.writeLock().lock();
		try {
			this.getBucketsByNameMap().remove(oldBucketName);
			this.getBucketsByNameMap().put(bucket.getName(), bucket);
			this.getBucketsByIdMap().put(bucket.getId(), bucket);
		} finally {
			this.bucketCacheLock.writeLock().unlock();
		}
		
	}
	
	
	@Override
	public void addBucketCache(ServerBucket bucket) {
		
		this.bucketCacheLock.writeLock().lock();
		try {
			this.getBucketsByIdMap().put(bucket.getId(), bucket);
			this.getBucketsByNameMap().put(bucket.getName(), bucket);
		} finally {
			this.bucketCacheLock.writeLock().unlock();
		}
	}

	
	private void removeBucketCache(ServerBucket bucket) {
		
		this.bucketCacheLock.writeLock().lock();
		try {
			this.getBucketsByIdMap().remove(bucket.getId());
			this.getBucketsByNameMap().remove(bucket.getName());
		} finally {
			this.bucketCacheLock.writeLock().unlock();
		}
	}
	
	
	/**
	 * 
	 */
	private synchronized void loadBuckets() {
		
		Map<String, ServerBucket> map = ((BaseIODriver) createVFSIODriver()).getBucketsMap();
		
		if (map.size()==0) 
			startuplogger.debug("Adding buckets to cache -> ok (no buckets)");
		
		else  {
			
			map.forEach((k,v) -> buckets_by_name.put(k,v));
			map.forEach((k,v) -> buckets_by_id.put(v.getId(),v));
			
			startuplogger.debug("Adding buckets to cache -> ok (" + String.valueOf(map.size()) + ")");
		}
		
		map.values().forEach( bucket -> {
											if (bucket.getId()==null)
												bucket.getBucketMetadata().id=Long.valueOf(0);
										});
		
		if (map.size()>0)
			bucket_id = new AtomicLong(map.values().stream().mapToLong(value -> value.getId()).max().orElseThrow(IllegalStateException::new));
		
		
		if ((bucket_id==null) || (bucket_id.get()<1))
			bucket_id = new AtomicLong(0);
		
		map.values().forEach( bucket -> {
			if (bucket.getId().longValue()==0) {
				bucket.getBucketMetadata().id= getNextBucketId();
				// save bucket !
				//
				
				logger.debug("save bucket -> " + bucket.getName());
				//createIODriver()
			}
		});
		
		//map.values().forEach( bucket -> {
		//	logger.debug( bucket.getBucketMetadata().bucketName +" -> " + bucket.getBucketMetadata().getId().longValue());
		//});
	}
	
	
	/**
	 * <p>deletes the folder in every Drive</p>
	 */
	private void removeBucket(ServerBucket bucket, boolean forceDelete) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(getBucketsByNameMap().containsKey(bucket.getName()), "bucket does not exist -> b:"  + bucket.getName());
		deleteBucketInternal(bucket,forceDelete);
	}

	/**
	 * <p>All drives must have the same directory of buckets</p>
	 */
	private void checkDriveBuckets() {
		try {
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (Entry<String,Drive> entry: getMapDrivesEnabled().entrySet()) {
				Drive drive=entry.getValue();
				List<DriveBucket> folders = drive.getBuckets();
				for (DriveBucket driveBucket: folders) {
					if (driveBucket.isAccesible()) {
						if (map.containsKey(driveBucket.getName())) {
							Integer index = map.get(driveBucket.getName()); 
							index = index + Integer.valueOf(1);
							map.put(driveBucket.getName(), index);
						}
						else {
							 map.put(driveBucket.getName(), Integer.valueOf(1));
						}
					}
				}
			}
			
			int size = getMapDrivesEnabled().size();
			List<String> errors = new ArrayList<String>();
			
			int buckets = map.size();
			
			for (Entry<String, Integer> entry: map.entrySet()) {
				if (entry.getValue()<size) {
					errors.add("Error with Bucket -> " + entry.getKey()  + " | count -> " + entry.getValue() +" / " + size);	
				}
			}
			if (errors.size()>0) {
				errors.forEach(item -> startuplogger.error(item.toString()));
				startuplogger.error("----------------------------------------------------");
				startuplogger.error("Structure Check -> Total errors " + errors.size());
				startuplogger.error("The server is in inconsistent state and can not run");
				startuplogger.error("Exiting");
				startuplogger.error("----------------------------------------------------");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
				}
				((ConfigurableApplicationContext) this.getApplicationContext()).close();
				System.exit(1);
				
				
			} else 	{
				startuplogger.debug("Structure check -> ok (d:" + String.valueOf(size)+ " | b:" + String.valueOf(buckets) + ")"  );
			}
			
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
			throw(e);
		}
	}

	/**
	 * <p>@param bucket bucket must exist in the system</p> 
	 */
	private void deleteBucketInternal(ServerBucket bucket, boolean forceDelete) {
		
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(isEmpty(bucket), "bucket is not Emtpy | b:" + bucket.getName()); 
		
		VFSOperation op = null;
		boolean done = false;
		
		List<Drive> listDrives = null;
		
		try {
			
			getLockService().getBucketLock(bucket.getId()).writeLock().lock();
			
			if (!existsBucket(bucket.getName()))
				throw new IllegalArgumentException("bucket does not exist -> b:" +bucket.getName());
			
			if ((!forceDelete) && (!isEmpty(bucket))) 
				throw new OdilonServerAPIException("bucket must be empty to be deleted -> b:" +bucket.getName());
			
			op = getJournalService().deleteBucket(bucket);
			
			listDrives = new ArrayList<Drive>(getMapDrivesAll().values());
			
			for (Drive drive: listDrives) {
				try {
					drive.markAsDeletedBucket(bucket.getId());
				} catch (Exception e) {
					done=false;
					throw new InternalCriticalException(e);
				}
			}

			/** once the bucket is marked as DELETED 
			 *  on all drives and the TRX commited -> it is gone,
			 *  it can not be restored */
			done = op.commit();
		}
		finally {
			try {
					if (op==null)
						return;
					 
					if (done) {
						/** once the TRX is completed 
						 *  buckets -> all marked as "deleted" or all marked as "enabled".
						 *  TBA this step can be Async
						 */
						
												
						removeBucketCache(bucket);
						
						if (listDrives!=null) {
							for (Drive drive: listDrives) {
								((OdilonDrive) drive).forceDeleteBucket(bucket.getId());
							}		
						}
					}
					else {
						/** rollback restores all buckets */
						createVFSIODriver().rollbackJournal(op, false);
						addBucketCache(bucket);
					}
					
			} catch (Exception e) {
					logger.error(e, SharedConstant.NOT_THROWN);
			}
			finally {
					getLockService().getBucketLock(bucket.getId()).writeLock().unlock();
			}
		}
	}
	
	
	/**
	 * 
	 * 
	 */
	private void setUpNewDrives() {
		
		boolean requireSetupDrives = false;
		List<String> newRoots = new ArrayList<String>();
		
		for (Entry<String,Drive> entry: getMapDrivesAll().entrySet()) {
			Drive drive=entry.getValue();
			if (drive.getDriveInfo().getStatus()==DriveStatus.NOTSYNC) {
				newRoots.add(drive.getRootDirPath());
				requireSetupDrives = true;
			}
		}
		
		if (!requireSetupDrives)
			return;
		
		startuplogger.info("Setting up new drives:");
		newRoots.forEach(item -> startuplogger.info(item));
		startuplogger.info("---------------");
		
		createVFSIODriver().setUpDrives();
	}
	
	/**
	 * 
	 * 
	 */
	private void lazyInjection() {

		
		/** FileCacheService -> lazy injection */
		getFileCacheService().setVFS(this);
		
		/** WalkerService -> lazy injection */
		((io.odilon.query.OdilonBucketIteratorService) getBucketIteratorService()).setVFS(this);

		/** JournalService -> lazy injection */ 
		((OdilonJournalService) getJournalService()).setVFS(this);

		/** ReplicationService -> lazy injection */
		getReplicationService().setVFS(this);
		
		/** SchedulerService -> lazy injection */
		getSchedulerService().setVFS(this);
	}
	
	
	/**
	 * this method must be called when there is only 1 thread accessing 
	 * the object operations
	 * 
	 */
	private synchronized void processJournalQueue(boolean recoveryMode) {

		/** Rollback TRX uncompleted */ 
		List<VFSOperation> list = getJournalPendingOperations();
		if (list==null || list.isEmpty())
			return;
		logger.debug("Processing Journal queue -> " + String.valueOf(list.size()));
		IODriver driver = createVFSIODriver();
		for (VFSOperation op: list)
			driver.rollbackJournal(op, recoveryMode);
	}

	/**
	 * 
	 * 
	 * 
	 */
	private void checkServerInfo() {
		
		IODriver driver = createVFSIODriver();
		
		/** If there is no ServerInfo, create it */
		OdilonServerInfo si = driver.getServerInfo();
		if (si==null) {
			driver.setServerInfo(getServerSettings().getDefaultOdilonServerInfo());
			return;
		}

		boolean requireUpdate = false;
		boolean forceSync = false;
		
		boolean newStandByConnection 	= ((getServerSettings().isStandByEnabled()) && (!si.isStandByEnabled()));
		boolean newVersionControl 		= ((getServerSettings().isVersionControl()) && (!si.isVersionControl()));
		boolean standByChangedUrl 		= false;
		
		OffsetDateTime now = OffsetDateTime.now();
		
		if(newVersionControl) {
			si.setVersionControlDate(now);
			requireUpdate = true;
		}
		
		if (si.getServerMode()==null || (!si.getServerMode().equals(getServerSettings().getServerMode()))) {
			si.setServerMode(getServerSettings().getServerMode());
			requireUpdate = true;
		}
		
		if (si.isStandByEnabled()!=getServerSettings().isStandByEnabled()) {
			requireUpdate = true;
		}
		
		/** If there is no standby  */
		if (!getServerSettings().isStandByEnabled()) {
			if (si.getStandByStartDate()!=null) {
				si.setStandByStartDate(null);
				si.setStandBySyncedDate(null);
				requireUpdate = true;
			}
		}
		
		if ( (si.getStandbyUrl()!=null) && (getServerSettings().getStandbyUrl()==null)) {
				requireUpdate = true;
				si.setStandByStartDate(null);
				si.setStandBySyncedDate(null);
		}
		else if ((si.getStandbyUrl()==null) && (getServerSettings().getStandbyUrl()!=null)) {
			requireUpdate = true;
			si.setStandByStartDate(now);
			si.setStandBySyncedDate(null);
		}
		else if ((si.getStandbyUrl()!=null) && (getServerSettings().getStandbyUrl()!=null)) {
			int settingsHash 	 = (getServerSettings().getStandbyUrl().trim().toLowerCase() + String.valueOf(getServerSettings().getStandbyPort())).hashCode();
			int siHash 		 	 = (si.getStandbyUrl().trim().toLowerCase() + String.valueOf(si.getStandbyPort())).hashCode();
			standByChangedUrl 	 = (settingsHash!=siHash);
		}

		forceSync = getServerSettings().isStandbySyncForce();
		
		if (newStandByConnection || standByChangedUrl || forceSync) {
			si.setStandByStartDate(now);
			si.setStandBySyncedDate(null);
			requireUpdate = true;
		}
		
		if ( !requireUpdate && ((si.getServerMode()!=null && getServerSettings().getServerMode()!=null) &&
								!si.getServerMode().equals(getServerSettings().getServerMode()))
			) {
				requireUpdate = true;
		}
		
		if  (!requireUpdate && (((si.getStandbyUrl()!=null) && getServerSettings().getStandByUrl()!=null) &&
								(!si.getStandbyUrl().equals(getServerSettings().getStandByUrl())))
			) {
				requireUpdate = true;
			}
			
		if  (!requireUpdate && (si.getStandbyPort()!=getServerSettings().getStandbyPort())
			) {
				requireUpdate = true;
			}
		
		if ( requireUpdate) {
			si.setServerMode(getServerSettings().getServerMode()); 
			si.setStandbyPort(getServerSettings().getStandbyPort());
			si.setStandbyUrl(getServerSettings().getStandByUrl());
			si.setStandByEnabled(getServerSettings().isStandByEnabled());
			si.setVersionControl(getServerSettings().isVersionControl());
			driver.setServerInfo(si);
		}
	}
	
	/**
	 * 
	 */
	private synchronized void cleanUpWorkDir() {
		try {
			getMapDrivesAll().values().forEach( item ->  {
						for ( ServerBucket bucket:listAllBuckets()) { 
								item.cleanUpWorkDir(bucket.getId());
								item.cleanUpCacheDir(bucket.getId());
						}
								
			});
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	

	private boolean isLinux() {
		if  (System.getenv("OS")!=null && System.getenv("OS").toLowerCase().contains("windows")) 
			return false;
		return true;
	}
	
	
	private Optional<String> providedMasterKey = Optional.empty();
	
	
	protected Optional<String> getProvidedMasterKey() {
		return this.providedMasterKey;
	}
	/**
	 * 
	 * 
	 */
	private void loadMasterKey() {

		/** check if the flag "initializeEncryption" is true */
		boolean isInitializeEnc = false;
		
		for ( String s:OdilonApplication.cmdArgs) {
			String ns=s.toLowerCase().trim().replace(" ", "");
			if ( ns.equals("-dinitializeencryption=true") || ns.equals("--initializeencryption=true")) {
				isInitializeEnc = true;
			}
			if ( ns.startsWith("-dmasterkey=") || ns.equals("--masterkey=")) {
				String k=s.trim();
				String separator = s.trim().startsWith("-DmasterKey=") ? "-DmasterKey=" : "--masterKey=";
				String arr[]=k.split(separator);
				if (arr.length>1)
					this.providedMasterKey = Optional.of(arr[1]); 
			}
		}
		
		
		/** if the flag initialize is true, 
		 * try to initialize or Rekey Encryption and exit */
		if (isInitializeEnc) {
			EncryptionInitializer init = new EncryptionInitializer(this, getProvidedMasterKey());
			init.execute();
			return;
		}
		
		/** no encryption -> nothing to do */
		if (!getServerSettings().isEncryptionEnabled())
			return;
			
		
		if (getServerSettings().getEncryptionKey()==null) {
			StringBuilder str = new StringBuilder();
			str.append("\n"+ServerConstant.SEPARATOR+"\n");
			str.append("odilon.properties:\n");
			str.append("encrypt=true but encryption.key is null\n");
			str.append("The Encryption key is required to use encryption.\n");
			str.append("It is generated when the encryption service is initialized.\n");
			str.append("If the encryption service has not been initialized please run the script -> " + getEnableEncryptionScriptName()+"\n");
			str.append(ServerConstant.SEPARATOR+"\n\n");
			throw new IllegalArgumentException(str.toString());
		}
		
		
		/** if master key is supplied by odilon.properties */
		
		if (getServerSettings().getInternalMasterKeyEncryptor()!=null) {
			startuplogger.info("MASTER KEY");
			startuplogger.info("----------");
			startuplogger.info("Master Key is overriden by variable 'encryption.masterKey' in 'odilon.properties'.");
			startuplogger.info("Master Key from -> odilon.properties");
			startuplogger.info(ServerConstant.SEPARATOR);
			String keyHex=getServerSettings().getInternalMasterKeyEncryptor();
			byte [] key  = ByteToString.hexStringToByte(keyHex);
			this.odilonKeyEncryptorService.setMasterKey(key);
			return;
		}

	
		IODriver driver = createVFSIODriver();
		OdilonServerInfo info = driver.getServerInfo();

		if (info==null)
			info=getServerSettings().getDefaultOdilonServerInfo();

		/** if encryption but it was not initialized yet, exit */
		if (!info.isEncryptionIntialized()) {
			EncryptionInitializer init = new EncryptionInitializer(this, getProvidedMasterKey());
			init.notInitializedError();
			return;
		}
		
		
		try {
			
			byte[] key = driver.getServerMasterKey();
			this.odilonKeyEncryptorService.setMasterKey(key);
		} catch (Exception e) {
			logger.error(e.getClass().getName() + " | " + e.getMessage(), SharedConstant.NOT_THROWN);
			throw new InternalCriticalException(e, "error with encryption key");
		}

	}
	
	private String getEnableEncryptionScriptName() {
		return isLinux() ? ServerConstant.ENABLE_ENCRYPTION_SCRIPT_LINUX : ServerConstant.ENABLE_ENCRYPTION_SCRIPT_WINDOWS;
	}


}


