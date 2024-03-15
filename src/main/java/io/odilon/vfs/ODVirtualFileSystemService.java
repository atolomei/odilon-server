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

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.OdilonApplication;
import io.odilon.cache.FileCacheService;
import io.odilon.cache.ObjectCacheService;
import io.odilon.encryption.EncryptionService;
import io.odilon.encryption.MasterKeyService;
import io.odilon.encryption.OdilonKeyEncryptorService;
import io.odilon.error.OdilonServerAPIException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;

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
import io.odilon.service.ServerSettings;
import io.odilon.service.util.ByteToString;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveBucket;
import io.odilon.vfs.model.DriveStatus;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.VFSOperation;

import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.IODriver;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VirtualFileSystemService;
import io.odilon.vfs.raid0.RAIDZeroDriver;
import io.odilon.vfs.raid1.RAIDOneDriver;
import io.odilon.vfs.raid6.RAIDSixDriver;
				
/**
 * <p>
 * Virtual Folders are maintained in a RAM cache by the {@link VirtualFileSystemService}
 * All drives managed by the VFS must have the same Folders (ie. each VFolder has a real folder on each Drive) 
 * </p>
 * <p>Stopped ->  Starting -> Running -> Stopping</p>
 */
@ThreadSafe
@Service
public class ODVirtualFileSystemService extends BaseService implements VirtualFileSystemService, ApplicationContextAware {
		
	static private Logger logger = Logger.getLogger(ODVirtualFileSystemService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
					
	@JsonIgnore
	@Autowired
	private SchedulerService schedulerService;
	
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
	private final ObjectCacheService objectCacheService;
	
	@JsonIgnore
	@Autowired
	private final LockService vfsLockService;
	
	@JsonIgnore
	@Autowired
	private final JournalService journalService;

	@JsonIgnore
	@Autowired
	private final BucketIteratorService walkerService;
	
	@JsonIgnore
	@Autowired
	private final ReplicationService replicationService;
	
	@JsonIgnore
	@Autowired							
	private final MasterKeyService masterKeyEncryptorService;

	@JsonIgnore
	@Autowired							
	private final OdilonKeyEncryptorService odilonKeyEncryptorService;
	
	@JsonIgnore
	@Autowired
	private final FileCacheService fileCacheService;
	
	@JsonIgnore
	private Map<String, Drive> drivesAll = new ConcurrentHashMap<String, Drive>();
	
	@JsonIgnore						
	private Map<String, Drive> drivesEnabled = new ConcurrentHashMap<String, Drive>();

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
	private Map<String, VFSBucket> buckets = new ConcurrentHashMap<String, VFSBucket>();

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
	public ODVirtualFileSystemService(	ServerSettings serverSettings, 
										SystemMonitorService montoringService,
										EncryptionService encrpytionService,
										LockService  vfsLockService,
										JournalService journalService,
										SchedulerService schedulerService,
										BucketIteratorService walkerService,
										ReplicationService replicationService,
										ObjectCacheService objectCacheService,
										MasterKeyService masterKeyEncryptorService,
										OdilonKeyEncryptorService odilonKeyEncryptorService,
										FileCacheService fileCacheService
								) {
		
		this.fileCacheService=fileCacheService;
		this.objectCacheService=objectCacheService;
		this.vfsLockService=vfsLockService;
		this.serverSettings=serverSettings;
		this.monitoringService=montoringService;
		this.encrpytionService=encrpytionService;
		this.journalService=journalService;
		this.schedulerService=schedulerService;
		this.walkerService=walkerService;
		this.raid=serverSettings.getRedundancyLevel();
		this.replicationService=replicationService;
		this.masterKeyEncryptorService = masterKeyEncryptorService;
		this.odilonKeyEncryptorService=odilonKeyEncryptorService;
		
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
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return createVFSIODriver().getObjectMetadataVersionAll(bucketName, objectName);
	}
	
	@Override
	public ObjectMetadata getObjectMetadataVersion(String bucketName, String objectName, int version) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return createVFSIODriver().getObjectMetadataVersion(bucketName, objectName, version);
	}
	
	@Override
	public ObjectMetadata getObjectMetadataPreviousVersion(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return createVFSIODriver().getObjectMetadataPreviousVersion(bucketName, objectName);
	}
	
	@Override
	public InputStream getObjectVersion(String bucketName, String objectName, int version) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(version>=0, "version must be >=0");
		return createVFSIODriver().getObjectVersionInputStream(bucketName, objectName, version);
	}
	
	@Override
	public boolean hasVersions(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		return createVFSIODriver().hasVersions(bucketName, objectName);
	}
				
	@Override
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta can not be null or empty");
		createVFSIODriver().deleteObjectAllPreviousVersions(meta);
	}
	
	@Override
	public void deleteBucketAllPreviousVersions(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		createVFSIODriver().deleteBucketAllPreviousVersions(bucketName);
	}
	
	@Override
	public void wipeAllPreviousVersions() {
		createVFSIODriver().wipeAllPreviousVersions();
	}

	@Override
	public ObjectMetadata restorePreviousVersion(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		return createVFSIODriver().restorePreviousVersion(bucketName, objectName);
	}

	/**
	 * <p>Creates the bucket folder in every Drive
	 * if the bucket does not exist
	 * creates the bucket
	 * rollback -> delete the bucket</p>
	 * <p>if the bucket exists mark as deleted</p>
	 */
	@Override
	public VFSBucket createBucket(String bucketName) throws IOException {
	
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(!this.buckets.containsKey(bucketName), "bucket already exist | b: " + bucketName);

		if (!bucketName.matches(SharedConstant.bucket_valid_regex)) 
			throw new IllegalArgumentException("bucketName contains invalid character | regular expression is -> " + SharedConstant.bucket_valid_regex +" |  b:" + bucketName);

		return createVFSIODriver().createBucket(bucketName);
	}
	
	/**
	 * <p>Deletes the bucket folder in every Drive
	 * mark as deleted on all drives
	 * rollback -> mark as enabled
	 * </p>
	 */
	@Override
	public void removeBucket(VFSBucket bucket) {
		removeBucket(bucket, false);
	}

	/**
	 */
	@Override
	public void removeBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		removeBucket(buckets.get(bucketName), false);
	}
	/**
	 */
	@Override
	public void forceRemoveBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		removeBucket(buckets.get(bucketName), true);
	}
	/**
	 */
	@Override
	public boolean existsBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		return (this.buckets.containsKey(bucketName));
	}
	/**
	 */
	@Override
	public boolean isEmptyBucket(String bucketName) {
		Check.requireNonNullArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist or is not Accesible | b: " + bucketName);
		return isEmpty(buckets.get(bucketName));
	}
	/**
	 * 
	 * 
	 * 
	 */
	@Override
	public boolean isEmpty(VFSBucket bucket) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(this.buckets.containsKey(bucket.getName()), "bucket does not exist or is not Accesible | b: " + bucket.getName());
		return createVFSIODriver().isEmpty(bucket);
	}
	/**
	 * 
	 * 
	 * 
	 */
	@Override
	public void putObject(VFSBucket bucket, String objectName, File file) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(this.buckets.containsKey(bucket.getName()), "bucket does not exist b: " + bucket.getName());
		createVFSIODriver().putObject(bucket, objectName, file);
	}

	/**
	 * 
	 * 
	 * 
	 */
	@Override
	public void putObject(String bucketName, String objectName, InputStream is, String fileName, String contentType) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		Check.requireNonNullArgument(objectName, "objectName can not be null -> b:" + bucketName);
		putObject(this.buckets.get(bucketName), objectName, is, fileName, contentType);
	}

	/**
	 */
	@Override
	public void putObject(VFSBucket bucket, String objectName, InputStream stream, String fileName, String contentType) {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireTrue(this.buckets.containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName can not be null -> b:" + bucket.getName());
		createVFSIODriver().putObject(bucket, objectName, stream, fileName, contentType);
	}

	/**
	 */
	@Override
	public VFSObject getObject(VFSBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireTrue(this.buckets.containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		return createVFSIODriver().getObject(bucket,  objectName);
	}

	/**
	 */
	@Override
	public VFSObject getObject(String bucketName, String objectName) {
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return createVFSIODriver().getObject(bucketName,  objectName);
	}
	
	/**
	 * <p>
	 * <pre>IMPORTANT</pre> 
	 * caller must close the {@link InputStream} returned
	 * </p>
	 * @throws IOException 
	 */
	@Override
	public InputStream getObjectStream(VFSBucket bucket, String objectName) throws IOException {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireTrue(this.buckets.containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucket.getName());
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
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		return getObjectStream(this.buckets.get(bucketName), objectName);
	}
	
	/**
	 * 
	 */
	@Override																		
	public DataList<Item<ObjectMetadata>> listObjects(String bucketName) {
	return 	listObjects(bucketName,
			Optional.empty(),
			Optional.empty(),
			Optional.empty(),
			Optional.empty());
	}
	
	/**
	 */
	@Override
	public DataList<Item<ObjectMetadata>> listObjects(String bucketName, Optional<Long> offset, Optional<Integer> pageSize,	Optional<String> prefix, Optional<String> serverAgentId) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return createVFSIODriver().listObjects(bucketName, offset, pageSize, prefix, serverAgentId);
	}
	
	/**
	 * <p>if the object does not exist or is in state DELETED -> not found</p>
	 */
	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return createVFSIODriver().getObjectMetadata(bucketName, objectName);
	}
	
	/**
	 * 
	 * 
	 */
	@Override					
	public boolean existsObject(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		return createVFSIODriver().exists(buckets.get(bucketName), objectName);
	}

	/**
	 * <p>returns true if the  object exist and is not in state DELETED</p>
	 */
	@Override
	public boolean existsObject(VFSBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket can not be null ");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucket.getName());
		Check.requireTrue(this.buckets.containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		return createVFSIODriver().exists(bucket, objectName);
	}
	
	/**
	 * 
	 */
	@Override
	public VFSBucket getBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		return buckets.get(bucketName);
	}
	
	/**
	 * 
	 */
	@Override
	public void deleteObject(String bucketName, String objectName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");
		Check.requireTrue(this.buckets.containsKey(bucketName), "bucket does not exist | b: " + bucketName);
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucketName);
		createVFSIODriver().delete(buckets.get(bucketName), objectName);
	}
	
	/**
	 * 
	 */
	@Override
	public void deleteObject(VFSBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(this.buckets.containsKey(bucket.getName()), "bucket does not exist | b: " + bucket.getName());
		Check.requireNonNullStringArgument(objectName, "objectName can not be null or empty | b:" + bucket.getName());
		Check.requireTrue(buckets.containsKey(bucket.getName()), "bucket does not exist -> b:"  + bucket.getName());
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
	 * 
	 */
	@Override					
	public boolean isEncrypt() {
		return getServerSettings().isEncryptionEnabled();
	}
	
	/**
	 * 
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
	public ObjectCacheService getObjectCacheService() {
		return this.objectCacheService;
	}
	
	@Override
	public RedundancyLevel getRedundancyLevel() {
		return this.raid;
	}

	@Override
	public Map<String, VFSBucket> getBucketsCache() {
		return buckets;
	}
	
	@Override
	public IODriver createVFSIODriver() {
												
		if (this.raid==RedundancyLevel.RAID_0) return getApplicationContext().getBean(RAIDZeroDriver.class,  this, vfsLockService);
		if (this.raid==RedundancyLevel.RAID_1) return getApplicationContext().getBean(RAIDOneDriver.class, this, vfsLockService);
		if (this.raid==RedundancyLevel.RAID_6) return getApplicationContext().getBean(RAIDSixDriver.class, this, vfsLockService);

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
	public List<VFSBucket> listAllBuckets() {
			List<VFSBucket> list = new ArrayList<VFSBucket>();
			Map<String, VFSBucket> map = new HashMap<String, VFSBucket>();
			for (Entry<String, Drive> entry: getMapDrivesEnabled().entrySet()) {
				List<DriveBucket> db = entry.getValue().getBuckets();
				db.forEach( item -> map.put(item.getName(), new ODVFSBucket(item)));
			}
			map.forEach( (k,v) -> list.add(v));
			return list;
	}

	@Override
	public boolean checkIntegrity(String bucketName, String objectName, boolean forceCheck) {
		return createVFSIODriver().checkIntegrity(bucketName, objectName, forceCheck);
	}
	
	@Override
	public BucketIteratorService getBucketIteratorService() {
		return walkerService;
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
	 * <p>if the object does not exist or is in state DELETE -> not found</p>
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

	
	private String getEnableEncryptionScriptName() {
		return isLinux() ? ServerConstant.ENABLE_ENCRYPTION_SCRIPT_LINUX : ServerConstant.ENABLE_ENCRYPTION_SCRIPT_WINDOWS;
	}
	
	private boolean isLinux() {
		if  (System.getenv("OS")!=null && System.getenv("OS").toLowerCase().contains("windows")) 
			return false;
		return true;
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
				break;
			}
		}
		
		
		/** if the flag initialize is true, 
		 * try to initialize Encryption and exit */
		if (isInitializeEnc) {
			EncryptionInitializer init = new EncryptionInitializer(this);
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
			str.append("It is generated and printed by the server when the encryption service is initialized.\n");
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
			EncryptionInitializer init = new EncryptionInitializer(this);
			init.notInitializedError();
			return;
		}
		
		/** if encryption but it was not initialized yet, exit */
		try {
			byte[] key = driver.getServerMasterKey();
			this.odilonKeyEncryptorService.setMasterKey(key);
		} catch (Exception e) {
			logger.error(e.getClass().getName() + " | " + e.getMessage());
			throw new InternalCriticalException(e, "error with encryption key");
		}

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
					}
				}

				startuplogger.info("Started -> " + VirtualFileSystemService.class.getSimpleName());
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
					drive=new ChunkedDrive(String.valueOf(configOrder), dir, configOrder);
					configOrder++;
				}
				else {
					drive=new ODSimpleDrive(String.valueOf(configOrder), dir, configOrder);
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
					((ODDrive) drive).forceDeleteBucket(driveBucket.getName());
				}
			}
		}
	}
	
	
	/**
	 * 
	 * 
	 */
	private void loadBuckets() {
		Map<String, VFSBucket> map = ((BaseIODriver) createVFSIODriver()).getBucketsMap();
		if (map.size()==0) 
			startuplogger.debug("Adding buckets to cache -> ok (no buckets)"); 
		else  {
			map.forEach((k,v) -> buckets.put(k,v));
			startuplogger.debug("Adding buckets to cache -> ok (" + String.valueOf(map.size()) + ")");
		}
	}
	
	/**
	 * <p>deletes the folder in every Drive</p>
	 */
	private void removeBucket(VFSBucket bucket, boolean forceDelete) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(buckets.containsKey(bucket.getName()), "bucket does not exist -> b:"  + bucket.getName());
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
			// map.forEach((k,v) -> logger.debug(k +" -> " + v));
			
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
				//SpringApplication.exit(this.getVFS().getApplicationContext(), () -> 1);
				((ConfigurableApplicationContext) this.getApplicationContext()).close();
				System.exit(1);
				
				
			} else 	{
				startuplogger.debug("Structure check -> ok (d:" + String.valueOf(size)+ " | b:" + String.valueOf(buckets) + ")"  );
			}
			
		} catch (Exception e) {
			logger.error(e);
			throw(e);
		}
	}

	/**
	 * <p>@param bucket bucket must exist in the system</p> 
	 */
	private void deleteBucketInternal(VFSBucket bucket, boolean forceDelete) {
		
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		Check.requireTrue(isEmpty(bucket), "bucket is not Emtpy | b:" + bucket.getName()); 
		
		VFSOperation op = null;
		boolean done = false;
		
		List<Drive> listDrives = null;
		
		try {
			
			getLockService().getBucketLock(bucket.getName()).writeLock().lock();
			
			if (!existsBucket(bucket.getName()))
				throw new IllegalArgumentException("bucket does not exist -> b:" +bucket.getName());
			
			if ((!forceDelete) && (!isEmpty(bucket))) 
				throw new OdilonServerAPIException("bucket must be empty to be deleted -> b:" +bucket.getName());
			
			op = getJournalService().deleteBucket(bucket.getName());
			
			listDrives = new ArrayList<Drive>(getMapDrivesAll().values());
			
			for (Drive drive: listDrives) {
				try {
					drive.markAsDeletedBucket(bucket.getName());
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
						this.buckets.remove(bucket.getName());
						if (listDrives!=null) {
							for (Drive drive: listDrives) {
								((ODDrive) drive).forceDeleteBucket(bucket.getName());
							}		
						}
					}
					else {
						/** rollback restores all buckets */
						createVFSIODriver().rollbackJournal(op, false);
						this.buckets.put(bucket.getName(), bucket);
					}
					
			} catch (Exception e) {
					logger.error(e, ServerConstant.NOT_THROWN);
			}
			finally {
					getLockService().getBucketLock(bucket.getName()).writeLock().unlock();
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
		((io.odilon.query.ODBucketIteratorService) getBucketIteratorService()).setVFS(this);

		/** JournalService -> lazy injection */ 
		((ODJournalService) getJournalService()).setVFS(this);

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
		boolean standByChangedUrl = false;
		
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
						for ( VFSBucket bucket:listAllBuckets()) { 
								item.cleanUpWorkDir(bucket.getName());
								item.cleanUpCacheDir(bucket.getName());
						}
								
			});
		} catch (Exception e) {
			logger.error(e);
		}
	}

	

}


