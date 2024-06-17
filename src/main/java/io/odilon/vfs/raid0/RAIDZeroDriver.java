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
package io.odilon.vfs.raid0;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.encryption.EncryptionService;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketStatus;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.scheduler.AbstractServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.service.util.ByteToString;
import io.odilon.util.Check;
import io.odilon.util.ODFileUtils;
import io.odilon.vfs.BaseIODriver;
import io.odilon.vfs.ODDrive;
import io.odilon.vfs.ODVFSBucket;
import io.odilon.vfs.ODVFSObject;
import io.odilon.vfs.ODVFSOperation;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveBucket;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.ODBucket;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.BucketIterator;
import io.odilon.vfs.model.VFSOp;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>
 * <b>RAID 0. Stripped Disks.</b>
 * </p>
 * <p>Two or more disks are combined to form a volume, which appears as a single 
 * virtual drive. It is not aconfiguration with data replication, its function is 
 * to provide greater storage and performance by allowing access to the disks in 
 * parallel.</p>
 * <p>
 * All buckets <b>must</b> exist on all drives. If a bucket is not present on a
 * drive -> the Bucket is considered <i>"non existent"</i>.<br/>
 * Each file is stored only on 1 Drive in RAID 0. If a file does not have the file's
 * Metadata Directory -> the file is considered <i>"non existent"</i>.
 * </p>
 * <p>NOTE:- There are no {@link Drive} in mode {@link DriveStatus.NOTSYNC} in RAID 0. 
 * All new drives are synchronized before the {@link VirtualFileSystemService} completes its 
 * initialization. </p>

 * <p>This Class is works as a <a href="https://en.wikipedia.org/wiki/Facade_pattern">Facade pattern</a> 
 * that uses {@link  RAIDZeroCreateObjectHandler}, {@link  RAIDZeroDeleteObjectHandler}, {@link  RAIDZeroUpdateObjectHandler} 
 * and other</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)	 
 * 
 */
@ThreadSafe
@Component
@Scope("prototype")
public class RAIDZeroDriver extends BaseIODriver implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(RAIDZeroDriver.class.getName());
	static private Logger std_logger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private ApplicationContext applicationContext;

	
	public RAIDZeroDriver(VirtualFileSystemService vfs, LockService vfsLockService) {
		super(vfs, vfsLockService);
	}

	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
	
	
	
	
	/**
	 * @return
	 
	@Override
	public byte[] getServerMasterKey() {

		getLockService().getServerLock().readLock().lock();
		
		try {
			
			File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
			
			if (file == null || !file.exists())
				return null;
			
			byte[] bDataEnc = FileUtils.readFileToByteArray(file);
			
			String encryptionKey = getVFS().getServerSettings().getEncryptionKey();
			String encryptionIV = getVFS().getServerSettings().getEncryptionIV();
			
			if (encryptionKey==null || encryptionIV==null)
				throw new InternalCriticalException(" encryption Key or IV is null");
			
			byte [] b_encryptionKey = ByteToString.hexStringToByte(encryptionKey);
			byte [] b_encryptionKeyIV = ByteToString.hexStringToByte(encryptionKey+encryptionIV);
			
			byte [] b_hmacOriginal;

			try {
				
				b_hmacOriginal = getVFS().HMAC(b_encryptionKeyIV, b_encryptionKey);
				
			} catch (InvalidKeyException | NoSuchAlgorithmException e) {
				throw new InternalCriticalException(e, "can not calculate HMAC for 'odilon.properties' encryption key");
			}
			
			// HMAC(32) + Master Key (16) + IV(12) + Salt (64) 
			byte[] bdataDec = getVFS().getMasterKeyEncryptorService().decryptKey(bDataEnc);

			
			byte[] b_hmacNew = new byte[EncryptionService.HMAC_SIZE];
			System.arraycopy(bdataDec, 0, b_hmacNew, 0, b_hmacNew.length);
			
			if (!Arrays.equals(b_hmacOriginal, b_hmacNew)) {
				logger.error("HMAC is not correct, HMAC of 'encryption.key' in 'odilon.properties' does not match with HMAC in 'key.enc'  -> encryption.key=" + encryptionKey+encryptionIV);
				throw new InternalCriticalException("HMAC is not correct, HMAC of 'encryption.key' in 'odilon.properties' does not match with HMAC in 'key.enc'  -> encryption.key=" + encryptionKey+encryptionIV);
			}
			
			// HMAC is correct 
			byte[] key = new byte[EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
			System.arraycopy(bdataDec, b_hmacNew.length, key, 0,  key.length);
			
			return key;

		} catch (InternalCriticalException e) {
			if ((e.getCause()!=null) && (e.getCause() instanceof javax.crypto.BadPaddingException))
				logger.error("possible cause -> the value of 'encryption.key' in 'odilon.properties' is incorrect");
			throw e;
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, "getServerMasterKey");

		} finally {
			getLockService().getServerLock().readLock().unlock();
		}
	}
	*/
	
	
	
	
	/**
	 * 
	 * 
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
						//isError = true;
						reqRestoreBackup = false;
						throw new InternalCriticalException(e, "Drive -> " + drive.getName());
					}
				}
				
				op = getJournalService().saveServerKey();
				
				reqRestoreBackup = true;
				
				Exception eThrow = null;

				byte[] data = new byte[hmac.length + iv.length + key.length + salt.length];
				
				/** HMAC(32) + Master Key (16) + IV(12) + Salt (64) */
				System.arraycopy(hmac, 0, data, 0        							, hmac.length);
				System.arraycopy(key,  0, data, hmac.length 						, key.length);
				System.arraycopy(iv,   0, data, (hmac.length+key.length) 			, iv.length);
				System.arraycopy(salt, 0, data, (hmac.length+iv.length+key.length)	, salt.length);
				
				byte[] dataEnc = getVFS().getMasterKeyEncryptorService().encryptKey(data, iv);

				/** save */
				for (Drive drive: getDrivesAll()) {
					try {
						File file = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
						FileUtils.writeByteArrayToFile(file, dataEnc);
						
					} catch (Exception e) {
						eThrow = new InternalCriticalException(e, "Drive -> " + drive.getName());
						break;
					}
				}
				
				if (eThrow!=null)
					throw eThrow;
				
				done = op.commit();
				
		} catch (InternalCriticalException e) {
			throw e;
			
		} catch (Exception e) {
			if (logger.isDebugEnabled())
				logger.error(e);
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
	public boolean hasVersions(ODBucket bucket, String objectName) {
		return !getObjectMetadataVersionAll(bucket, objectName).isEmpty();
	}


	/**
	 * 
	 * <p>
	 * Delete all versions older than the current <b>head version</b>. <br/>
	 * <b>IMPORTANT</b>. It does not delete the current head version. <br/>
	 * <br/>
	 * If the current <b>head version</b> does not have previous versions it does
	 * nothing.
	 * </p>
	 * 
	 * @see {@link RAIDZeroUpdateObjectHandler}
	 */
	@Override
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
		agent.deleteObjectAllPreviousVersions(meta);

	}

	/**
	 * <p>
	 * Restores the version that is previous to the current <b>head version</b>.<br/>
	 * The previous version becomes the new head version, and the current head 
	 * version is deleted.If the current <b>head version</b> does not have previous 
	 * version it does nothing.
	 * </p>
	 * 
	 * @see {@link RAIDZeroUpdateObjectHandler}
	 */
	@Override
	public ObjectMetadata restorePreviousVersion(ODBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie." + BucketStatus.ARCHIVED.getName() + " or " + BucketStatus.ENABLED.getName() + ") | b:" + bucket.getName());

		RAIDZeroUpdateObjectHandler agent = new RAIDZeroUpdateObjectHandler(this);
		return agent.restorePreviousVersion(bucket, objectName);
	}

	/**
	 * 
	 * <p>
	 * Creates a ServiceRequest to walk through all objects and delete versions.
	 * This process is Async and handler returns immediately.
	 * </p>
	 * <p>
	 * The ServiceRequest {@link DeleteBucketObjectPreviousVersionServiceRequest}
	 * creates N Threads to scan all Objects and remove previous versions. In case
	 * of failure (for example. the server is shutdown before completion), it is
	 * retried up to 5 times.
	 * </p>
	 * <p>
	 * Although the removal of all versions for every Object is Transactional, the
	 * ServiceRequest itself is not Transactional, and it can not be Rollback
	 * </p>
	 * 
	 */
	@Override
	public void wipeAllPreviousVersions() {
		RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
		agent.wipeAllPreviousVersions();
	}

	/**
	 * <p>
	 * Creates a ServiceRequest to walk through all objects and delete versions.
	 * This process is Async and handler returns immediately.
	 * </p>
	 * <p>
	 * The ServiceRequest {@link DeleteBucketObjectPreviousVersionServiceRequest}
	 * creates N Threads to scan all Objects and remove previous versions. In case
	 * of failure (for example. the server is shutdown before completion), it is
	 * retried up to 5 times.
	 * </p>
	 * <p>
	 * Although the removal of all versions for every Object is Transactional, the
	 * ServiceRequest itself is not Transactional, and it can not be Rollback
	 * </p>
	 * 
	 */
	@Override
	public void deleteBucketAllPreviousVersions(ODBucket bucket) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucket.getName());
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() + " or " + BucketStatus.ENABLED.getName() + ") | b:" + bucket.getName());

		RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
		agent.deleteBucketAllPreviousVersions(bucket);
	}

	/**
	 * <p>
	 * </p>
	 */
	@Override
	public void putObject(ODBucket bucket, String objectName, InputStream stream, String fileName,	String contentType) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		Check.requireNonNullStringArgument(fileName, "fileName is null | b: " + bucket.getName() + " o:" + objectName);
		Check.requireNonNullArgument(stream, "InpuStream can not null -> b:" + bucket.getName() + " | o:" + objectName);

		//
		// TODO AT -> we must fix this, the exist call must be inside the locked section
		//
		if (exists(bucket, objectName)) {
			RAIDZeroUpdateObjectHandler updateAgent = new RAIDZeroUpdateObjectHandler(this);
			updateAgent.update(bucket, objectName, stream, fileName, contentType);
			getVFS().getSystemMonitorService().getUpdateObjectCounter().inc();
		} else {
			RAIDZeroCreateObjectHandler createAgent = new RAIDZeroCreateObjectHandler(this);
			createAgent.create(bucket, objectName, stream, fileName, contentType);
			getVFS().getSystemMonitorService().getCreateObjectCounter().inc();
		}
	}

	/**
	 * <p>This method is called only for Objects that already exist</p>
	 */
	@Override
	public void putObjectMetadata(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDZeroUpdateObjectHandler updateAgent = new RAIDZeroUpdateObjectHandler(this);
		updateAgent.updateObjectMetadata(meta);
	}

	@Override
	public void delete(ODBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		RAIDZeroDeleteObjectHandler agent = new RAIDZeroDeleteObjectHandler(this);
		agent.delete(bucket, objectName);
	}

	/**
	 * <p>This method is executed Async by the {@link SchedulerService} to cleanup
	 * work files after a Object is deleted</p>
	 */
	@Override
	public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDZeroDeleteObjectHandler createAgent = new RAIDZeroDeleteObjectHandler(this);
		createAgent.postObjectDelete(meta, headVersion);
	}

	/**
	 * <p>This method is executed Async by the {@link SchedulerService}</p>
	 */
	@Override
	public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta,int headVersion) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDZeroDeleteObjectHandler createAgent = new RAIDZeroDeleteObjectHandler(this);
		createAgent.postObjectPreviousVersionDeleteAll(meta, headVersion);
	}

	/**
	 * <p>Set up a new drive</p>
	 * @see {@link RAIDZeroDriveSetupSync}
	 */
	@Override
	public boolean setUpDrives() {
		return getApplicationContext().getBean(RAIDZeroDriveSetupSync.class, this).setup();
	}

	/**
	 * <p>Precondition -> Bucket does not exist</p>
	
	
	public ODBucket createBucket(String bucketName) {

		Check.requireNonNullArgument(bucketName, "bucketName is null");

		BucketMetadata meta = new BucketMetadata(bucketName);
		VFSOperation op = null;
		boolean done = false;

		meta.status = BucketStatus.ENABLED;
		meta.appVersion = OdilonVersion.VERSION;
		meta.id=getVFS().getNextBucketId();

		ODBucket bucket = new ODVFSBucket(meta);
		boolean isMainException = false;

		getLockService().getBucketLock(meta.id).writeLock().lock();

		try {
			if (getVFS().existsBucket(bucketName))
				throw new IllegalArgumentException("bucket already exist | b: " + bucketName);

			op = getJournalService().createBucket(meta.id, bucketName);

			OffsetDateTime now = OffsetDateTime.now();

			meta.creationDate = now;
			meta.lastModified = now;

			for (Drive drive: getDrivesAll()) {
				try {
					
					drive.createBucket(meta);
					
				} catch (Exception e) {
					done = false;
					isMainException = true;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			done = op.commit();
			return bucket;
		} finally {
			try {
				if (done) {
					getVFS().addBucketCache(bucket);
				}
				else
					rollbackJournal(op);
			} catch (Exception e) {
				if (!isMainException)
					throw new InternalCriticalException(e, "finally");
				else
				logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				getLockService().getBucketLock(meta.id).writeLock().unlock();
			}
		}
	}
	 */

	/**
	 * @param bucket bucket must exist in the system
	 */
	public void deleteBucket(ODBucket bucket) {
		getVFS().removeBucket(bucket);
	}

	/**
	 * <p>
	 * RAID 0 -> Bucket must be empty on all Disks VFSBucket bucket must exist and
	 * be ENABLED
	 * </p>
	 */
	@Override
	public boolean isEmpty(ODBucket bucket) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(existsBucketInDrives(bucket.getId()), "bucket does not exist in all drives -> b: " + bucket.getName());

		getLockService().getBucketLock(bucket.getId()).readLock().lock();
		
		try {
			for (Drive drive: getDrivesEnabled()) {
				if (!drive.isEmpty(bucket.getId()))
					return false;
			}
			return true;
		} catch (Exception e) {
			throw new InternalCriticalException(e, "b:" + bucket.getName());
		} finally {
			getLockService().getBucketLock(bucket.getId()).readLock().unlock();
		}
	}


	/**
	 * <p>
	 * The object must be in status {@code BucketStatus.ENABLED} or
	 * {@code BucketStatus.ARCHIVED} if the object is DELETED -> it will be purged
	 * from the system at some point. The normal use case is to check {@link exists}
	 * before calling this method.
	 * </p>
	 */
	@Override
	public VFSObject getObject(ODBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. must be " + ObjectStatus.ENABLED.getName() + " or " + ObjectStatus.ARCHIVED.getName() + " b:" + bucket.getName());
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		try {
			
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().lock();
			
			try {
			
				getLockService().getBucketLock(bucket.getId()).readLock().lock();
	
				/** read is from only 1 drive in RAID 0 */
				Drive readDrive = getReadDrive(bucket, objectName);
	
				if (!readDrive.existsBucket(bucket.getId()))
					throw new IllegalArgumentException(" b:" + bucket.getName() + " does not exist for -> d:" + readDrive.getName() + " | raid -> " + this.getClass().getSimpleName());
	
				if (!exists(bucket, objectName))
					throw new OdilonObjectNotFoundException("object does not exists for ->  b:" + bucket.getName() + " | o:" + objectName + " | class:" + this.getClass().getSimpleName());
	
				ObjectMetadata meta = getObjectMetadataInternal(bucket, objectName, true);
	
				if (meta.status == ObjectStatus.ENABLED || meta.status == ObjectStatus.ARCHIVED) {
					return new ODVFSObject(bucket, objectName, getVFS());
				}
	
				/**
				 * if the object is DELETED or DRAFT -> it will be purged from the system at
				 * some point.
				 */
				throw new OdilonObjectNotFoundException(String.format("object not found | status must be %s or %s -> b: %s | o:%s | o.status: %s",
								ObjectStatus.ENABLED.getName(), ObjectStatus.ARCHIVED.getName(),
								Optional.ofNullable(bucket.getName()).orElse("null"),
								Optional.ofNullable(bucket.getName()).orElse("null"), meta.status.getName()));
			}
	
			catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucket.getName() +objectName);
			} finally {
				getLockService().getBucketLock(bucket.getId()).readLock().unlock();
			}
		}
		finally {
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();
		}
	}

	/**
	 * <p>
	 * RAID 0 -> object is stored only on 1 Drive
	 * </p>
	 */
	@Override
	public boolean exists(ODBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		getLockService().getObjectLock(bucket.getId(), objectName).readLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
				boolean exists = getDrive(bucket, objectName).existsObjectMetadata(bucket.getId(), objectName);
				if (!exists)
					return false;
				/** TBA chequear que no este "deleted" en el drive */
				return true;
			} finally {
				getLockService().getBucketLock(bucket.getId()).readLock().unlock();
			}
		}
		finally {
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();
		}
	}

	/**
	 * <p>
	 * Returns a {@link DataList} of {@link Items} <br/>
	 * The list contained by the {@link DataList} has <code>pageSize</code> items
	 * starting from <code>offset</code> (first element is 0), or less if there are
	 * not enough items.
	 * 
	 * {@link Item} is a wrapper of an {@link ObjectMetadata} or an error. The items
	 * in the DataList are not ordered.<br/>
	 * 
	 * @param serverAgentId is an optional Id that works as a cache of the object
	 *                      that is generating the pages for this query.
	 *                      {@link BucketIteratorService} contains a cache of
	 *                      {@link BucketIterator} for ongoing queries.
	 *                      </p>
	 * 
	 * @param prefix        of the objectname
	 * 
	 */
	@Override
	public DataList<Item<ObjectMetadata>> listObjects(ODBucket bucket, Optional<Long> offset, Optional<Integer> pageSize, Optional<String> prefix, Optional<String> serverAgentId) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());

		BucketIterator iterator = null;
		
		try {

			/**
			 * serverAgentId works as a cache of the object that is generating the pages of
			 * the query
			 */
			if (serverAgentId.isPresent())
				iterator = getVFS().getBucketIteratorService().get(serverAgentId.get());

			if (iterator == null) {
				iterator = new RAIDZeroBucketIterator(this, bucket, offset, prefix);
				getVFS().getBucketIteratorService().register(iterator);
			}

			List<Item<ObjectMetadata>> list = new ArrayList<Item<ObjectMetadata>>();

			int size = pageSize.orElseGet(() -> ServerConstant.DEFAULT_PAGE_SIZE);
			int counter = 0;

			while (iterator.hasNext() && (counter++ < size)) {
				Item<ObjectMetadata> item;
				try {

					ObjectMetadata meta = getOM(bucket, iterator.next().toFile().getName(), Optional.empty(), false);
					item = new Item<ObjectMetadata>(meta);
					
				} catch (Exception e) {
					logger.error(e, SharedConstant.NOT_THROWN);
					item = new Item<ObjectMetadata>(e);
				}
				list.add(item);
			}

			DataList<Item<ObjectMetadata>> result = new DataList<Item<ObjectMetadata>>(list);

			/**
			 * EOD (End of Data) is used to prevent the client to send a new Request after
			 * returning the last page of the result
			 */
			if (!iterator.hasNext())
				result.setEOD(true);

			result.setOffset(iterator.getOffset());
			result.setPageSize(size);
			result.setAgentId(iterator.getAgentId());

			return result;

		} finally {

			if (iterator != null && (!iterator.hasNext())) {
				/** removing from IteratorService closes the stream	 */
				getVFS().getBucketIteratorService().remove(iterator.getAgentId());
			}
		}
	}

	/**
	 * <p>
	 * It returns ObjectMetadata of all previous versions it does not include head
	 * version
	 * </p>
	 */
	@Override
	public List<ObjectMetadata> getObjectMetadataVersionAll(ODBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() + " or " + BucketStatus.ENABLED.getName() + ") | b:" + bucket.getName());

		List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

		Drive readDrive = null;

		getLockService().getObjectLock(bucket.getId(), objectName).readLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
				
				/** read is from only 1 drive */
				readDrive = getReadDrive(bucket, objectName);
	
				if (!readDrive.existsBucket(bucket.getId()))
					throw new IllegalStateException("CRITICAL ERROR | bucket -> b:" + bucket.getName() + " does not exist for -> d:" + readDrive.getName() + " | raid -> " + this.getClass().getSimpleName());

				ObjectMetadata meta = getObjectMetadataInternal(bucket, objectName, true);
	
				if ((meta == null) || (!meta.isAccesible()))
					throw new OdilonObjectNotFoundException(ObjectMetadata.class.getSimpleName() + " does not exist");
	
				if (meta.version == 0)
					return list;
	
				for (int version = 0; version < meta.version; version++) {
					
					ObjectMetadata meta_version = readDrive.getObjectMetadataVersion(bucket.getId(), objectName, version);

					
					if (meta_version != null) {
						/** bucketName is not stored on disk, only bucketId, we must set it explicitly */
						meta_version.setBucketName(bucket.getName());
						list.add(meta_version);
					}
				}
				return list;
	
			} catch (OdilonObjectNotFoundException e) {
				e.setErrorMessage((e.getMessage() != null ? (e.getMessage() + " | ") : "") + "b:" + bucket.getName() + ", o:" + objectName +", d:" + (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null"));
				throw e;
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucket.getName() + ", o:" + objectName +", d:" + (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null"));
			} finally {
				getLockService().getBucketLock(bucket.getId()).readLock().unlock();
			}
		}
		finally {
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();			
		}
	}

	/**
	 * 
	 */
	@Override
	public ObjectMetadata getObjectMetadata(ODBucket bucket, String objectName) {
		return getOM(bucket, objectName, Optional.empty(), true);
	}

	@Override
	public ObjectMetadata getObjectMetadataVersion(ODBucket bucket, String objectName, int version) {
		return getOM(bucket, objectName, Optional.of(Integer.valueOf(version)), true);
	}


	/**
	 * <p>
	 * If version does not exist -> throws OdilonObjectNotFoundException
	 * </p>
	 */
	@Override
	public InputStream getObjectVersionInputStream(ODBucket bucket, String objectName, int version) {

		Check.requireNonNullArgument(bucket, "bucket is null");

		//ODBucket bucket = getVFS().getBucketById(bucketId);

		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() + " or " + BucketStatus.ENABLED.getName() + ") | b:" + bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		getLockService().getObjectLock(bucket.getId(), objectName).readLock().lock();
		
		try {
		
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
	
				/** RAID 0: read is from only 1 drive */
				Drive readDrive = getReadDrive(bucket, objectName);
	
				if (!readDrive.existsBucket(bucket.getId()))
					throw new IllegalStateException("bucket -> b:" + bucket.getName() + " does not exist for -> d:"	+ readDrive.getName() + " | v:" + String.valueOf(version));
	
				ObjectMetadata meta = getObjectMetadataVersion(bucket, objectName, version);
	
				if ((meta != null) && meta.isAccesible()) {
					InputStream stream = getInputStreamFromSelectedDrive(readDrive, bucket.getId(), objectName, version);
					if (meta.encrypt)
						return getVFS().getEncryptionService().decryptStream(stream);
					else
						return stream;
				} else
					throw new OdilonObjectNotFoundException("object version does not exist -> b:" + bucket.getName() + " | o:" + objectName + " | v:" + String.valueOf(version));
	
			} catch (OdilonObjectNotFoundException e) {
				throw e;
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + (Optional.ofNullable(bucket).isPresent() ? (bucket.getName()) : "null") + ", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName) : "null"));
			} finally {
				getLockService().getBucketLock(bucket.getId()).readLock().unlock();
				
			}
		} finally {
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();
		}
		
		
	}

	/**
	 * <p>
	 * <b>IMPORTANT</b> -> caller must close the {@link InputStream} returned
	 * </p>
	 * 
	 * @param bucketName
	 * @param objectName
	 * @return
	 * 
	 */
	@Override
	public InputStream getInputStream(ODBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(),	"bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		getLockService().getObjectLock(bucket.getId(), objectName).readLock().lock();
		
		try {
		
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
	
				/** RAID 0: read is from only 1 drive */
				Drive readDrive = getReadDrive(bucket, objectName);
	
				if (!readDrive.existsBucket(bucket.getId()))
					throw new IllegalStateException("bucket -> b:" + bucket.getName() + " does not exist for drive -> d:" + readDrive.getName() + " | class -> " + this.getClass().getSimpleName());
	
				ObjectMetadata meta = getObjectMetadataInternal(bucket, objectName, true);
	
				if ((meta != null) && meta.isAccesible()) {
					InputStream stream = getInputStreamFromSelectedDrive(readDrive, bucket.getId(), objectName);
					return (meta.encrypt) ? getVFS().getEncryptionService().decryptStream(stream) : stream;
				}
				throw new OdilonObjectNotFoundException("object does not exists for -> b:" + bucket.getName() + " | o:" + objectName + " | class:" + this.getClass().getSimpleName());
	
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + (Optional.ofNullable(bucket).isPresent() ? (bucket.getName()) : "null") + ", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName) : "null"));
			} finally {
				getLockService().getBucketLock(bucket.getId()).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();
		}
	}

	/**
	 * <p>
	 * RAID 0. Journal Files go to drive 0
	 * </p>
	 */
	@Override
	public List<VFSOperation> getJournalPending(JournalService journalService) {

		List<VFSOperation> list = new ArrayList<VFSOperation>();
		Drive drive = getDrivesEnabled().get(0);

		File dir = new File(drive.getJournalDirPath());

		if (!dir.exists())
			return list;

		if (!dir.isDirectory())
			return list;

		File[] files = dir.listFiles();

		if (files.length == 0)
			return list;

		for (File file: files) {

			if (!file.isDirectory()) {
				Path pa = Paths.get(file.getAbsolutePath());
				try {
					String str = Files.readString(pa);
					ODVFSOperation op = getObjectMapper().readValue(str, ODVFSOperation.class);
					op.setJournalService(getJournalService());
					list.add(op);
				} catch (IOException e) {
					logger.debug(e, "f:" + (Optional.ofNullable(file).isPresent() ? (file.getName()) : "null"));
					try {
						Files.delete(file.toPath());
					} catch (IOException e1) {
						logger.error(e);
					}
				}
			}
		}
		std_logger.info("Total operations that will rollback -> " + String.valueOf(list.size()));
		return list;
	}

	/**
	 * <p>
	 * before starting operations load Requests that are stored on disk
	 * </p>
	 */
	@Override
	public List<ServiceRequest> getSchedulerPendingRequests(String queueId) {

		List<ServiceRequest> list = new ArrayList<ServiceRequest>();

		Drive drive = getDrivesEnabled().get(0);

		for (File file: drive.getSchedulerRequests(queueId)) {
			try {
				list.add((ServiceRequest) getObjectMapper().readValue(file, AbstractServiceRequest.class));
			} catch (IOException e) {
				try {
					Files.delete(file.toPath());
				} catch (IOException e1) {
					logger.error(e, SharedConstant.NOT_THROWN);
				}
			}
		}
		return list;
	}

	/**
	 * <p>
	 * RAID 0. Journal Files go to drive 0
	 * </p>
	 */
	@Override
	public void saveJournal(VFSOperation op) {
		getDrivesEnabled().get(0).saveJournal(op);
	}

	/**
	 * <p>
	 * RAID 0. Journal Files go to drive 0
	 * </p>
	 */
	@Override
	public void removeJournal(String id) {
		getDrivesEnabled().get(0).removeJournal(id);
	}

	public void rollbackJournal(VFSOperation op) {
		rollbackJournal(op, false);
	}

	/**
	 * <p>
	 * Rollback from Journal
	 * 
	 * Required locks must be applied before calling this method
	 * 
	 * </p>
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {

		Check.requireNonNullArgument(op, "VFSOperation is null");

		switch (op.getOp()) {
				case CREATE_OBJECT: {
				RAIDZeroCreateObjectHandler handler = new RAIDZeroCreateObjectHandler(this);
				handler.rollbackJournal(op, recoveryMode);
				return;
			}  case UPDATE_OBJECT: {
				RAIDZeroUpdateObjectHandler handler = new RAIDZeroUpdateObjectHandler(this);
				handler.rollbackJournal(op, recoveryMode);
				return;
			} case DELETE_OBJECT: {
				RAIDZeroDeleteObjectHandler handler = new RAIDZeroDeleteObjectHandler(this);
				handler.rollbackJournal(op, recoveryMode);
				return;
			} case DELETE_OBJECT_PREVIOUS_VERSIONS: {
				RAIDZeroDeleteObjectHandler handler = new RAIDZeroDeleteObjectHandler(this);
				handler.rollbackJournal(op, recoveryMode);
				return;
			} case RESTORE_OBJECT_PREVIOUS_VERSION: {
				RAIDZeroUpdateObjectHandler handler = new RAIDZeroUpdateObjectHandler(this);
				handler.rollbackJournal(op, recoveryMode);
				return;
			} case UPDATE_OBJECT_METADATA: {
				RAIDZeroUpdateObjectHandler handler = new RAIDZeroUpdateObjectHandler(this);
				handler.rollbackJournal(op, recoveryMode);
				return;
			}
			default:
				break;
		}
		
		// --
		String objectName = op.getObjectName();
		Long bucketId = op.getBucketId();
		
		boolean done = false;

		try {

			if (getVFS().getServerSettings().isStandByEnabled()) {
				getVFS().getReplicationService().cancel(op);
			}
			else if (op.getOp() == VFSOp.CREATE_SERVER_MASTERKEY) {
				for (Drive drive : getDrivesAll()) {
					File file = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
					if ((file != null) && file.exists())
						FileUtils.forceDelete(file);
				}
				done = true;
			}
			else if (op.getOp() == VFSOp.CREATE_BUCKET) {
				for (Drive drive : getDrivesAll())
					((ODDrive) drive).forceDeleteBucket(bucketId);
				done = true;
			}
			else if (op.getOp() == VFSOp.DELETE_BUCKET) {
				for (Drive drive : getDrivesAll())
					drive.markAsEnabledBucket(bucketId);
				done = true;
			} else if (op.getOp() == VFSOp.UPDATE_BUCKET) {
				restoreBucketMetadata(getVFS().getBucketById(bucketId));
				done = true;
			} else if (op.getOp() == VFSOp.CREATE_SERVER_METADATA) {
				if (objectName != null) {
					for (Drive drive : getDrivesAll()) {
						drive.removeSysFile(op.getObjectName());
					}
				}
				done = true;
			} else if (op.getOp() == VFSOp.UPDATE_SERVER_METADATA) {
				if (objectName != null) {
					logger.debug("no action yet, rollback -> " + VFSOp.UPDATE_SERVER_METADATA.getName());
				}
				done = true;
			} else if (op.getOp() == VFSOp.UPDATE_BUCKET) {
				logger.debug("no action yet, rollback -> " + VFSOp.UPDATE_BUCKET.getName());
				done = true;
			}

		} catch (InternalCriticalException e) {
			String msg = "The following Operation can not be Rollback: \n"
					+ (Optional.ofNullable(op).isPresent() ? op.toString() : "null");
			logger.error(msg);
			if (!recoveryMode)
				throw (e);

		} catch (Exception e) {
			String msg = "The following Operation can not be Rollback: \n"
					+ (Optional.ofNullable(op).isPresent() ? op.toString() : "null");
			logger.error(msg);
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
		} finally {
			if (done || recoveryMode) {
				op.cancel();
			} else {
				if (getVFS().getServerSettings().isRecoverMode()) {
					logger.error("---------------------------------------------------------------");
					logger.error("Cancelling failed operation -> " + op.toString());
					logger.error("---------------------------------------------------------------");
					op.cancel();
				}
			}
		}
	}

	/**
	 * RAID 0 -> read drive and write drive are the same
	 */
	public Drive getWriteDrive(ODBucket bucket, String objectName) {
		return getDrive(bucket, objectName);
	}


	protected Drive getDrive(ODBucket bucket, String objectName) {
		return getDrivesEnabled().get(Math.abs(objectName.hashCode() % getDrivesEnabled().size()));
	}

	/**
	 * 
	 * 
	 */
	@Override
	public boolean checkIntegrity(ODBucket bucket, String objectName, boolean forceCheck) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null for b:" + bucket.getName());

		OffsetDateTime thresholdDate = OffsetDateTime.now().minusDays(getVFS().getServerSettings().getIntegrityCheckDays());

		Drive readDrive = null;
		ObjectMetadata metadata = null;

		boolean objectLock = false;
		boolean bucketLock = false;

		try {
			
			try {
				objectLock = getLockService().getObjectLock(bucket.getId(), objectName).readLock().tryLock(20, TimeUnit.SECONDS);
				if (!objectLock) {
					logger.warn("Can not acquire read Lock for o: " + objectName + ". Assumes -> check is ok");
					return true;
				}
			} catch (InterruptedException e) {
				return true;
			}

			try {
				bucketLock = getLockService().getBucketLock(bucket.getId()).readLock().tryLock(20, TimeUnit.SECONDS);
				if (!bucketLock) {
					logger.warn("Can not acquire read Lock for b: " + bucket.getName()+ ". Assumes -> check is ok");
					return true;
				}
			} catch (InterruptedException e) {
				logger.warn(e);
				return true;
			}

			// ---
			//
			// For RAID 0 the check is in the head version
			// there is no way to fix a damaged file
			// the goal of this process is to warn that a Object is damaged
			//
			//						
			readDrive = getReadDrive(bucket, objectName);
			metadata = readDrive.getObjectMetadata(bucket.getId(), objectName);

			if ((forceCheck) || (metadata.integrityCheck != null) && (metadata.integrityCheck.isAfter(thresholdDate))) {
				return true;
			}

			OffsetDateTime now = OffsetDateTime.now();

			String originalSha256 = metadata.sha256;

			if (originalSha256 == null) {
				metadata.integrityCheck = now;
				getVFS().getObjectMetadataCacheService().remove(metadata.bucketId, metadata.objectName);
				readDrive.saveObjectMetadata(metadata);
				return true;
			}

			File file = ((SimpleDrive) readDrive).getObjectDataFile(bucket.getId(), objectName);
			String sha256 = null;

			try {
				sha256 = ODFileUtils.calculateSHA256String(file);

			} catch (NoSuchAlgorithmException | IOException e) {
				logger.error(e);
				return false;
			}

			if (originalSha256.equals(sha256)) {
				metadata.integrityCheck = now;
				readDrive.saveObjectMetadata(metadata);
				return true;
			} else {
				logger.error("Integrity Check failed for -> d: " + readDrive.getName() + " | b:" + bucket.getName() + " | o:" + objectName + " | " + SharedConstant.NOT_THROWN);
			}
			/**
			 * it is not possible to fix the file if the integrity fails because there is no
			 * redundancy in RAID 0
			 **/
			return false;
		} finally {
			
			try {
				if (bucketLock)
					getLockService().getBucketLock(bucket.getId()).readLock().unlock();
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
			
			try {
				if (objectLock)
					getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
		}
	}

	/**
	 * 
	 */

	@Override
	public RedundancyLevel getRedundancyLevel() {
		return RedundancyLevel.RAID_0;
	}

	/**
	 * Scheduler goes to drive 0
	 */
	@Override
	public void saveScheduler(ServiceRequest request, String queueId) {
		getDrivesEnabled().get(0).saveScheduler(request, queueId);
	}

	/**
	 * 
	 */
	@Override
	public void removeScheduler(ServiceRequest request, String queueId) {
		getDrivesEnabled().get(0).removeScheduler(request, queueId);
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	
	/**
	 * 
	 */
	@Override
	public OdilonServerInfo getServerInfo() {
		try {
			getLockService().getServerLock().readLock().lock();
			File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
			if (file == null || !file.exists())
				return null;
			return getObjectMapper().readValue(file, OdilonServerInfo.class);
		} catch (IOException e) {
			logger.error(e);
			throw new InternalCriticalException(e);

		} finally {
			getLockService().getServerLock().readLock().unlock();
		}
	}

	
	/**
	 * 
	 */
	@Override
	public void setServerInfo(OdilonServerInfo serverInfo) {

		Check.requireNonNullArgument(serverInfo, "serverInfo is null");

		if (getServerInfo() == null) {
			saveNewServerInfo(serverInfo);
			return;
		}

		boolean done = false;
		boolean mayReqRestoreBackup = false;
		VFSOperation op = null;

		try {
			getLockService().getServerLock().writeLock().lock();
			op = getJournalService().updateServerMetadata();
			String jsonString = getObjectMapper().writeValueAsString(serverInfo);

			for (Drive drive: getDrivesAll()) {
				try {
					// drive.putSysFile(ServerConstant.ODILON_SERVER_METADATA_FILE, jsonString);
					// backup
				} catch (Exception e) {
					done = false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}

			mayReqRestoreBackup = true;

			for (Drive drive: getDrivesAll()) {
				try {
					drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
				} catch (Exception e) {
					done = false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			done = op.commit();

		} catch (Exception e) {
			logger.error(e, serverInfo.toString());
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

	/**
	 * <p>
	 * RAID 0 -> all enabled Drives have all buckets
	 * </p>
	 */
	@Override
	protected Map<String, ODBucket> getBucketsMap() {

		Map<String, ODBucket> map = new HashMap<String, ODBucket>();
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

		// any drive is ok because all have all the buckets
		Drive drive = getDrivesEnabled().get(0);
		for (DriveBucket bucket : drive.getBuckets()) {
			String name = bucket.getName();
			if (control.containsKey(name)) {
				Integer count = control.get(name);
				if (count == totalDrives) {
					ODBucket vfsbucket = new ODVFSBucket(bucket);
					map.put(vfsbucket.getName(), vfsbucket);
				}
			}
		}
		return map;
	}

	
	//protected Drive getObjectMetadataReadDrive(Long bucketId, String objectName) {
	//	return getReadDrive(bucketId, objectName);
	//}
	
	/**
	 * <p>
	 * RAID 0. there is only 1 Drive for each object
	 * </p>
	 * 
	 * @param bucketName
	 * @param objectName
	 * @return
	 */
	protected Drive getReadDrive(ODBucket bucket, String objectName) {
		return getDrive(bucket, objectName);
	}

	
	
	//protected Drive getReadDrive(Long bucketId, String objectName) {
	//	return getDrive(bucketId, objectName);
	//}

	protected Drive getReadDrive(ODBucket bucket) {
		return getDrive(bucket, null);
	}

	protected InputStream getInputStreamFromSelectedDrive(Drive readDrive, Long bucketId, String objectName) throws IOException {
		return Files.newInputStream(Paths.get(readDrive.getRootDirPath() + File.separator + bucketId.toString() + File.separator + objectName));
	}

	protected InputStream getInputStreamFromSelectedDrive(Drive readDrive, Long bucketId, String objectName, int version) throws IOException {
		return Files.newInputStream(Paths.get(readDrive.getRootDirPath() + File.separator + bucketId.toString() + File.separator
				+ VirtualFileSystemService.VERSION_DIR + File.separator + objectName
				+ VirtualFileSystemService.VERSION_EXTENSION + String.valueOf(version)));
	}

	/**
	 * 
	 */
	private void saveNewServerInfo(OdilonServerInfo serverInfo) {

		boolean done = false;
		VFSOperation op = null;

		try {
			
			getLockService().getServerLock().writeLock().lock();

			op = getJournalService().createServerMetadata();
			String jsonString = getObjectMapper().writeValueAsString(serverInfo);
			
			for (Drive drive: getDrivesAll()) {
				try {
					drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
				} catch (Exception e) {
					done = false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			done = op.commit();

		} catch (InternalCriticalException e) {
			throw(e);
			
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

	@Override
	public void syncObject(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		logger.error("not done", SharedConstant.NOT_THROWN);
	}


	@Override
	protected Drive getObjectMetadataReadDrive(ODBucket bucket, String objectName) {
		return getReadDrive(bucket, objectName);
	}


	/**
	 * <p>
	 * If the bucket does not exist on the selected Drive -> the system is in an
	 * illegal state
	 * </p>
	 */
	private ObjectMetadata getOM(ODBucket bucket, String objectName, Optional<Integer> o_version, boolean addToCacheifMiss) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
	 
		Drive readDrive = null;
		
		getLockService().getObjectLock(bucket.getId(), objectName).readLock().lock();
		
		try {
		
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
		
				/** read is from only 1 drive */
				readDrive = getReadDrive(bucket, objectName);
	
				if (!readDrive.existsBucket(bucket.getId()))
					throw new IllegalStateException("bucket -> b:" + bucket.getName() + " does not exist for -> d:" + readDrive.getName() + " | raid -> " + this.getClass().getSimpleName());
	
				ObjectMetadata meta = null;
	
				if (o_version.isPresent()) {
					meta = readDrive.getObjectMetadataVersion(bucket.getId(), objectName, o_version.get());
					meta.setBucketName(bucket.getName());
				}
				else  {
					meta = getObjectMetadataInternal(bucket, objectName, addToCacheifMiss);
				}
				
				if ((meta != null) && meta.isAccesible())
					return meta;
				else
					throw new OdilonObjectNotFoundException("ObjectMetadata does not exist");
				
			} catch (OdilonObjectNotFoundException e) {
				e.setErrorMessage("b:" + bucket.getName() + " "	+ (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null")	+ (o_version.isPresent() ? (", v:" + String.valueOf(o_version.get())) : ""));
				throw e;
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucket.getName() + ", o:" + objectName + ", d:"	+ (Optional.ofNullable(readDrive).isPresent() ? (readDrive.getName()) : "null")	+ (o_version.isPresent() ? (", v:" + String.valueOf(o_version.get())) : ""));
			} finally {
				getLockService().getBucketLock(bucket.getId()).readLock().unlock();
				
			}
		}finally {
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();
		}
	}


}
