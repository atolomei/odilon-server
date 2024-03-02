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
package io.odilon.vfs.raid1;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.odilon.OdilonVersion;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;
import io.odilon.model.ServerConstant;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.query.BucketIteratorService;
import io.odilon.replication.ReplicationService;
import io.odilon.util.Check;
import io.odilon.util.ODFileUtils;
import io.odilon.vfs.BaseIODriver;
import io.odilon.vfs.ODDrive;
import io.odilon.vfs.ODVFSBucket;
import io.odilon.vfs.ODVFSObject;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.BucketIterator;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p><b>RAID 1</b></p>
 * <p>
 * {@link Bucket} structure is the same for all drives <br/>
 * {@link VirtualFileSystemService} checks consistency during startup.
 * </p>
 * <p>For each object, a copy is created on each {@link Drive}.</p> 
 */
@ThreadSafe
@Component
@Scope("prototype")
public class RAIDOneDriver extends BaseIODriver  {

	private static Logger logger = Logger.getLogger(RAIDOneDriver.class.getName());
	


	/**
	 * <p>
	 * @param vfs
	 * @param vfsLockService
	 * </p>
	 */
	public RAIDOneDriver(VirtualFileSystemService vfs, LockService vfsLockService) {
		super(vfs, vfsLockService);
	}

	
	@Override
	public boolean hasVersions(String bucketName, String objectName) {
		return !getObjectMetadataVersionAll(bucketName,objectName).isEmpty();
	}
	
	@Override
	public void wipeAllPreviousVersions() {
		RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
		agent.wipeAllPreviousVersions();
	}

	@Override
	public void deleteBucketAllPreviousVersions(String bucketName) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		
		RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
		agent.deleteBucketAllPreviousVersions(bucket);
	}

	/**
	 * 
	 * 
	 */
	public ObjectMetadata restorePreviousVersion(String bucketName, String objectName) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);

		RAIDOneUpdateObjectHandler agent = new RAIDOneUpdateObjectHandler(this);
		return agent.restorePreviousVersion(bucket, objectName);
	}

	
	/**
	 * <p>
	 * If version does not exist -> throws OdilonObjectNotFoundException
	 * </p>
	 */
	public InputStream getObjectVersionInputStream(String bucketName, String objectName, int version) {
		
		Check.requireNonNullArgument(bucketName, "bucket is null");
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
				
		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			/** RAID 1: read is from any of the drives */
			Drive readDrive = getReadDrive(bucket, objectName);
			
			if (!readDrive.existsBucket(bucket.getName()))
				throw new IllegalStateException("bucket -> b:" +  bucket.getName() + " does not exist for -> d:" + readDrive.getName() + " | v:" + String.valueOf(version));

			ObjectMetadata meta = getObjectMetadataVersion(bucket.getName(), objectName, version);
			
			if ((meta!=null) && meta.isAccesible()) {
				
				InputStream stream = getInputStreamFromSelectedDrive(readDrive, bucket.getName(), objectName, version);
				
				if (meta.encrypt)
					return getVFS().getEncryptionService().decryptStream(stream);
				else
					return stream;	
			}
			else
				throw new OdilonObjectNotFoundException("object version does not exists for -> b:" +  bucket.getName() +" | o:" + objectName + " | v:" + String.valueOf(version));
			
		}
		catch ( OdilonObjectNotFoundException e) {
			logger.error(e);
			throw e;
		}
		catch (Exception e) {
				final String msg = "b:"   + (Optional.ofNullable( bucket).isPresent()    ? (bucket.getName()) :"null") + ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") + " | v:" + String.valueOf(version);
				logger.error(e, msg);
				throw new InternalCriticalException(e, msg);
		}
		finally {
			
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}
	
	@Override
	public void deleteObjectAllPreviousVersions(String bucketName, String objectName) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:"+ bucketName);
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);		
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		
		if (!exists(bucket, objectName))
			throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
		
		RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
		agent.deleteObjectAllPreviousVersions(bucket, objectName);
	}	
	
	

	/**
	 * 
	 * 
	 */
	public void putObject(VFSBucket bucket, String objectName, InputStream stream, String fileName, String contentType) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:"+ bucket.getName());
		Check.requireNonNullStringArgument(fileName, "fileName is null | b: " + bucket.getName() + " o:" + objectName);
		Check.requireNonNullArgument(stream, "InpuStream can not null -> b:" + bucket.getName() + " | o:"+objectName);
		
		// TODO AT -> lock must be before creating the agent
		if (exists(bucket, objectName)) {
			RAIDOneUpdateObjectHandler updateAgent = new RAIDOneUpdateObjectHandler(this);
			updateAgent.update(bucket, objectName, stream, fileName, contentType);
			getVFS().getSystemMonitorService().getUpdateObjectCounter().inc();
		}
		else {
			RAIDOneCreateObjectHandler createAgent = new RAIDOneCreateObjectHandler(this);
			createAgent.create(bucket, objectName, stream, fileName, contentType);
			getVFS().getSystemMonitorService().getCreateObjectCounter().inc();
		}
	}

	/**
	 * <p>RAID 1: Delete from all Drives</p> 
	 */
	@Override
	public void delete(VFSBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		RAIDOneDeleteObjectHandler agent = new RAIDOneDeleteObjectHandler(this);
		agent.delete(bucket, objectName);
	}

	/**
	 * <p>THis method is executed Async by the SchedulerService </p>  
	 */
	@Override
	public void postObjectDeleteTransaction(String bucketName, String objectName, int headVersion) {
	
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		RAIDOneDeleteObjectHandler deleteAgent = new RAIDOneDeleteObjectHandler(this);
		deleteAgent.postObjectDeleteTransaction(bucketName, objectName, headVersion);
		
	}

	/**
	 * 
	 */
	@Override
	public void postObjectPreviousVersionDeleteAllTransaction(String bucketName, String objectName, int headVersion) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		RAIDOneDeleteObjectHandler deleteAgent = new RAIDOneDeleteObjectHandler(this);
		deleteAgent.postObjectPreviousVersionDeleteAllTransaction(bucketName, objectName, headVersion);
	}

	/**
	 * 
	 */
	@Override			
	public void putObjectMetadata(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDOneUpdateObjectHandler updateAgent = new RAIDOneUpdateObjectHandler(this);
		updateAgent.updateObjectMetadata(meta);
		getVFS().getSystemMonitorService().getUpdateObjectCounter().inc();
	}
	
	
	/**
	 * <p>The process is Async for RAID 1</p>
	 */
	@Override
	public boolean setUpDrives() {
		logger.debug("Starting async process to set up drives");
		RAIDOneDriveSetup setup = getApplicationContext().getBean(RAIDOneDriveSetup.class, this);
		return setup.setup();
	}
	
	
	
	
	/**
	 * <p></p>
	 */
	@Override
	public VFSBucket createBucket(String bucketName) {
	
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		
		VFSOperation op = null;
		boolean done = false;
		
		BucketMetadata meta = new BucketMetadata(bucketName);
		
		meta.status = BucketStatus.ENABLED;
		meta.appVersion = OdilonVersion.VERSION;
		
		OffsetDateTime now = OffsetDateTime.now();
		
		VFSBucket bucket = new ODVFSBucket(meta);

		try {
		
			getLockService().getBucketLock(bucketName).writeLock().lock();

			if (getVFS().existsBucket(bucketName))
				throw new IllegalArgumentException("bucket already exist | b: " + bucketName);
			
			op = getJournalService().createBucket(bucketName);
			
			meta.creationDate=now;
			meta.lastModified=now;

			// ALL --
			
			for (Drive drive: getDrivesAll()) {
				try {
					drive.createBucket(bucketName, meta);
				} catch (Exception e) {
					done=false;
					logger.error(e);
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			
			done=op.commit();
			return bucket;
		}
		finally {
			try {
				if (done) {
					getVFS().getBucketsCache().put(bucket.getName(), bucket);
				}
				else {
					if (op!=null)
						rollbackJournal(op);
				}
				
			} catch (Exception e) {
				logger.error(e, ServerConstant.NOT_THROWN);
			}
			finally {
				getLockService().getBucketLock(bucketName).writeLock().unlock();
			}
		}
	}
	
	/**
	 * <p>@param bucket bucket must exist in the system</p> 
	 */
	public void deleteBucket(VFSBucket bucket) {
		getVFS().removeBucket(bucket);
		
	}
	
	/**
	 * <p>RAID 1 -> Read drive can be any from the pool</p>
	 */
	@Override
	public boolean isEmpty(VFSBucket bucket) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(existsBucketInDrives(bucket.getName()), "bucket does not exist -> b: " + bucket.getName());
		
		try {
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			return getReadDrive(bucket).isEmpty(bucket.getName());
		}
		catch (Exception e) {
				String msg = "b:" + (Optional.ofNullable( bucket).isPresent() ? (bucket.getName()) :"null"); 
				logger.error(e, msg);
				throw new InternalCriticalException(e, msg);
				 
		} finally {						
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
		}
	}

	
	@Override
	public List<ObjectMetadata> getObjectMetadataVersionAll(String bucketName, String objectName) {
		
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
	
		List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
		
		Drive readDrive = null;
		
		try {

			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			/** read is from only 1 drive */
			readDrive = getReadDrive(bucket, objectName);

			if (!readDrive.existsBucket(bucket.getName()))
				  throw new IllegalStateException("CRITICAL ERROR | bucket -> b:" +  bucket.getName() + " does not exist for -> d:" + readDrive.getName() +" | raid -> " + this.getClass().getSimpleName());

			ObjectMetadata meta = getObjectMetadataInternal(bucketName, objectName, true);
			
			if ((meta==null) || (!meta.isAccesible()))
				throw new OdilonObjectNotFoundException(ObjectMetadata.class.getName() + " does not exist");

			if (meta.version==0)
				return list;
			
			for (int version=0; version<meta.version; version++) {
				ObjectMetadata meta_version = readDrive.getObjectMetadataVersion(bucketName, objectName, version);
				if (meta_version!=null)
					list.add(meta_version);
			}
			
			return list;
			
		}
		catch (OdilonObjectNotFoundException e) {
			String msg = 	"b:"   + (Optional.ofNullable( bucket).isPresent()    ? (bucket.getName()) 		: "null") + 
							", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       		: "null") +
							", d:" + (Optional.ofNullable(readDrive).isPresent()  ? (readDrive.getName())  	: "null"); 
			
			e.setErrorMessage((e.getMessage()!=null? (e.getMessage()+ " | ") : "") + msg);
			throw e;
		}
		catch (Exception e) {
			String msg = 	"b:"   + (Optional.ofNullable( bucket).isPresent()    ? (bucket.getName()) 		: "null") + 
							", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       		: "null") +
							", d:" + (Optional.ofNullable(readDrive).isPresent()  ? (readDrive.getName())  	: "null"); 
							
			logger.error(e, msg);
			throw new InternalCriticalException(e, msg);
		}
		finally {
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}
	
	
	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String objectName) {
			return getOM(bucketName, objectName, Optional.empty(), true);
	}
	
	@Override
	public ObjectMetadata getObjectMetadataVersion(String bucketName, String objectName, int version) {
		return getOM(bucketName, objectName, Optional.of(Integer.valueOf(version)), true);		
	}
	
	

	/**
	 * 
	 */
	public VFSObject getObject(String bucketName, String objectName) {
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		VFSBucket bucket = getVFS().getBucket(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist -> " + bucketName);
		return getObject(bucket, objectName);
	}
	
	/**
	 * 
	 */
	public VFSObject getObject(VFSBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		
		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			/** read is from only one of the drive (randomly selected) drive */
			Drive readDrive = getReadDrive(bucket, objectName);
			
			if (!readDrive.existsBucket(bucket.getName()))
				  throw new IllegalArgumentException("bucket control folder -> b:" +  bucket.getName() + " does not exist for drive -> d:" + readDrive.getName() +" | Raid -> " + this.getClass().getSimpleName());

			if (!exists(bucket, objectName))
				  throw new IllegalArgumentException("object does not exists for ->  b:" +  bucket.getName() +" | o:" + objectName + " | " + this.getClass().getSimpleName());			

			ObjectMetadata meta = getObjectMetadataInternal(bucket.getName(), objectName, true);
			
			if (meta.status==ObjectStatus.ENABLED || meta.status==ObjectStatus.ARCHIVED) {
				return new ODVFSObject(bucket, objectName, getVFS());
			}
			
			/**
			 * if the object is DELETED  or DRAFT -> it will be purged from the system at some point.
			 */
			throw new OdilonObjectNotFoundException(		String.format("object not found | status must be %s or %s -> b: %s | o:%s | o.status: %s", 
															ObjectStatus.ENABLED.getName(),
															ObjectStatus.ARCHIVED.getName(),
															Optional.ofNullable(bucket.getName()).orElse("null"), 
															Optional.ofNullable(bucket.getName()).orElse("null"),
															meta.status.getName())
													);
		}
		catch (Exception e) {
			String msg = "b:"   + (Optional.ofNullable( bucket).isPresent()    ? (bucket.getName()) :"null") + 
						 ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null");
			logger.error(e, msg);
			throw new InternalCriticalException(e, msg);
		}
		finally {
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}

	/**
	 * <p>Invariant: all drives contain the same bucket structure</p>
	 */
	@Override
	public boolean exists(VFSBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			return getReadDrive(bucket, objectName).existsObjectMetadata(bucket.getName(), objectName);
		}
		catch (Exception e) {
			String msg = 	"b:"   + (Optional.ofNullable( bucket).isPresent()    ? (bucket.getName()) :"null") + 
							", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null");
			logger.error(e, msg);
			throw new InternalCriticalException(e, msg);
		}
		finally {
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}
	
	/**
	 * <p>Weak Consistency.<br/> 
	 * If a file gives error while bulding the {@link DataList}, 
	 * the Item will contain an String with the error
	 * {code isOK()} should be used before getObject()</p>
	 */
	@Override
	public DataList<Item<ObjectMetadata>> listObjects(String bucketName, Optional<Long> offset, Optional<Integer> pageSize, Optional<String> prefix, Optional<String> serverAgentId) {
		
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		BucketIterator walker = null;
		BucketIteratorService walkerService = getVFS().getWalkerService();
		
		try {
			if (serverAgentId.isPresent())
				walker = walkerService.get(serverAgentId.get());
			
			if (walker==null) {
				walker = new RAIDOneBucketIterator(this, bucket.getName(), offset, prefix);
				walkerService.register(walker);
			}
			
			List<Item<ObjectMetadata>> list =  new ArrayList<Item<ObjectMetadata>>();

			int size = pageSize.orElseGet(() -> ServerConstant.DEFAULT_PAGE_SIZE);
			
			int counter = 0;
			
			while (walker.hasNext() && counter++<size) {
				Item<ObjectMetadata> item;
				try {
					Path path = walker.next();
					String objectName = path.toFile().getName();
					item = new Item<ObjectMetadata>(getObjectMetadata(bucketName,objectName));
				
				} catch (IllegalMonitorStateException e) {
					logger.debug(e);
					item = new Item<ObjectMetadata>(e);
				} catch (Exception e) {
					logger.error(e);
					item = new Item<ObjectMetadata>(e);
				}
				list.add(item);
			}
		
			DataList<Item<ObjectMetadata>> result = new DataList<Item<ObjectMetadata>>(list);
			
			if (!walker.hasNext())
				result.setEOD(true);
			
			result.setOffset(walker.getOffset());
			result.setPageSize(size);
			result.setAgentId(walker.getAgentId());
			
			return result;
			
		} finally {

			if (walker!=null && (!walker.hasNext()))
				/**{@link WalkerService} closes the stream upon removal */
				getVFS().getWalkerService().remove(walker.getAgentId());
		}
	}

	/**
	 * <b>IMPORTANT</b> -> caller must close the {@link InputStream} returned
	 * @param bucketName
	 * @param objectName
	 * @return
	 * @throws IOException 
	 */
	public InputStream getInputStream(VFSBucket bucket, String objectName) throws IOException {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		try {
		
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();

			/** read is from only 1 drive, randomly selected */
			Drive readDrive = getReadDrive(bucket, objectName);

			if (!readDrive.existsBucket(bucket.getName())) {
				logger.error("bucket -> b:" +  bucket.getName() + " does not exist for drive -> d:" + readDrive.getName() +" | class -> " + this.getClass().getSimpleName());
				throw new IllegalStateException("bucket -> b:" +  bucket.getName() + " does not exist for drive -> d:" + readDrive.getName() +" | class -> " + this.getClass().getSimpleName());
			}
			
			ObjectMetadata meta = getObjectMetadataInternal(bucket.getName(), objectName, true);
			
			InputStream stream = getInputStreamFromSelectedDrive(readDrive, bucket.getName(), objectName);

			if (meta.encrypt)
				return getVFS().getEncryptionService().decryptStream(stream);
			else
				return stream;
		}
		catch (Exception e) {
				String msg = "b:"   + (Optional.ofNullable( bucket).isPresent()    ? (bucket.getName()) :"null") + 
							 ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null");
				logger.error(e, msg);
				throw new InternalCriticalException(e, msg);
		}
		finally {
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}

	/**
	 * <p>returns true if the object integrity is ok
	 * or if it can be fixed for all copies
	 * </p>
	 * <p>if it can not be fixed for at least one copy it returns false </p>
	 */
	@Override
	public boolean checkIntegrity(String bucketName, String objectName, boolean forceCheck) {

		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null");
		
		OffsetDateTime thresholdDate = OffsetDateTime.now().minusDays(getVFS().getServerSettings().getIntegrityCheckDays());
		
		boolean retValue = true;

		Boolean iCheck[] = new Boolean[ getDrivesEnabled().size() ];
		{
			int total = getDrivesEnabled().size();
			for (int p=0; p<total; p++)
				iCheck[p] = Boolean.valueOf(true);
		}
		
		Drive goodDrive = null;
		ObjectMetadata goodDriveMeta = null;
		
		boolean objectLock = false;
		boolean bucketLock = false;
		
		try {
			
			try {
				objectLock = getLockService().getObjectLock(bucketName, objectName).readLock().tryLock(10, TimeUnit.SECONDS);
				if(!objectLock) {
					logger.warn("Can not acquire read Lock for Object. Assumes check is ok -> " + objectName);
					return true;
				}
			}
			catch (InterruptedException e) {
				logger.warn(e);
				return true;
			}
			
			try {
				bucketLock = getLockService().getBucketLock(bucketName).readLock().tryLock(10, TimeUnit.SECONDS);
				if(!bucketLock) {
					logger.warn("Can not acquire read Lock for Bucket. Assumes check is ok -> " + bucketName);
					return true;
				}
			}
			catch (InterruptedException e) {
				logger.warn(e);
				return true;
			}
			
			int n = 0;
			
			for (Drive drive: getDrivesEnabled()) {
				
				ObjectMetadata meta = drive.getObjectMetadata(bucketName, objectName);
				
				if ((forceCheck) || (meta.integrityCheck==null) || (meta.integrityCheck.isBefore(thresholdDate))) {
				
					String originalSha256 = meta.sha256;
					String sha256 = null;
					
					File file = ((SimpleDrive) drive).getObjectDataFile(bucketName, objectName);
					
					try {

						sha256 = ODFileUtils.calculateSHA256String(file);
						
						if (originalSha256==null) {
							meta.sha256=sha256;
							originalSha256=sha256;
						}
								
						if (originalSha256.equals(sha256)) {
							
							if (goodDrive==null)
								goodDrive=drive;
							
							meta.integrityCheck=OffsetDateTime.now();
							drive.saveObjectMetadata(meta);
							iCheck[n] = Boolean.valueOf(true);
							
						}
						else {
							logger.error("Integrity Check failed for -> d: " + drive.getName() + " | b:"+ bucketName + " | o:" + objectName);
							iCheck[n]=Boolean.valueOf(false);
						}
					
					} catch (NoSuchAlgorithmException | IOException e) {
						logger.error(e);
						iCheck[n]=Boolean.valueOf(false);
					}	
				}
				else
					iCheck[n]=Boolean.valueOf(true);
				n++;
			}

			retValue = true;
			
			int total = iCheck.length;
			for (int p=0; p<total; p++) {
				if (!iCheck[p].booleanValue()) {
					retValue=false;
					if (goodDrive!=null)
						goodDriveMeta=goodDrive.getObjectMetadata(bucketName, objectName);
					break;
				}
			}
		}
		finally {
			
			if (bucketLock)
				getLockService().getBucketLock(bucketName).readLock().unlock();

			if (objectLock)
				getLockService().getObjectLock(bucketName, objectName).readLock().unlock();
		}
		
		if (bucketLock && objectLock && (!retValue)) {
			if (goodDrive!=null) {
				if (goodDriveMeta==null)
					goodDriveMeta=goodDrive.getObjectMetadata(bucketName, objectName);
				retValue = fix(bucketName, objectName, goodDriveMeta, iCheck, goodDrive);
			}
		}
		
		return retValue;
	}

	/**
	 * 
	 */
	@Override
	public RedundancyLevel getRedundancyLevel() {
		return RedundancyLevel.RAID_1; 
	}

	
	

	
	
	/**				
	 * @param op
	 * @param bucket
	 * @param objectName
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
	
		
		
		Check.requireNonNullArgument(op, "VFSOperation is null");
		
		if (op.getOp()==VFSop.CREATE_OBJECT) {
			RAIDOneCreateObjectHandler handler = new RAIDOneCreateObjectHandler(this);
			handler.rollbackJournal(op, recoveryMode);
			return;
		}
		else if (op.getOp()==VFSop.UPDATE_OBJECT) {
			RAIDOneUpdateObjectHandler handler = new RAIDOneUpdateObjectHandler(this);
			handler.rollbackJournal(op, recoveryMode);
			return;
			
		}
		else if (op.getOp()==VFSop.DELETE_OBJECT) {
			RAIDOneDeleteObjectHandler handler = new RAIDOneDeleteObjectHandler(this);
			handler.rollbackJournal(op, recoveryMode);
			return;
		}
		
		else if (op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS) {
			RAIDOneDeleteObjectHandler handler = new RAIDOneDeleteObjectHandler(this);
			handler.rollbackJournal(op, recoveryMode);
			return;
		}
		
		else if (op.getOp()==VFSop.UPDATE_OBJECT_METADATA) {
			RAIDOneUpdateObjectHandler handler = new RAIDOneUpdateObjectHandler(this);
			handler.rollbackJournal(op, recoveryMode);
			return;
		}

		

		String objectName = op.getObjectName();
		String bucketName = op.getBucketName();
		
		boolean done = false;
		
		
		try {
			
			if (getVFS().getServerSettings().isStandByEnabled()) {
				ReplicationService rs = getVFS().getReplicationService();
				rs.cancel(op);
			}
			
			else if (op.getOp() == VFSop.CREATE_SERVER_MASTERKEY) {
				for (Drive drive : getDrivesAll()) {
					File file = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
					if ((file != null) && file.exists())
						FileUtils.forceDelete(file);
				}
				done = true;
			}
			else if (op.getOp()==VFSop.CREATE_BUCKET) {
				for (Drive drive: getDrivesAll())
					((ODDrive) drive).forceDeleteBucket(bucketName);
				done=true;
			}
			else if (op.getOp()==VFSop.UPDATE_BUCKET) {
				logger.error("not done -> "  +op.getOp() + " | " + bucketName );
				done=true;
			}
			else if (op.getOp()==VFSop.DELETE_BUCKET) {
				for (Drive drive: getDrivesAll())
					drive.markAsEnabledBucket(bucketName);
				done=true;
			}
			else if (op.getOp()==VFSop.CREATE_SERVER_METADATA) {
				if (objectName!=null) {
					for (Drive drive: getDrivesAll()) {
						drive.removeSysFile(op.getObjectName());
					}
				}
				done=true;
			}
			else if (op.getOp()==VFSop.UPDATE_SERVER_METADATA) {
				if (objectName!=null) {
					logger.error("not done -> "  +op.getOp() + " | " + bucketName );
				}
				done=true;
			}
			
			
		} catch (Exception e) {
			logger.error(e);
			throw new InternalCriticalException(e);
		}
		finally {
			if (done) {
				op.cancel();
			}
			else {
				if (getVFS().getServerSettings().isRecoverMode()) {
					logger.error("---------------------------------------------------------------");
					logger.error("Cancelling failed operation -> " + op.toString());
					logger.error("---------------------------------------------------------------");
					op.cancel();
				}
			}
		}
	}
	
	/**  DATA CONSISTENCY
	 *   ---------------
		 The system crashes before Commit or Cancel -> next time the system starts up it will 
		 CANCEL all operations that are incomplete.  
		 REDO in this version means deleting the object from the drives where it completed
	 */

	/**
	 * <p>RAID 1: return any drive randomly, all {@link Drive} contain the same data</p>
	 */
	protected Drive getReadDrive(VFSBucket bucket, String objectName) {
		return getReadDrive(bucket.getName(), objectName);
	}

	protected Drive getReadDrive(String bucketName, String objectName) {
		return getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random()*1000)).intValue() % getDrivesEnabled().size());
	}
	
	protected Drive getReadDrive(VFSBucket bucket) {
		return getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random()*1000)).intValue() % getDrivesEnabled().size());
	}

		
	


	protected InputStream getInputStreamFromSelectedDrive(Drive readDrive, String bucketName, String objectName) throws IOException {
		return Files.newInputStream(Paths.get(readDrive.getRootDirPath() + File.separator + bucketName + File.separator + objectName));
	}
	
	protected InputStream getInputStreamFromSelectedDrive(Drive readDrive, String bucketName, String objectName, int version) throws IOException {
		return Files.newInputStream(Paths.get(readDrive.getRootDirPath() + File.separator + bucketName + File.separator + VirtualFileSystemService.VERSION_DIR + File.separator + objectName + VirtualFileSystemService.VERSION_EXTENSION + String.valueOf(version)));
	}
	
	 
	/**
	 * 
	 */
	private boolean fix(String bucketName, String objectName, ObjectMetadata goodDriveMeta, Boolean[] iCheck, Drive goodDrive) {
		
		Check.requireNonNullArgument(goodDrive, "goodDrive is null");
		Check.requireNonNullArgument(goodDriveMeta, "goodDriveMeta is null");
		
		boolean retValue = true;

		try {
		
			getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			ObjectMetadata currentMeta = goodDrive.getObjectMetadata(bucketName, objectName);
			
			if (!currentMeta.lastModified.equals(goodDriveMeta.lastModified))
				return true;
			
			iCheck[0] = true;
			iCheck[1] = false; 
			
			int total = iCheck.length;
			
			for (int p=0; p<total; p++) {
				
				if (!iCheck[p].booleanValue()) {
					SimpleDrive destDrive = (SimpleDrive) getDrivesEnabled().get(p);
					try {
						if (!goodDrive.equals(destDrive)) {
							InputStream in = ((SimpleDrive) goodDrive).getObjectInputStream(bucketName, objectName);
							destDrive.putObjectStream(bucketName, objectName, in);
							goodDriveMeta.drive=destDrive.getName();
							destDrive.saveObjectMetadata(goodDriveMeta);
							logger.debug("Fixed -> d: " + destDrive.getName() + " | b:"+ bucketName + " | o:" + objectName);
						}
						
					} catch (IOException e) {
							logger.error(e);
							retValue = false;
					}
				}
			}			
		}
		catch (Exception e) {
			logger.error(e);
			retValue = false;
		}
		finally {
			getLockService().getBucketLock(bucketName).readLock().unlock();
			getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();
		}
		return retValue;
	}

	/**
	 * 
	 * <p>Object must be locked (either for reading or writing) before calling this method</p>
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param addToCacheIfmiss
	 * @return
	 */
	protected ObjectMetadata getObjectMetadataInternal(String bucketName, String objectName, boolean addToCacheIfmiss) {
		
		
		if ((!getVFS().getServerSettings().isUseObjectCache()) || (getVFS().getObjectCacheService().size() >= MAX_CACHE_SIZE))  {
			return getReadDrive(bucketName, objectName).getObjectMetadata(bucketName, objectName);
		}
		
		if (getVFS().getObjectCacheService().containsKey(bucketName, objectName)) {
			getVFS().getSystemMonitorService().getCacheObjectHitCounter().inc();
			return getVFS().getObjectCacheService().get(bucketName, objectName);
		}
		
		ObjectMetadata meta = getReadDrive(bucketName, objectName).getObjectMetadata(bucketName, objectName);
		getVFS().getSystemMonitorService().getCacheObjectMissCounter().inc();
		
		if (addToCacheIfmiss) {
			getVFS().getObjectCacheService().put(bucketName, objectName, meta);
		}
		
		return meta;
		
	}
	/**
	 *<p> RAID 1. read is from only 1 drive, selected randomly from all drives</p>
	 */
	private ObjectMetadata getOM(String bucketName, String objectName, Optional<Integer> o_version, boolean addToCacheifMiss) {
		
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);

		Drive readDrive = null;
		
		try {

			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			/** read is from only 1 drive */
			readDrive = getReadDrive(bucket, objectName);
			
			if (!readDrive.existsBucket(bucket.getName()))												
				  throw new IllegalArgumentException("bucket -> b:" +  bucket.getName() + " does not exist for -> d:" + readDrive.getName() +" | raid -> " + this.getClass().getSimpleName());

			if (!exists(bucket, objectName))
				  throw new IllegalArgumentException("Object does not exists for ->  b:" +  bucket.getName() +" | o:" + objectName + " | class:" + this.getClass().getSimpleName());			

			if (o_version.isPresent())
				return readDrive.getObjectMetadataVersion(bucketName, objectName, o_version.get());
			else
		 		return getObjectMetadataInternal(bucketName, objectName, addToCacheifMiss);
		}
		catch (Exception e) {
			
			String msg = 	"b:"   + (Optional.ofNullable( bucket).isPresent()    ? (bucket.getName()) 		: "null") + 
							", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       		: "null") +
							", d:" + (Optional.ofNullable(readDrive).isPresent()  ? (readDrive.getName())  	: "null") +
							(o_version.isPresent()? (", v:" + String.valueOf(o_version.get())) :"");
			logger.error(e, msg);
			throw new InternalCriticalException(e, msg);
		}
		finally {
			
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}

}
