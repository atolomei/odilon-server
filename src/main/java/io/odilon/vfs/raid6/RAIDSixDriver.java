package io.odilon.vfs.raid6;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketStatus;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.query.BucketIteratorService;
import io.odilon.util.Check;
import io.odilon.vfs.BaseIODriver;
import io.odilon.vfs.ODVFSObject;
import io.odilon.vfs.model.BucketIterator;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>
 * RAID 6. 4+2
 * </p>
 * 
 * <p>
 * All buckets <b>must</b> exist on all drives. If a bucket is not present on a
 * drive -> the bucket is considered "non existent".<br/>
 * Each file is stored only on 6 Drives. If a file does not have the file's
 * Metadata Directory -> the file is considered "non existent"
 * </p>
 * 
 */
@ThreadSafe
@Component
@Scope("prototype")
public class RAIDSixDriver extends BaseIODriver implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(RAIDSixDriver.class.getName());

	@JsonIgnore
	private ApplicationContext applicationContext;
 
	public RAIDSixDriver(VirtualFileSystemService vfs, LockService vfsLockService) {
		super(vfs, vfsLockService);
	}

	
	/**
	 * 
	 * 
	 */
	@Override
	public InputStream getInputStream(VFSBucket bucket, String objectName) throws IOException {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		try {

			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
		
			Drive readDrive = getObjectMetadataReadDrive(bucket, objectName);

			if (!readDrive.existsBucket(bucket.getName()))
				throw new IllegalStateException("bucket -> b:" + bucket.getName() + " does not exist for drive -> d:" + readDrive.getName() + " | class -> " + this.getClass().getSimpleName());

			ObjectMetadata meta = getObjectMetadataInternal(bucket.getName(), objectName, true);

			if ((meta != null) && meta.isAccesible()) {

				RSDecoder decoder = new RSDecoder(this);
				
				File file = decoder.decode(bucket.getName(), objectName);
				
				if (meta.encrypt)
					return getVFS().getEncryptionService().decryptStream(Files.newInputStream(file.toPath()));
				else
					return Files.newInputStream(file.toPath());
			}
			throw new OdilonObjectNotFoundException("object does not exists for -> b:" + bucket.getName() + " | o:" + objectName + " | class:" + this.getClass().getSimpleName());
		
		} catch (Exception e) {
			
			final String msg = "b:" + (Optional.ofNullable(bucket).isPresent() ? (bucket.getName()) : "null") + ", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName) : "null");
			logger.error(e, msg);
			throw new InternalCriticalException(e, msg);
			
		} finally {
			getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}

	@Override
	public InputStream getObjectVersionInputStream(String bucketName, String objectName, int version) {
		throw new RuntimeException("not done");
		//return null;
	}

	
	@Override
	public boolean checkIntegrity(String bucketName, String objectName, boolean forceCheck) {
		throw new RuntimeException("not done");
		//return false;
	}
	
	
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		throw new RuntimeException("not done");
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext()  {
		return this.applicationContext;
	}
	
	
	@Override
	public VFSBucket createBucket(String bucketName) {return super.createBucket(bucketName);}

	@Override
	public void deleteBucket(VFSBucket bucket) {super.deleteBucket(bucket);	}

	@Override
	public boolean isEmpty(VFSBucket bucket) {return super.isEmpty(bucket);	}
	
	@Override
	public ObjectMetadata getObjectMetadata(String bucketName, String objectName) {
		return getOM(bucketName, objectName, Optional.empty(), true);
	}

	
	/**
	 * <p>Invariant: all drives contain the same bucket structure</p>
	 */
	@Override
	public boolean exists(VFSBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ENABLED.getName() +" or " + BucketStatus.ENABLED.getName() + "  | b:" +  bucket.getName());
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			return getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucket.getName(), objectName);
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
	 * 
	 */
	@Override
	public void putObject(VFSBucket bucket, String objectName, InputStream stream, String fileName,	String contentType) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:"+ bucket.getName());
		Check.requireNonNullStringArgument(fileName, "fileName is null | b: " + bucket.getName() + " o:" + objectName);
		Check.requireNonNullArgument(stream, "InpuStream can not null -> b:" + bucket.getName() + " | o:"+objectName);
		
		// TODO AT -> lock must be before creating the agent
		
		if (exists(bucket, objectName)) {
			RAIDSixUpdateObjectHandler updateAgent = new RAIDSixUpdateObjectHandler(this);
			updateAgent.update(bucket, objectName, stream, fileName, contentType);
			getVFS().getSystemMonitorService().getUpdateObjectCounter().inc();
		}
		else {
			RAIDSixCreateObjectHandler createAgent = new RAIDSixCreateObjectHandler(this);
			createAgent.create(bucket, objectName, stream, fileName, contentType);
			getVFS().getSystemMonitorService().getCreateObjectCounter().inc();
		}
	}

	/**
	 * 
	 */
	@Override
	public void putObjectMetadata(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDSixUpdateObjectHandler updateAgent = new RAIDSixUpdateObjectHandler(this);
		updateAgent.updateObjectMetadata(meta);
		getVFS().getSystemMonitorService().getUpdateObjectCounter().inc();
	}

	/**
	 * 
	 */
	@Override
	public VFSObject getObject(String bucketName, String objectName) {
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		VFSBucket bucket = getVFS().getBucket(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist -> " + bucketName);
		return getObject(bucket, objectName);
	}

	/**
	 * 
	 */
	@Override
	public VFSObject getObject(VFSBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. enabled or archived) b:" + bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		
		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			/** read is from only one of the drive (randomly selected) drive */
			Drive readDrive = getObjectMetadataReadDrive(bucket, objectName);
			
			if (!readDrive.existsBucket(bucket.getName()))
				  throw new IllegalArgumentException("bucket control folder -> b:" +  bucket.getName() + " does not exist for drive -> d:" + readDrive.getName() +" | RAID -> " + this.getClass().getSimpleName());

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
	 * 
	 */
	@Override
	public void postObjectDeleteTransaction(String bucketName, String objectName, int headVersion) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		RAIDSixDeleteObjectHandler deleteAgent = new RAIDSixDeleteObjectHandler(this);
		deleteAgent.postObjectDeleteTransaction(bucketName, objectName, headVersion);
	}

	/**
	 * 
	 */
	@Override
	public void postObjectPreviousVersionDeleteAllTransaction(String bucketName, String objectName, int headVersion) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		RAIDSixDeleteObjectHandler deleteAgent = new RAIDSixDeleteObjectHandler(this);
		deleteAgent.postObjectPreviousVersionDeleteAllTransaction(bucketName, objectName, headVersion);
	}

	

	/**
	 * 
	 */
	@Override
	public boolean hasVersions(String bucketName, String objectName) {
		return !getObjectMetadataVersionAll(bucketName,objectName).isEmpty();
	}

	/**
	 * 
	 */
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
			readDrive = getObjectMetadataReadDrive(bucket, objectName);

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
	
	/**
	 * 
	 */
	@Override
	public void wipeAllPreviousVersions() {
		RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
		agent.wipeAllPreviousVersions();
	}

	/**
	 * 
	 */
	@Override
	public void delete(VFSBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
		agent.delete(bucket, objectName);
	}
	
	/**
	 * 
	 */
	@Override
	public ObjectMetadata getObjectMetadataVersion(String bucketName, String objectName, int version) {
		return getOM(bucketName, objectName, Optional.of(Integer.valueOf(version)), true);
	}

	/**
	 * 
	 */
	@Override
	public ObjectMetadata restorePreviousVersion(String bucketName, String objectName) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		VFSBucket bucket = getVFS().getBucket(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		RAIDSixUpdateObjectHandler agent = new RAIDSixUpdateObjectHandler(this);
		return agent.restorePreviousVersion(bucket, objectName);
	}

	
	/**
	 * 
	 */
	@Override
	public void deleteObjectAllPreviousVersions(String bucketName, String objectName) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:"+ bucketName);
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);		
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		
		if (!exists(bucket, objectName))
			throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
		
		RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
		agent.deleteObjectAllPreviousVersions(bucket, objectName);

	}

	/**
	 * 
	 */
	@Override
	public void deleteBucketAllPreviousVersions(String bucketName) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		VFSBucket bucket = getVFS().getBucket(bucketName);
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		
		RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
		agent.deleteBucketAllPreviousVersions(bucket);
	}

	/**
	 * 
	 */
	@Override
	public RedundancyLevel getRedundancyLevel() {
		return RedundancyLevel.RAID_6;
	}

	/**
	 * 
	 */
	public void rollbackJournal(VFSOperation op) {
		rollbackJournal(op, false);
	}

	/**
	 * 
	 */
	@Override
	public boolean setUpDrives() {
		return false;
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
				walker = new RAIDSixBucketIterator(this, bucket.getName(), offset, prefix);
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
	 * <p>RAID 6: return any drive randomly, all {@link Drive} contain the same ObjectMetadata</p>
	 */
	protected Drive getObjectMetadataReadDrive(VFSBucket bucket, String objectName) {
		return getObjectMetadataReadDrive(bucket.getName(), objectName);
	}

	protected Drive getObjectMetadataReadDrive(String bucketName, String objectName) {
		return getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random()*1000)).intValue() % getDrivesEnabled().size());
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
			return getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadata(bucketName, objectName);
		}
		
		if (getVFS().getObjectCacheService().containsKey(bucketName, objectName)) {
			getVFS().getSystemMonitorService().getCacheObjectHitCounter().inc();
			return getVFS().getObjectCacheService().get(bucketName, objectName);
		}
		
		ObjectMetadata meta = getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadata(bucketName, objectName);
		getVFS().getSystemMonitorService().getCacheObjectMissCounter().inc();
		
		if (addToCacheIfmiss) {
			getVFS().getObjectCacheService().put(bucketName, objectName, meta);
		}
		
		return meta;
		
	}
	
	
	/**
	 *<p> RAID 6. Metadata read is from only 1 drive, selected randomly from all drives</p>
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
			readDrive = getObjectMetadataReadDrive(bucket, objectName);
			
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
