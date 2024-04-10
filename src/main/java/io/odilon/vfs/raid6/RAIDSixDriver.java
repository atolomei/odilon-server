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
package io.odilon.vfs.raid6;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
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
import io.odilon.replication.ReplicationService;
import io.odilon.util.Check;
import io.odilon.vfs.BaseIODriver;
import io.odilon.vfs.ODDrive;
import io.odilon.vfs.ODVFSObject;
import io.odilon.vfs.model.BucketIterator;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>RAID 6. Driver</p>
 * <p>The coding convention for RS blocks is:
 * <ul>
 * <li><b>objectName.[chunk#].[block#]</b></li>
 * <li><b>objectName.[chunk#].[block#].v[version#]</b></li>
 *</ul>
 *where: <br/>
 *<ul>
 * <li><b>chunk#</b><br/> 	
 * 0..total_chunks, depending of the size of the file to encode ({@link ServerConstant.MAX_CHUNK_SIZE} is 32 MB)
 *this means that for files smaller or equal to 32 MB there will be only one chunk (chunk=0), for
 *files up to 64 MB there will be 2 chunks and so on.
 *<br/><br/>
 *</li>		
 * <li><b>block#</b><br/>
 * is the disk order [0..(data+parity-1)]
 * <br/><br/>
 * </li>
 * <li><b>version#</b><br/>
 * is omitted for head version.
 * <br/><br/>
 * </li>
 *</ul>
 *<p> The total number of files once the src file is encoded are:
 * <br/><br/>
 * (data+parity) * (file_size / MAX_CHUNK_SIZE ) rounded to the following integer. Examples:
 *</p>
 *<p> 
 * objectname.block#.disk# <br/>
 * <br/>
 * D:\odilon-data-raid6\drive0\bucket1\TOLOMEI.0.0 <br/>                                  
 * _______________________________________________________________ <br/>
 * <br/>                                       
 * D:\odilon-data-raid6\drive0\bucket1\TOLOMEI.0.0 <br/>
 * D:\odilon-data-raid6\drive1\bucket1\TOLOMEI.1.0 <br/>
 * D:\odilon-data-raid6\drive1\bucket1\TOLOMEI.2.0 <br/>
 *</p>
 * <p>
 * RAID 6. The only configurations supported in v1.x is -><br/>   
 * <br/>
 * data shards = 2 + parity shards=1 -> 3 disks <br/>
 * data shards = 4 + parity shards=2 -> 6 disks <br/>
 * </p>
 * <p>
 * All buckets <b>must</b> exist on all drives. If a bucket is not present on a
 * drive -> the bucket is considered "non existent".<br/>
 * Each file is stored only on 6 Drives. If a file does not have the file's
 * Metadata Directory -> the file is considered "non existent"
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
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
	 */
	@Override
	public void syncObject(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDSixSyncObjectHandler handler = new RAIDSixSyncObjectHandler(this);
		handler.sync(meta);
	}
	
	/**
	 * 
	 */
	@Override
	public InputStream getInputStream(VFSBucket bucket, String objectName) throws IOException {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ENABLED.getName() +" or " + BucketStatus.ENABLED.getName() + "  | b:" +  bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		String bucketName = bucket.getName();
		
		getLockService().getObjectLock(bucketName, objectName).readLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			try {
				
				Drive readDrive = getObjectMetadataReadDrive(bucketName, objectName);
	
				if (!readDrive.existsBucket(bucket.getName()))
					throw new IllegalStateException("b:" + bucketName+ " does not exist for drive -> d:" + readDrive.getName() + " | class -> " + this.getClass().getSimpleName());
	
				ObjectMetadata meta = getObjectMetadataInternal(bucketName, objectName, true);
	
				if ((meta != null) && meta.isAccesible()) {
					RAIDSixDecoder decoder = new RAIDSixDecoder(this);
					return (meta.encrypt) ? getVFS().getEncryptionService().decryptStream(Files.newInputStream(decoder.decodeHead(meta).toPath())) : Files.newInputStream(decoder.decodeHead(meta).toPath());
				}
				throw new OdilonObjectNotFoundException("b:" + bucketName + " | o:" + objectName + " | class:" + this.getClass().getSimpleName());
			
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucketName + ", o:"	+ objectName);
				
			} finally {
				getLockService().getObjectLock(bucketName, objectName).readLock().unlock();
			}
		} finally {
			getLockService().getBucketLock(bucketName).readLock().unlock();
		}
	}

	/**
	 * 
	 * 
	 */
	@Override
	public InputStream getObjectVersionInputStream(String bucketName, String objectName, int version) {
		
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
			
		getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			try {
				
				/** RAID 6: read is from any of the drives */
				Drive readDrive = getObjectMetadataReadDrive(bucketName, objectName);
				
				if (!readDrive.existsBucket(bucket.getName()))
					throw new IllegalStateException("bucket -> b:" +  bucket.getName() + " does not exist for -> d:" + readDrive.getName() + " | v:" + String.valueOf(version));
	
				ObjectMetadata meta = readDrive.getObjectMetadataVersion(bucket.getName(), objectName, version);
				
				if ((meta==null) || (!meta.isAccesible()))
					throw new OdilonObjectNotFoundException("object version does not exists for -> b:" +  bucket.getName() +" | o:" + objectName + " | v:" + String.valueOf(version));
				
				RAIDSixDecoder decoder = new RAIDSixDecoder(this);  
				File file = decoder.decodeVersion(meta);
				
				if (meta.encrypt)
					return getVFS().getEncryptionService().decryptStream(Files.newInputStream(file.toPath()));
				else
					return Files.newInputStream(file.toPath());
			}
			catch (OdilonObjectNotFoundException e) {
				throw e;
			}
			catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucketName + ", o:" + objectName + " | v:" + String.valueOf(version));
			}
			finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}


	/**
	 * <p>falta completar</p>
	 */
	@Override
	public boolean checkIntegrity(String bucketName, String objectName, boolean forceCheck) {
		
		// TODO
		
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null");

		OffsetDateTime thresholdDate = OffsetDateTime.now().minusDays(getVFS().getServerSettings().getIntegrityCheckDays());

		Drive readDrive = null;
		ObjectMetadata metadata = null;

		boolean objectLock = false;
		boolean bucketLock = false;

		try {
			
			try {
				objectLock = getLockService().getObjectLock(bucketName, objectName).readLock().tryLock(20, TimeUnit.SECONDS);
				if (!objectLock) {
					logger.error("Can not acquire read Lock for o: " + objectName + ". Assumes -> check is ok");
					return true;
				}
			} catch (InterruptedException e) {
				return true;
			}

			try {
				bucketLock = getLockService().getBucketLock(bucketName).readLock().tryLock(20, TimeUnit.SECONDS);
				if (!bucketLock) {
					logger.error("Can not acquire read Lock for b: " + bucketName + ". Assumes -> check is ok");
					return true;
				}
			} catch (InterruptedException e) {
				return true;
			}

			/** 
			 For RAID 0 the check is in the head version
			 there is no way to fix a damaged file
			 the goal of this process is to warn that a Object is damaged
			*/
			readDrive = getObjectMetadataReadDrive(bucketName, objectName);
			metadata = readDrive.getObjectMetadata(bucketName, objectName);

			if ((forceCheck) || (metadata.integrityCheck != null) && (metadata.integrityCheck.isAfter(thresholdDate))) 
				return true;

			return true;
			
			// OffsetDateTime now = OffsetDateTime.now();

			//String originalSha256 = metadata.sha256;

			//if (originalSha256 == null) {
			//	metadata.integrityCheck = now;
			//	getVFS().getObjec  tCacheService().rem ove(metadata.bucketName, metadata.objectName);
			//	readDrive.saveObjectMetadata(metadata);
			//	return true;
			//}

			//File file = ((SimpleDrive) readDrive).getObjectDataFile(bucketName, objectName);
			//String sha256 = null;
			//
			//try {
			//	sha256 = ODFileUtils.calculateSHA256String(file);
			//
			//} catch (NoSuchAlgorithmException | IOException e) {
			//	logger.error(e);
			//	return false;
			//}
			//
			// if (originalSha256.equals(sha256)) {
			//	metadata.integrityCheck = now;
			//	readDrive.saveObjectMetadata(metadata);
			//	return true;
			//} else {
			//	logger.error("Integrity Check failed for -> d: " + readDrive.getName() + " | b:" + bucketName + " | o:" + objectName + " | " + ServerConstant.NOT_THROWN);
			//}
			/**
			 * it is not possible to fix the file if the integrity fails because there is no
			 * redundancy in RAID 0
			 **/
			// return false;
			
		} finally {
			
			try {
				if (bucketLock)
					getLockService().getBucketLock(bucketName).readLock().unlock();
			} catch (Exception e) {
				logger.error(e, ServerConstant.NOT_THROWN);
			}
			
			try {
				if (objectLock)
					getLockService().getObjectLock(bucketName, objectName).readLock().unlock();
			} catch (Exception e) {
				logger.error(e, ServerConstant.NOT_THROWN);
			}
		}
		
	}
	
	
	/**
	 * 
	 * <p>This method is not ThreadSafe.
	 * The calling object must ensure concurrency control.
	 * 
	 * from VFS -> there is only one Thread active
	 * from Handler -> objects are locked before calling this
	 * 
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
			
			Check.requireNonNullArgument(op, "VFSOperation is null");
		
			switch (op.getOp()) {
				case CREATE_OBJECT: {
					RAIDSixCreateObjectHandler handler = new RAIDSixCreateObjectHandler(this);
					handler.rollbackJournal(op, recoveryMode);
					return;
				}
				case UPDATE_OBJECT: {
					RAIDSixUpdateObjectHandler handler = new RAIDSixUpdateObjectHandler(this);
					handler.rollbackJournal(op, recoveryMode);
					return;
				}
				case DELETE_OBJECT: {
					RAIDSixDeleteObjectHandler handler = new RAIDSixDeleteObjectHandler(this);
					handler.rollbackJournal(op, recoveryMode);
					return;
				}
				case DELETE_OBJECT_PREVIOUS_VERSIONS: {
					RAIDSixDeleteObjectHandler handler = new RAIDSixDeleteObjectHandler(this);
					handler.rollbackJournal(op, recoveryMode);
					return;
				}
				case UPDATE_OBJECT_METADATA: {
					RAIDSixUpdateObjectHandler handler = new RAIDSixUpdateObjectHandler(this);
					handler.rollbackJournal(op, recoveryMode);
					return;
				}
				default:
					break;
			}
			
			String bucketName = op.getBucketName();
			String objectName = op.getObjectName();
			
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
				throw new InternalCriticalException(e, op.toString());
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
		
		String bucketName = bucket.getName();
			
		getLockService().getObjectLock(bucketName , objectName).readLock().lock();
		
		try {
			getLockService().getBucketLock(bucketName ).readLock().lock();
			try {
				return getObjectMetadataReadDrive(bucketName, objectName).existsObjectMetadata(bucket.getName(), objectName);
			}
			catch (Exception e) {
				throw new InternalCriticalException(e, "b:"   + bucketName + ", o:" + objectName);
			}
			finally {
				getLockService().getBucketLock(bucketName).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(bucketName, objectName).readLock().unlock();
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
		updateAgent.updateObjectMetadataHeadVersion(meta);
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
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ENABLED.getName() +" or " + BucketStatus.ENABLED.getName() + "  | b:" +  bucket.getName());
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		
		String bucketName = bucket.getName();
		
		getLockService().getObjectLock(bucketName, objectName).readLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			try {
				
				/** read is from only one of the drive (randomly selected) drive */
				Drive readDrive = getObjectMetadataReadDrive(bucketName, objectName);
				
				if (!readDrive.existsBucket(bucketName))
					  throw new IllegalArgumentException("bucket control folder -> b:" +  bucket.getName() + " does not exist for drive -> d:" + readDrive.getName() +" | RAID -> " + this.getClass().getSimpleName());
	
				if (!exists(bucket, objectName))
					  throw new IllegalArgumentException("object does not exists for ->  b:" +  bucket.getName() +" | o:" + objectName + " | " + this.getClass().getSimpleName());			
	
				ObjectMetadata meta = getObjectMetadataInternal(bucketName, objectName, true);
				
				if (meta.status==ObjectStatus.ENABLED || meta.status==ObjectStatus.ARCHIVED) {
					return new ODVFSObject(bucket, objectName, getVFS());
				}
				
				/**
				 * if the object is DELETED  or DRAFT -> 
				 * it will be purged from the system at some point.
				 */									
				throw new OdilonObjectNotFoundException(String.format("object not found | status must be %s or %s -> b: %s | o:%s | o.status: %s",ObjectStatus.ENABLED.getName(),ObjectStatus.ARCHIVED.getName(),Optional.ofNullable(bucketName).orElse("null"),Optional.ofNullable(objectName).orElse("null"),meta.status.getName()));
			}
			catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucketName +  ", o:" + objectName);
			}
			finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				
			}
		} finally {
			getLockService().getObjectLock(bucket.getName(), objectName).readLock().unlock();
		}
	}
	
	/**
	 * 
	 */
	@Override
	public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);

		RAIDSixDeleteObjectHandler deleteAgent = new RAIDSixDeleteObjectHandler(this);
		deleteAgent.postObjectDelete(meta, headVersion);
	}

	/**
	 * 
	 */
	@Override
	public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta, int headVersion) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		RAIDSixDeleteObjectHandler deleteAgent = new RAIDSixDeleteObjectHandler(this);
		deleteAgent.postObjectPreviousVersionDeleteAll(meta, headVersion);
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
		
		getLockService().getObjectLock(bucket.getName(), objectName).readLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			try {
				/** read is from only 1 drive */
				readDrive = getObjectMetadataReadDrive(bucketName, objectName);
	
				if (!readDrive.existsBucket(bucketName))
					  throw new IllegalStateException("bucket -> b:" +  bucket.getName() + " does not exist for -> d:" + readDrive.getName() +" | raid -> " + this.getClass().getSimpleName());
	
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
				e.setErrorMessage((e.getMessage()!=null? (e.getMessage()+ " | ") : "") + "b:"   + bucketName + ", o:" + objectName + ", d:" + (Optional.ofNullable(readDrive).isPresent()  ? (readDrive.getName())  	: "null"));
				throw e;
			}
			catch (Exception e) {
				throw new InternalCriticalException(e, "b:"   + bucketName + ", o:" + objectName + ", d:" + (Optional.ofNullable(readDrive).isPresent()  ? (readDrive.getName())  	: "null"));
			}
			finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
			}
		} finally {
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
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		
		Check.requireNonNullStringArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:"+ bucketName);
		
		VFSBucket bucket = getVFS().getBucket(bucketName);
		
		Check.requireNonNullArgument(bucket, "bucket does not exist -> b:" + bucketName);		
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ARCHIVED.getName() +" or " + BucketStatus.ENABLED.getName() + ") | b:" + bucketName);
		
		if (!exists(bucket, objectName))
			throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
		
		RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this);
		agent.deleteObjectAllPreviousVersions(meta);
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
		logger.debug("Starting async process to set up drives");
		return getApplicationContext().getBean(RAIDSixDriveSetup.class, this).setup();
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
		BucketIteratorService walkerService = getVFS().getBucketIteratorService();
		
		try {
		
			if (serverAgentId.isPresent())
				walker = walkerService.get(serverAgentId.get());
			
			if (walker==null) {
				walker = new RAIDSixIterator(this, bucket.getName(), offset, prefix);
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
					logger.error(e, ServerConstant.NOT_THROWN);
					item = new Item<ObjectMetadata>(e);
				} catch (Exception e) {
					logger.error(e, ServerConstant.NOT_THROWN);
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
				getVFS().getBucketIteratorService().remove(walker.getAgentId());  /** closes the stream upon removal */
		}
	}
 	
	/**
	 * <p>RAID 6: return an enabled drive randomly, all enabled {@link Drive} contain the same ObjectMetadata</p>
	 */
	protected Drive getObjectMetadataReadDrive(String bucketName, String objectName) {
		return getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random()*1000)).intValue() % getDrivesEnabled().size());
	}
	
	/**
	 * <p>Object must be locked (either for reading or writing) before calling this method</p>
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param addToCacheIfmiss
	 * @return
	 */
	protected ObjectMetadata getObjectMetadataInternal(String bucketName, String objectName, boolean addToCacheIfmiss) {
		
		if ((!getVFS().getServerSettings().isUseObjectCache()))  {
			return getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadata(bucketName, objectName);
		}
		
		if (getVFS().getObjectMetadataCacheService().containsKey(bucketName, objectName)) {
			getVFS().getSystemMonitorService().getCacheObjectHitCounter().inc();
			return getVFS().getObjectMetadataCacheService().get(bucketName, objectName);
		}
		
		getVFS().getSystemMonitorService().getCacheObjectMissCounter().inc();
		ObjectMetadata meta = getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadata(bucketName, objectName);
		
		if (addToCacheIfmiss) 
			getVFS().getObjectMetadataCacheService().put(bucketName, objectName, meta);
		
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
			readDrive = getObjectMetadataReadDrive(bucketName, objectName);
			
			if (!readDrive.existsBucket(bucket.getName()))												
				  throw new IllegalArgumentException("b:" +  bucketName + " does not exist for -> d:" + readDrive.getName() +" | raid -> " + this.getClass().getSimpleName());

			if (!exists(bucket, objectName))
				  throw new IllegalArgumentException("b:" +  bucketName +" | o:" + objectName + " | class:" + this.getClass().getSimpleName());			

			if (o_version.isPresent())
				return readDrive.getObjectMetadataVersion(bucketName, objectName, o_version.get());
			else
		 		return getObjectMetadataInternal(bucketName, objectName, addToCacheifMiss);
		}
		catch (Exception e) {
			throw new InternalCriticalException(e, "b:"  + bucketName+	", o:" + objectName + ", d:" + readDrive.getName() + (o_version.isPresent()? (", v:" + String.valueOf(o_version.get())) :""));
		}
		finally {
			getLockService().getBucketLock(bucketName).readLock().unlock();
			getLockService().getObjectLock(bucketName, objectName).readLock().unlock();
		}
	}

 
	
	/**
	 * @param meta
	 * @param version
	 * @return
	 */
	protected Map<Drive, List<String>> getObjectDataFilesNames(ObjectMetadata meta, Optional<Integer> version) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		
		Map<Drive, List<String>> map = new HashMap<Drive, List<String>>();
								
		for(Drive drive: getDrivesAll())
			map.put(drive, new ArrayList<String>());
		 		
		int totalBlocks = meta.getSha256Blocks().size();

		int totalDisks = getVFS().getServerSettings().getRAID6DataDrives()+getVFS().getServerSettings().getRAID6ParityDrives();
		Check.checkTrue(totalDisks>0, "total disks must be greater than zero" );
		
		int chunks = totalBlocks / totalDisks;
		Check.checkTrue(chunks>0, "chunks must be greater than zero");
		
		for (int chunk=0; chunk<chunks; chunk++) {
					for (int disk=0; disk<getDrivesAll().size(); disk++) {
						String suffix = "."+String.valueOf(chunk)+"."+String.valueOf(disk) + (version.isEmpty() ? "" : (".v"+String.valueOf(version.get())));						
						Drive drive = getDrivesAll().get(disk);
						map.get(drive).add(meta.objectName+suffix);
					}
		}
		return map;
	}
	
	
	protected boolean isConfigurationValid(int dataShards, int parityShards) {
	  return getVFS().getServerSettings().isRAID6ConfigurationValid(dataShards, parityShards);	
	}
	
	/**
	 * 
	 * @param meta
	 * @param version
	 * @return
	 */
	protected List<File> getObjectDataFiles(ObjectMetadata meta, Optional<Integer> version) {
	
		List<File> files = new ArrayList<File>();
		
		if (meta==null)
			return files;
		
		int totalBlocks = meta.getSha256Blocks().size();

		int totalDisks = getVFS().getServerSettings().getRAID6DataDrives()+getVFS().getServerSettings().getRAID6ParityDrives();
		Check.checkTrue(totalDisks>0, "total disks must be greater than zero");
		
		int chunks = totalBlocks / totalDisks;
		Check.checkTrue(chunks>0, "chunks must be greater than zero");
		
		for (int chunk=0; chunk<chunks; chunk++) {
			for (int disk=0; disk<getDrivesAll().size(); disk++) {
				String suffix = "."+String.valueOf(chunk)+"."+String.valueOf(disk) + (version.isEmpty() ? "" : (".v"+String.valueOf(version.get())));						
				Drive drive = getDrivesAll().get(disk);
				if (version.isEmpty())
					files.add(new File(drive.getBucketObjectDataDirPath(meta.bucketName), meta.objectName+suffix));
				else
					 files.add(new File(drive.getBucketObjectDataDirPath(meta.bucketName)+File.separator + VirtualFileSystemService.VERSION_DIR, meta.objectName+suffix));
			}
		}
		return files;
	}
}
