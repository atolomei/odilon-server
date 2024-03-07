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
import java.time.OffsetDateTime;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;

public class RAIDSixDeleteObjectHandler extends RAIDSixHandler {

	private static Logger logger = Logger.getLogger(RAIDSixDeleteObjectHandler.class.getName());
	
	/**
	 * <p>Instances of this class are used
	 * internally by {@link RAIDSixDriver}</p>
	 * 
	 * @param driver
	 */
	protected RAIDSixDeleteObjectHandler(RAIDSixDriver driver) {
		super(driver);
	}
	
	/**
	 * @param bucket
	 * @param objectName
	 */
	public void delete(VFSBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		if (!getDriver().exists(bucket, objectName))
			throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
		
		VFSOperation op = null;
		boolean done = false;
		int headVersion = -1;
		
		getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();
		
		try {
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			ObjectMetadata meta = getDriver().getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket.getName(), objectName);
			headVersion = meta.version;
		
			op = getJournalService().deleteObject(bucket.getName(), objectName, headVersion);
			
			backupMetadata(meta);
			
			getVFS().getObjectCacheService().remove(bucket.getName(), meta.objectName);
			
			for (Drive drive: getDriver().getDrivesEnabled()) 
				drive.deleteObjectMetadata(bucket.getName(), objectName);
			
			done = op.commit();
			
		} catch (OdilonObjectNotFoundException e1) {
			done=false;
			logger.error(e1);
			throw e1;
			
		} catch (Exception e) {
			done=false;
			throw new InternalCriticalException(e, "op:" + op.getOp().getName() +	", b:"  + bucket.getName() +", o:" 	+ objectName);
		}
		finally {
			
			try {
				boolean requiresRollback = (!done) && (op!=null);
				if (requiresRollback) {
					try {
						rollbackJournal(op, false);
					} catch (Exception e) {
						throw new InternalCriticalException(e, "op:" + op.getOp().getName() +	", b:"  + bucket.getName() +", o:" 	+ objectName);
					}
				}

				/**  DATA CONSISTENCY
				 *   ----------------
					 If The system crashes before Commit or Cancel -> next time the system starts up it will REDO all stored operations.
					 Also, the if there are error buckets in the drives, they will be normalized when the system starts. 
				 */
				
			} catch (Exception e) {
				logger.error(e, "op:" + op.getOp().getName() +	", b:"  + bucket.getName() +", o:" 	+ objectName, ServerConstant.NOT_THROWN);
			}
			finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				getLockService().getObjectLock(bucket.getName() , objectName).writeLock().unlock();
			}
		}
		
		if(done)
			onAfterCommit(op, headVersion);
	}
	

	public void wipeAllPreviousVersions() {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class));
	}

	public void deleteBucketAllPreviousVersions(VFSBucket bucket) {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName()));
	}

	public void deleteObjectAllPreviousVersions(VFSBucket bucket, String objectName) {

		VFSOperation op = null;  
		boolean done = false;
		
		int headVersion = -1;

		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();

			boolean exists = getDriver().getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucket.getName(), objectName);
			
			if (!exists)
				throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
			
			ObjectMetadata meta = getDriver().getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket.getName(), objectName);
			headVersion = meta.version;
			
			/** It does not delete the head version, only previous versions */
			if (meta.version==0)
				return;
			
			op = getJournalService().deleteObjectPreviousVersions(bucket.getName(),objectName, headVersion);
			
			backupMetadata(meta);

			/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
			for (int version=0; version < headVersion; version++) {
				File metadataVersionFile = getDriver().getObjectMetadataReadDrive(bucket, objectName).getObjectMetadataVersionFile(bucket.getName(), objectName, version);
				FileUtils.deleteQuietly(metadataVersionFile);
			}

			meta.addSystemTag("delete versions");
			meta.lastModified = OffsetDateTime.now();
									
			for (Drive drive: getDriver().getDrivesEnabled())
				drive.saveObjectMetadata(meta);
			
			getVFS().getObjectCacheService().remove(bucket.getName(), meta.objectName);
			done=op.commit();
			
		
		} catch (OdilonObjectNotFoundException e1) {
			done=false;
			throw (e1);
		
		} catch (Exception e) {
			done=false;
			throw new InternalCriticalException(e);
		}
		
		finally {

			try {
			
				boolean requiresRollback = (!done) && (op!=null);
				
				if (requiresRollback) {
					try {
						
						rollbackJournal(op, false);
						
					} catch (Exception e) {
						String msg =  	"b:"   + (Optional.ofNullable(bucket).isPresent()    	? (bucket.getName()) :"null") + 
										", o:" + (Optional.ofNullable(objectName).isPresent() 	? (objectName)       :"null");   
						logger.error(e, msg);
						throw new InternalCriticalException(e);
					}
				}
			}
			finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
			}
		}

		if(done)
			onAfterCommit(op, headVersion);
		
	}

	
	public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta, int headVersion) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		try {
			/** delete data versions(0..headVersion-1). keep headVersion **/
			for (int n=0; n<headVersion; n++) {
				ObjectMetadata metaVersion = getDriver().getObjectMetadata(bucketName, objectName);
				if (metaVersion!=null)
					getDriver().getObjectDataFiles(meta, Optional.of(n)).forEach(item->FileUtils.deleteQuietly(item));
			}

			/** delete backup Metadata */
			for (Drive drive:getDriver().getDrivesEnabled())
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketName), objectName));
			
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}

	}


	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		if (logger.isDebugEnabled()) {
			/** checked by the calling driver */
			Check.requireNonNullArgument(op, "op is null");
			Check.requireTrue(	op.getOp()==VFSop.DELETE_OBJECT ||  
								op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS, "VFSOperation invalid -> op: " + op.getOp().getName());
		}

			
		String objectName = op.getObjectName();
		String bucketName = op.getBucketName();
		
		boolean done = false;
	 
		try {

			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			/** rollback is the same for both operations */
			if (op.getOp()==VFSop.DELETE_OBJECT)
				restoreMetadata(bucketName,objectName);
			
			else if (op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS)
				restoreMetadata(bucketName,objectName);
			
			done=true;
			
		} catch (InternalCriticalException e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			logger.error(msg);
			if (!recoveryMode)
				throw(e);
			
		} catch (Exception e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			logger.error(msg);
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
		}
		finally {
			if (done || recoveryMode) 
				op.cancel();
		}
	}
	
	/**
	 * 
	 * <p>This method is called <b>Async</b> by the Scheduler after the transaction is committed
	 * It does not require locking
	 * <br/>
	 * bucketName and objectName may not be the same called in other methods
	 * </p>
	 * 
	 * <p>
	 * data (head)
	 * data (versions)
	 * metadata dir (all versions) backup
	 * 
	 * metadata dir -> this was already deleted before the commit
	 * </p>
	 * @param bucketName 
	 * @param objectName
	 * @param headVersion newest version of the Object just deleted
	 */
	public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		try {
			
			/** delete data versions(0..head-1) */
			for (int n=0; n<headVersion; n++) {
				getDriver().getObjectDataFiles(meta, Optional.of(n)).forEach(item->FileUtils.deleteQuietly(item));
			}
			
			/** delete data (head) */
			getDriver().getObjectDataFiles(meta, Optional.empty()).forEach(item->FileUtils.deleteQuietly(item));
			
			/** delete backup Metadata */
			for (Drive drive:getDriver().getDrivesEnabled())
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketName), objectName));
		
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}

	
	
	/**
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ObjectMetadata meta) {
	
		/** copy metadata directory */
		try {
			for (Drive drive: getDriver().getDrivesEnabled()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketName, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName;
				File src = new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:" + meta.bucketName + ", o:" + meta.objectName);
		}
	}

	/**
	 * @param op
	 * @param headVersion
	 */
	private void onAfterCommit(VFSOperation op, int headVersion) {
		try {
			if (op.getOp()==VFSop.DELETE_OBJECT || op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS)
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), op.getBucketName(), op.getObjectName(), headVersion));
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
	
	
	private void restoreMetadata(String bucketName, String objectName) {
		/** restore metadata directory */
		for (Drive drive: getDriver().getDrivesEnabled()) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucketName) + File.separator + objectName;
			String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucketName, objectName);
			
			try {
				FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
				logger.debug("restore: " + objectMetadataBackupDirPath +" -> " +objectMetadataDirPath);
				
			} catch (IOException e) {
				String msg = 	"b:"   + (Optional.ofNullable(bucketName).isPresent()    ? (bucketName) :"null") + 
								", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null");  
				throw new InternalCriticalException(e, msg);
			}
		}
	}
}
