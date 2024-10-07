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
import java.time.OffsetDateTime;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSOp;

/**
 * <p>RAID 1 Handler <br/>  
 * Delete methods ({@link VFSOp.DELETE_OBJECT})</p> * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneDeleteObjectHandler extends RAIDOneHandler  {

private static Logger logger = Logger.getLogger(RAIDOneDeleteObjectHandler.class.getName());

	/**
	 *  Instances of this class are used
	 * internally by {@link RAIDOneDriver}
	 * @param driver
	 */
	protected RAIDOneDeleteObjectHandler(RAIDOneDriver driver) {
		super(driver);
	}
	

	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
	 */
	protected void delete(ServerBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		
		
		Check.requireNonNullArgument(bucket.getId(), "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		if (!getDriver().exists(bucket, objectName))
			throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+objectName);
		
		VFSOperation op = null;
		boolean done = false;
		int headVersion = -1;
		ObjectMetadata meta = null;

		getLockService().getObjectLock(bucket.getId(), objectName).writeLock().lock();
		
		try {
				
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
											
				meta = getDriver().getReadDrive(bucket, objectName).getObjectMetadata(bucket.getId(), objectName);
				headVersion = meta.version;
				op = getJournalService().deleteObject(bucket.getId(), objectName, headVersion);
				
				backupMetadata(meta);
				
				for (Drive drive: getDriver().getDrivesAll()) 
					((SimpleDrive)drive).deleteObjectMetadata(bucket.getId(), objectName);
				
				done = op.commit();
				
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				throw e1;
				
			} catch (Exception e) {
				done=false;
				throw new InternalCriticalException(e, "op:" + op.getOp().getName() + ", b:"  + bucket.getName() +	", o:" 	+ objectName);
			}
			finally {
				
				try {
					
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							throw new InternalCriticalException(e, "op:" + op.getOp().getName() + ", b:"  + bucket.getName()  +	", o:" 	+ objectName);
						}
					}
					else if (done)
						postObjectDeleteCommit(meta, headVersion);
	
					/**  DATA CONSISTENCY
					 *   ----------------
						 If The system crashes before Commit or Cancel -> next time the system starts up it will REDO all stored operations.
						 Also, the if there are error buckets in the drives, they will be normalized when the system starts. 
					 */
					
				} catch (Exception e) {
					logger.error(e, "op:" + op.getOp().getName() + ", b:"  + bucket.getName() + ", o:" 	+ objectName, SharedConstant.NOT_THROWN);
				}
				finally {
					getLockService().getBucketLock(bucket.getId()).readLock().unlock();
				
				}
			}
		} finally{
			getLockService().getObjectLock(bucket.getId(), objectName).writeLock().unlock();
		}
			
			if(done) 
				onAfterCommit(op, meta, headVersion);
		}

	
	/**
	 * <p>Adds a {@link DeleteBucketObjectPreviousVersionServiceRequest} to the {@link SchedulerService} 
	 *  to walk through all objects and delete versions.
	 *  This process is Async and handler returns immediately.</p>
	 * 
	 * <p>The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads to scan all 
	 * Objects and remove previous versions. In case of failure (for example. the server is shutdown 
	 * before completion), it is retried up to 5 times.</p>
	 *  
	 * <p>Although the removal of all versions for every Object is transactional, 
	 * the {@link ServiceRequest} itself is not transactional, 
	 * and it can not be rollback</p>
	 */
	
	protected void wipeAllPreviousVersions() {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class));
	}

	
	/**
	 * <p>Adds a {@link DeleteBucketObjectPreviousVersionServiceRequest} to the {@link SchedulerService} 
	 * to walk through all objects and delete versions. 
	 * This process is Async and handler returns immediately.</p>
	 * 
	 * <p>The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads to scan all 
	 * Objects and remove previous versions. In case of failure (for example. the server is shutdown 
	 * before completion), it is retried up to 5 times.</p>
	 *  
	 * <p>Although the removal of all versions for every Object is transactional, the ServiceRequest 
	 * itself is not transactional, and it can not be rollback</p>
	 */
	
	protected void deleteBucketAllPreviousVersions(ServerBucket bucket) {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName(), bucket.getId()));
	}
	
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 */
	
	protected void deleteObjectAllPreviousVersions(ObjectMetadata meta) {

		Check.requireNonNullArgument(meta, "meta is null");
		
		VFSOperation op = null;  
		boolean done = false;
		boolean isMainException = false;

		int headVersion = -1;
		String objectName =  meta.objectName;
		
		getLockService().getObjectLock(meta.bucketId, objectName).writeLock().lock();
		
		try {
		
			getLockService().getBucketLock(meta.bucketId).readLock().lock();

			try {
				
					ServerBucket bucket = getDriver().getVFS().getBucketById(meta.bucketId);
				
					if (!getDriver().getReadDrive(bucket, objectName).existsObjectMetadata(meta.bucketId, objectName))
						throw new IllegalArgumentException("object does not exist -> b:" + meta.bucketId.toString() + " o:" + objectName);
		
					headVersion = meta.version;
					
					/** It does not delete the head version, only previous versions */
					if (meta.version==0)
						return;
					
					op = getJournalService().deleteObjectPreviousVersions(meta.bucketId,objectName, headVersion);
					
					backupMetadata(meta);
		
					/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
					for (int version=0; version < headVersion; version++) { 
						for (Drive drive: getDriver().getDrivesAll()) {
							FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(meta.bucketId, objectName, version));
						}
					}
		
					/** update head metadata with the tag */
					meta.addSystemTag("delete versions");
					meta.lastModified = OffsetDateTime.now();
					for (Drive drive: getDriver().getDrivesAll()) {
						ObjectMetadata metaDrive = drive.getObjectMetadata(meta.bucketId, objectName);
						meta.drive=drive.getName();	
						drive.saveObjectMetadata(metaDrive);							
					}
					
					done=op.commit();
					
				} catch (Exception e) {
					done=false;
					isMainException=true;
					throw new InternalCriticalException(e);
				}
				finally {
					try {
						if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
							} catch (Exception e) {
								if (!isMainException)
									throw new InternalCriticalException(e, "b:" + meta.bucketName + ", o:" + meta.objectName);
								else
									logger.error(e, "b:" + meta.bucketName + ", o:" + meta.objectName, SharedConstant.NOT_THROWN);
							}
						}
						else if (done) {
							postObjectPreviousVersionDeleteAllCommit(meta, headVersion);
						}
					}
					finally {
						getLockService().getBucketLock(meta.bucketId).readLock().unlock();

					}
				}
		} finally {
			getLockService().getObjectLock(meta.bucketId,meta.objectName).writeLock().unlock();			
		}

		if(done)
			onAfterCommit(op, meta, headVersion);
	}

	
	protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		/** checked by the calling driver */
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue(op.getOp()==VFSOp.DELETE_OBJECT || op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS, "VFSOperation invalid -> op: " + op.getOp().getName());
			
		String objectName = op.getObjectName();
		Long bucketId = op.getBucketId();
		
		//String bucketName = op.getBucketName();
		
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketId.toString());
		
		boolean done = false;
	 
		try {

			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			/** rollback is the same for both operations */
			if (op.getOp()==VFSOp.DELETE_OBJECT)
				restoreMetadata(bucketId,objectName);
			
			else if (op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
				restoreMetadata(bucketId,objectName);
			
			done=true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
		} catch (Exception e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
			else
				logger.error(msg);
				
		}
		finally {
			if (done || recoveryMode) 
				op.cancel();
		}
	}

	protected void postObjectDelete(ObjectMetadata meta, int headVersion) 						{}
	protected void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion) 	{}


	/**
	 * 
	 * 
	 */
	private void postObjectPreviousVersionDeleteAllCommit(ObjectMetadata meta, int headVersion) {
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		try {
			/** delete data versions(1..n-1). keep headVersion **/
			for (int n=0; n<headVersion; n++)	{
				for (Drive drive: getDriver().getDrivesAll()) 
					FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataVersionFile(meta.bucketId, objectName, n));
			}
			
			/** delete backup Metadata */
			for (Drive drive: getDriver().getDrivesAll()) {
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(meta.bucketId) + File.separator + objectName));
			}
			
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
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
	
	private void postObjectDeleteCommit(ObjectMetadata meta, int headVersion)  {
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
				
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		try {
			
					/** delete data versions(1..n-1) */
					for (int n=0; n<=headVersion; n++)	{
						for (Drive drive: getDriver().getDrivesAll())
							FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataVersionFile(meta.bucketId, objectName, n));
					}
					
					/** delete data (head) */
					for (Drive drive:getDriver().getDrivesAll())
						FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataFile(meta.bucketId, objectName));
					
					/** delete backup Metadata */
					for (Drive drive:getDriver().getDrivesAll()) 
						FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(meta.bucketId) + File.separator + objectName));
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ObjectMetadata meta) {
	
		/** copy metadata directory */
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketId, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(meta.bucketId) + File.separator +  meta.objectName;
				File src = new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:"   + (Optional.ofNullable(meta.bucketId).isPresent()    ? (meta.bucketName) :"null") +", o:" + (Optional.ofNullable(meta.objectName).isPresent() ? 	(meta.objectName) :"null"));
		}
	}
	
	
	private void restoreMetadata(Long bucketId, String objectName) {
		
		/** restore metadata directory */
		for (Drive drive: getDriver().getDrivesAll()) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucketId) + File.separator + objectName;
			String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucketId, objectName);
			try {
				FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
			} catch (IOException e) {
				throw new InternalCriticalException(e, "b:"   + (Optional.ofNullable(bucketId).isPresent()    ? (bucketId.toString()) :"null") + ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null"));
			}
		}
	}

	/**
	 * <p>This method is called after the TRX commit. It is used to clean temp files, if the system crashes those
	 * temp files will be removed on system startup</p>
	 * 
	 */
	private void onAfterCommit(VFSOperation op, ObjectMetadata meta, int headVersion) {
		try {
			if (op.getOp()==VFSOp.DELETE_OBJECT || op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), meta, headVersion));
		} catch (Exception e) {
			logger.error(e, " | " + SharedConstant.NOT_THROWN);
		}
	}


}
