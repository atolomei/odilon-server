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
import io.odilon.model.ServerConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.util.Check;
import io.odilon.vfs.RAIDDeleteObjectHandler;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneDeleteObjectHandler extends RAIDOneHandler implements  RAIDDeleteObjectHandler {

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
	@Override
	public void delete(VFSBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		String bucketName = bucket.getName();
		
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		if (!getDriver().exists(bucket, objectName))
			throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+objectName);
		
		VFSOperation op = null;
		boolean done = false;
		int headVersion = -1;
		ObjectMetadata meta = null;

		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
				
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			try {
											
				meta = getDriver().getReadDrive(bucketName, objectName).getObjectMetadata(bucket.getName(), objectName);
				headVersion = meta.version;
				op = getJournalService().deleteObject(bucketName, objectName, headVersion);
				
				backupMetadata(meta);
				
				for (Drive drive: getDriver().getDrivesAll()) 
					((SimpleDrive)drive).deleteObjectMetadata(bucketName, objectName);
				
				getVFS().getObjectCacheService().remove(bucketName, meta.objectName);
				
				done = op.commit();
				
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				throw e1;
				
			} catch (Exception e) {
				done=false;
				throw new InternalCriticalException(e, "op:" + op.getOp().getName() + ", b:"  + bucketName +	", o:" 	+ objectName);
			}
			finally {
				
				try {
					
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							throw new InternalCriticalException(e, "op:" + op.getOp().getName() + ", b:"  + bucketName +	", o:" 	+ objectName);
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
					logger.error(e, "op:" + op.getOp().getName() + ", b:"  + bucketName +	", o:" 	+ objectName, ServerConstant.NOT_THROWN);
				}
				finally {
					getLockService().getBucketLock(bucketName).readLock().unlock();
				
				}
			}
		} finally{
			getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();
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
	@Override
	public void wipeAllPreviousVersions() {
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
	@Override
	public void deleteBucketAllPreviousVersions(VFSBucket bucket) {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName()));
	}
	
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 */
	@Override
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {

		VFSOperation op = null;  
		boolean done = false;
		boolean isMainException = false;

		
		int headVersion = -1;

		String bucketName = meta.bucketName;
		String objectName =  meta.objectName;
		
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
		
			getLockService().getBucketLock(bucketName).readLock().lock();

			try {
				
					if (!getDriver().getReadDrive(bucketName, objectName).existsObjectMetadata(bucketName, objectName))
						throw new IllegalArgumentException("object does not exist -> b:" + bucketName+ " o:" + objectName);
		
					headVersion = meta.version;
					
					/** It does not delete the head version, only previous versions */
					if (meta.version==0)
						return;
					
					op = getJournalService().deleteObjectPreviousVersions(bucketName,objectName, headVersion);
					
					backupMetadata(meta);
		
					/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
					for (int version=0; version < headVersion; version++) { 
						for (Drive drive: getDriver().getDrivesAll()) {
							FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucketName, objectName, version));
						}
					}
		
					/** update head metadata with the tag */
					meta.addSystemTag("delete versions");
					meta.lastModified = OffsetDateTime.now();
					for (Drive drive: getDriver().getDrivesAll()) {
						ObjectMetadata metaDrive = drive.getObjectMetadata(bucketName, objectName);
						meta.drive=drive.getName();	
						drive.saveObjectMetadata(metaDrive);							
					}
					
					getVFS().getObjectCacheService().remove(bucketName, objectName);
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
									logger.error(e, "b:" + meta.bucketName + ", o:" + meta.objectName, ServerConstant.NOT_THROWN);
							}
						}
						else if (done) {
							postObjectPreviousVersionDeleteAllCommit(meta, headVersion);
						}
					}
					finally {
						getLockService().getBucketLock(meta.bucketName).readLock().unlock();

					}
				}
		} finally {
			getLockService().getObjectLock(meta.bucketName,meta.objectName).writeLock().unlock();			
		}

		if(done)
			onAfterCommit(op, meta, headVersion);
	}

		

	

	
	@Override
	public  void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		/** checked by the calling driver */
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue(op.getOp()==VFSop.DELETE_OBJECT || op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS, "VFSOperation invalid -> op: " + op.getOp().getName());
			
		String objectName = op.getObjectName();
		String bucketName = op.getBucketName();
		
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
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

	public void postObjectDelete(ObjectMetadata meta, int headVersion) 						{}
	public void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion) 	{}

	
	
	
	
	private void postObjectPreviousVersionDeleteAllCommit(ObjectMetadata meta, int headVersion) {
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		try {
			/** delete data versions(1..n-1). keep headVersion **/
			for (int n=0; n<headVersion; n++)	{
				for (Drive drive: getDriver().getDrivesAll()) 
					FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataVersionFile(bucketName, objectName, n));
			}
			
			/** delete backup Metadata */
			for (Drive drive: getDriver().getDrivesAll()) {
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketName) + File.separator + objectName));
			}
			
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
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
							FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataVersionFile(bucketName, objectName, n));
					}
					
					/** delete data (head) */
					for (Drive drive:getDriver().getDrivesAll())
						FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataFile(bucketName, objectName));
					
					/** delete backup Metadata */
					for (Drive drive:getDriver().getDrivesAll()) 
						FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketName) + File.separator + objectName));
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
			for (Drive drive: getDriver().getDrivesAll()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketName, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath( meta.bucketName) + File.separator +  meta.objectName;
				File src = new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:"   + (Optional.ofNullable(meta.bucketName).isPresent()    ? (meta.bucketName) :"null") +", o:" + (Optional.ofNullable(meta.objectName).isPresent() ? 	(meta.objectName) :"null"));
		}
	}
	
	
	private void restoreMetadata(String bucketName, String objectName) {
		
		/** restore metadata directory */
		for (Drive drive: getDriver().getDrivesAll()) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucketName) + File.separator + objectName;
			String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucketName, objectName);
			try {
				FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
			} catch (IOException e) {
				throw new InternalCriticalException(e, "b:"   + (Optional.ofNullable(bucketName).isPresent()    ? (bucketName) :"null") + ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null"));
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
			if (op.getOp()==VFSop.DELETE_OBJECT || op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS)
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), meta, headVersion));
		} catch (Exception e) {
			logger.error(e, " | " + ServerConstant.NOT_THROWN);
		}
	}


}
