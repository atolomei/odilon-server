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
 * 
 * 
 * 
 */
@ThreadSafe
public class RAIDZeroDeleteObjectHandler extends RAIDZeroHandler implements  RAIDDeleteObjectHandler {
			
	private static Logger logger = Logger.getLogger(RAIDZeroDeleteObjectHandler.class.getName());
	
	protected RAIDZeroDeleteObjectHandler(RAIDZeroDriver driver) {
		super(driver);
	}
	
	/**
	 * <p>Adds a {@link DeleteBucketObjectPreviousVersionServiceRequest} to the {@link SchedulerService} to walk through all objects and delete versions. 
	 * This process is Async and handler returns immediately.</p>
	 * <p>The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads to scan all Objects and remove previous versions.
	 * In case of failure (for example. the server is shutdown before completion), it is retried up to 5 times.</p> 
	 * <p>Although the removal of all versions for every Object is transactional, the ServiceRequest itself is not transactional, 
	 * and it can not be rollback</p>
	 */
	@Override
	public void wipeAllPreviousVersions() {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class));
	
	}

	/**
	 * <p>Adds a {@link DeleteBucketObjectPreviousVersionServiceRequest} to the {@link SchedulerService} to walk through all objects and delete versions. 
	 * This process is Async and handler returns immediately.</p>
	 * <p>The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads to scan all Objects and remove previous versions.
	 * In case of failure (for example. the server is shutdown before completion), it is retried up to 5 times.</p> 
	 * <p>Although the removal of all versions for every Object is transactional, the ServiceRequest itself is not transactional, 
	 * and it can not be rollback</p>
	 */
	@Override
	public void deleteBucketAllPreviousVersions(VFSBucket bucket) {
			getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName()));
	}

	/**
	 * <p>It does not delete the head version, only previous versions</p>
	 *  
	 * @param bucket
	 * @param objectName
	 */
	@Override
	public void deleteObjectAllPreviousVersions(VFSBucket bucket, String objectName) {
		
		VFSOperation op = null;  
		boolean done = false;
		
		int headVersion = -1;

		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();

			boolean exists = getDriver().getReadDrive(bucket, objectName).existsObjectMetadata(bucket.getName(), objectName);
			
			if (!exists)
				throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
			
			ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, true);
			
			headVersion = meta.version;
			
			/** It does not delete the head version, only previous versions */
			if (meta.version==0)
				return;
									
			op = getJournalService().deleteObjectPreviousVersions(bucket.getName(), objectName, headVersion);
			
			backupMetadata(bucket, objectName);

			/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
			for (int version=0; version < headVersion; version++) {
				File metadataVersionFile = getDriver().getReadDrive(bucket.getName(), objectName).getObjectMetadataVersionFile(bucket.getName(), objectName, version);
				FileUtils.deleteQuietly(metadataVersionFile);
			}

			meta.addSystemTag("delete versions");
			meta.lastModified = OffsetDateTime.now();

			getDriver().getWriteDrive(bucket.getName(), objectName).saveObjectMetadata(meta);
			
			getVFS().getObjectCacheService().remove(meta.bucketName, meta.objectName);
			done=op.commit();
			
		} catch (Exception e) {
			done=false;
			logger.error(e);
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
	
	
	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
	 */
	@Override
	public void delete(VFSBucket bucket, String objectName) {
		
				
		VFSOperation op = null;  
		boolean done = false;
		
		Drive drive = getDriver().getDrive(bucket.getName(), objectName);
		int headVersion = -1;

		try {
			
			getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();

			if (!getDriver().exists(bucket, objectName))
				throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));

			ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, true);
			
			headVersion = meta.version;
			
			op = getJournalService().deleteObject(bucket.getName(), objectName,	headVersion);
			
			backupMetadata(bucket, objectName);
			
			
			drive.deleteObjectMetadata(bucket.getName(), objectName);
			
			getVFS().getObjectCacheService().remove(meta.bucketName, meta.objectName);
			done=op.commit();
			
		} catch (OdilonObjectNotFoundException e1) {
			done=false;
			logger.error(e1);
			throw e1;
		
		} catch (Exception e) {
			done=false;
			logger.error(e);
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
	 * 
	 */
	@Override
	public void postObjectPreviousVersionDeleteAllTransaction(String bucketName, String objectName, int headVersion) {

		try {
			
			/** delete data versions(1..n-1). keep headVersion **/
			for (int n=0; n<headVersion; n++)	{
				File version_n= ((SimpleDrive) getWriteDrive(bucketName, objectName)).getObjectDataVersionFile(bucketName, objectName, n);
				if (version_n.exists())
					FileUtils.deleteQuietly(version_n);	
			}
						
			/** delete backup Metadata */
			String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName;
			File omb = new File(objectMetadataBackupDirPath);
			if (omb.exists())
				FileUtils.deleteQuietly(omb);
			
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}

	/**
	 * <p>This method is executed <b>Async</b> by one of the worker threads of the {@link StandardSchedulerWorker}</p> 
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param headVersion
	 */
	@Override
	public void postObjectDeleteTransaction(String bucketName, String objectName, int headVersion) {
		try {
				/** delete data versions(1..n-1) **/
				for (int n=0; n<=headVersion; n++)	{
					File version_n= ((SimpleDrive) getWriteDrive(bucketName, objectName)).getObjectDataVersionFile(bucketName, objectName, n);
					if (version_n.exists())
						FileUtils.deleteQuietly(version_n);	
				}
				
				/** delete data (head) */
				File head= ((SimpleDrive) getWriteDrive(bucketName, objectName)).getObjectDataFile(bucketName, objectName);
				if (head.exists())
					FileUtils.deleteQuietly(head);
				
				/** delete backup Metadata */
				String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName;
				File omb = new File(objectMetadataBackupDirPath);
				
				if (omb.exists())
					FileUtils.deleteQuietly(omb);
				
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}

	/**
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(VFSBucket bucket, String objectName) {
	
		/** copy metadata directory */
		try {
			
			String objectMetadataDirPath = getDriver().getWriteDrive(bucket.getName(), objectName).getObjectMetadataDirPath(bucket.getName(), objectName);
			String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket.getName(), objectName).getBucketWorkDirPath(bucket.getName()) + File.separator + objectName;
			FileUtils.copyDirectory(new File(objectMetadataDirPath), new File(objectMetadataBackupDirPath));
			
		} catch (IOException e) {
			String msg = "b:"   + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) :"null") + 
						 ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)      :"null");  
			throw new InternalCriticalException(e, msg);
		}
	}

	/**
	 * 
	 * @param bucketName
	 * @param objectName
	 */
	private void restoreMetadata(String bucketName, String objectName) {

		/** restore metadata directory */
		String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName;
		String objectMetadataDirPath = getDriver().getWriteDrive(bucketName, objectName).getObjectMetadataDirPath(bucketName, objectName);
		
		try {
			FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
			logger.debug("restore: " + objectMetadataBackupDirPath +" -> " +objectMetadataDirPath);
			
		} catch (IOException e) {
			String msg = 	"b:"   + (Optional.ofNullable(bucketName).isPresent()    ? (bucketName) :"null") + 
							", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null");  
			throw new InternalCriticalException(e, msg);
		}
	}

	/**
	 * <p>This method is called after the TRX commit. It is used to clean temp files, if the system crashes those
	 * temp files will be removed on system startup</p>
	 * 
	 */
	private void onAfterCommit(VFSOperation op, int headVersion) {
		try {
			if (op.getOp()==VFSop.DELETE_OBJECT || op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS)
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), op.getBucketName(), op.getObjectName(), headVersion));
		} catch (Exception e) {
			logger.error(e);
		}
	}

	

}
