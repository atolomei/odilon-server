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
 * <p>RAID 0. Delete Handler</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDZeroDeleteObjectHandler extends RAIDZeroHandler implements  RAIDDeleteObjectHandler {
			
	private static Logger logger = Logger.getLogger(RAIDZeroDeleteObjectHandler.class.getName());
	
	protected RAIDZeroDeleteObjectHandler(RAIDZeroDriver driver) {
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
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		String bucketName = bucket.getName();
		
		VFSOperation op = null;  
		boolean done = false;
		boolean isMainException = false;
		
		Drive drive = getDriver().getDrive(bucket.getName(), objectName);
		int headVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			try {
	
				if (!getDriver().exists(bucket, objectName))
					throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
	
				meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, false);
				
				headVersion = meta.version;
				
				op = getJournalService().deleteObject(bucketName, objectName,	headVersion);
				
				backupMetadata(meta.bucketName, meta.objectName);
				
				drive.deleteObjectMetadata(bucketName, objectName);
				
				getVFS().getObjectCacheService().remove(meta.bucketName, meta.objectName);
				
				done=op.commit();
				
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				isMainException = true;
				throw e1;
			
			} catch (Exception e) {
				done=false;
				isMainException = true;
				throw new InternalCriticalException(e, "b:" + bucketName + ", o:" + objectName);
			}
			finally {

				try {
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							if (!isMainException)
								throw new InternalCriticalException(e, "b:" + bucketName + ", o:" + objectName);
							else
								logger.error(e, "b:" + bucketName + ", o:" + objectName, ServerConstant.NOT_THROWN);
						}
					}
					else if (done) { 
						try {
							postObjectDeleteCommit(meta, headVersion);
						} catch (Exception e) {
							logger.error(e, "b:" + bucketName + ", o:" + objectName, ServerConstant.NOT_THROWN);
						} 
					}
				}
				finally {
					getLockService().getBucketLock(bucketName).readLock().unlock();
				}
			}
		} finally {
			getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();
		}

		if(done)
			onAfterCommit(op, meta, headVersion);
	}
	
	
	/**
	 * <p>This method does <b>not</b> delete the head version, 
	 * only previous versions</p>
	 *  
	 * @param bucket
	 * @param objectName
	 */
	@Override
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		Check.requireNonNullArgument(meta.bucketName, "bucketName is null");
		Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketName);

		boolean isMainExcetion = false;
		int headVersion = -1;		  
		boolean done = false;
		VFSOperation op = null;

		getLockService().getObjectLock(meta.bucketName, meta.objectName).writeLock().lock();
		
		try {
				getLockService().getBucketLock(meta.bucketName).readLock().lock();
			
				try {
						
					if (!getDriver().getReadDrive(meta.bucketName, meta.objectName).existsObjectMetadata(meta.bucketName, meta.objectName))													
						throw new OdilonObjectNotFoundException("object does not exist -> b:" + meta.bucketName+ " o:" + meta.objectName);
					
					headVersion = meta.version;
					
					/** It does not delete the head version, 
					 * only previous versions */
					
					if (meta.version==0)
						return;
											
					op = getJournalService().deleteObjectPreviousVersions(meta.bucketName, meta.objectName, headVersion);
					
					backupMetadata(meta.bucketName, meta.objectName);
		
					/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
					for (int version=0; version < headVersion; version++) 
						FileUtils.deleteQuietly(getDriver().getReadDrive(meta.bucketName, meta.objectName).getObjectMetadataVersionFile(meta.bucketName, meta.objectName, version));
		
					meta.addSystemTag("delete versions");
					meta.lastModified = OffsetDateTime.now();
		
					getDriver().getWriteDrive(meta.bucketName, meta.objectName).saveObjectMetadata(meta);
					
					getVFS().getObjectCacheService().remove(meta.bucketName, meta.objectName);
					
					done=op.commit();
					
				} catch (InternalCriticalException e) {
					done=false;
					isMainExcetion=true;
					throw e;
					
				} catch (Exception e) {
					done=false;
					isMainExcetion=true;
					throw new InternalCriticalException(e, "b:" + meta.bucketName + " o:" + meta.objectName); 
				}
				finally {
		
					try {
					
						if ((!done) && (op!=null)) {
							try {
								
								rollbackJournal(op, false);
								
							} catch (InternalCriticalException e) {
								if (!isMainExcetion)
									throw e;
								else
									logger.error(e, "b:" + meta.bucketName + " o:" 	+ meta.objectName+" | " + ServerConstant.NOT_THROWN );
							} catch (Exception e) {
								if (!isMainExcetion)
									throw new InternalCriticalException(e, "b:" + meta.bucketName + " o:" 	+ meta.objectName);
								else
									logger.error(e, "b:" + meta.bucketName + " o:" 	+ meta.objectName+" | " + ServerConstant.NOT_THROWN );
							}
						}
						else if (done) {
							try {
								
								postObjectPreviousVersionDeleteAllCommit(meta, headVersion);
								
							} catch (Exception e) {
								logger.error(e, "b:" + meta.bucketName + " o:" 	+ meta.objectName+" | " + ServerConstant.NOT_THROWN);
							}
						}
					}
					finally {
						getLockService().getBucketLock(meta.bucketName).readLock().unlock();
					
					}
				}
		} finally {
				getLockService().getObjectLock(meta.bucketName, meta.objectName).writeLock().unlock();
		}
		
		if(done)
			onAfterCommit(op, meta, headVersion);
	}
	
	/**
	 * 
	 * 
	 * 
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {

		Check.requireNonNullArgument(op, "op is null");

		/** also checked by the calling driver */
		Check.requireTrue(op.getOp()==VFSop.DELETE_OBJECT || op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS, VFSOperation.class.getName() + " invalid -> op: " + op.getOp().getName());
		
		String objectName = op.getObjectName();
		String bucketName = op.getBucketName();
		
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		boolean done = false;
	 
		try {

			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			/** Rollback is the same for both operations ->
			 *  
			 * DELETE_OBJECT and  
			 * DELETE_OBJECT_PREVIOUS_VERSIONS
			 * 
			 * */
			if (op.getOp()==VFSop.DELETE_OBJECT)
				restoreMetadata(bucketName,objectName);
			
			else if (op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS)
				restoreMetadata(bucketName,objectName);
			
			done=true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null") + " | " + ServerConstant.NOT_THROWN );
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + op.toString());
			else
				logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null") + " | " + ServerConstant.NOT_THROWN );
		}
		finally {
			if (done || recoveryMode) 
				op.cancel();
		}
	}

	/**
	 * <p>Adds a {@link DeleteBucketObjectPreviousVersionServiceRequest} to the {@link SchedulerService} 
	 * to walk through all objects and delete versions. 
	 * This process is Async and handler returns immediately.</p>
	 * 
	 * <p>The {@link DeleteBucketObjectPreviousVersionServiceRequest} creates n Threads to scan all Objects 
	 * and remove previous versions.In case of failure (for example. the server is shutdown before completion), 
	 * it is retried up to 5 times.</p> 
	 * 
	 * <p>Although the removal of all versions for every Object is transactional, the {@link ServiceRequest} 
	 * itself is not transactional, and it can not be rollback</p>
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
	 * <p>This method is ThreadSafe <br/>
	 * It does not require lock because once previous versions have been deleted
	 * they can not be created again by another Thread</p> 
	 *  
	 */

	
	/** do nothing by the moment */
	public void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion) 	{}
	
	/** do nothing by the moment */
	public void postObjectDelete(ObjectMetadata meta, int headVersion) 						{}


	/**
	 * 
	 * @param meta
	 * @param headVersion
	 */
	private void postObjectPreviousVersionDeleteAllCommit(ObjectMetadata meta, int headVersion) {

		Check.requireNonNullArgument(meta, "meta is null");

		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
				
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		try {
			/** delete data versions(1..n-1). keep headVersion **/
			for (int n=0; n<headVersion; n++)
				FileUtils.deleteQuietly(((SimpleDrive) getWriteDrive(bucketName, objectName)).getObjectDataVersionFile(bucketName, objectName, n));	
						
			/** delete backup Metadata */
			FileUtils.deleteQuietly(new File(getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName));
			
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}

	/**
	 * <p>This method is executed by the delete thread.
	 * It does not need to control concurrent access because the caller method does it.
	 * It should also be fast since it is part of the main transactiion</p> 
	 * 
	 * @param meta
	 * @param headVersion
	 */
	private void postObjectDeleteCommit(ObjectMetadata meta, int headVersion) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		
		/** delete data versions(1..n-1) **/
		for (int n=0; n<=headVersion; n++)	
			FileUtils.deleteQuietly(((SimpleDrive) getWriteDrive(bucketName, objectName)).getObjectDataVersionFile(bucketName, objectName, n));	
		
		/** delete metadata (head) */
		/** not required because it was done before commit */
		
		/** delete data (head) */
		FileUtils.deleteQuietly(((SimpleDrive) getWriteDrive(bucketName, objectName)).getObjectDataFile(bucketName, objectName));
		
		/** delete backup Metadata */
		FileUtils.deleteQuietly(new File(getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName));
	}
	
	
	 
	 
	/**
	 * copy metadata directory
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(String bucketName, String objectName) {
		
		try {
			String objectMetadataDirPath = getDriver().getWriteDrive(bucketName, objectName).getObjectMetadataDirPath(bucketName, objectName);
			String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName;
			FileUtils.copyDirectory(new File(objectMetadataDirPath), new File(objectMetadataBackupDirPath));
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, "backupMetadata 	| b:" + bucketName + ", o:" +  objectName);
		}
	}

	/**
	 * restore metadata directory
	 * 
	 * @param bucketName
	 * @param objectName
	 */
	private void restoreMetadata(String bucketName, String objectName) {

		String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName;
		String objectMetadataDirPath = getDriver().getWriteDrive(bucketName, objectName).getObjectMetadataDirPath(bucketName, objectName);
		try {
			FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
		} catch (InternalCriticalException e) {
			throw e;
		
		} catch (IOException e) {
			throw new InternalCriticalException(e, "restoreMetadata | b:" + bucketName + ", o:" + objectName);
		}
	}

	/**
	 * <p>This method is called after the TRX commit. 
	 * It is used to clean temp files, if the system crashes 
	 * those temp files will be removed on system startup</p>
	 */
	private void onAfterCommit(VFSOperation op, ObjectMetadata meta, int headVersion) {
		
		if ((op==null) || (meta==null)) {
			logger.error("op or meta is null, should not happen", ServerConstant.NOT_THROWN);
			return;
		}
		
		try {
			if (op.getOp()==VFSop.DELETE_OBJECT || op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS)
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), meta, headVersion));
			
		} catch (Exception e) {
			logger.error(e, " onAfterCommit | " + op.toString() + " | " + ServerConstant.NOT_THROWN);
		}
	}

}
