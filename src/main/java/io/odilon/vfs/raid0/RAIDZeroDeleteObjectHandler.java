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
import io.odilon.vfs.RAIDDeleteObjectHandler;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSOp;

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
	public void delete(ServerBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		VFSOperation op = null;  
		boolean done = false;
		boolean isMainException = false;
		
		Drive drive = getDriver().getDrive(bucket, objectName);
		int headVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucket.getId(), objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
	
				if (!getDriver().exists(bucket, objectName))
					throw new OdilonObjectNotFoundException("object does not exist -> " + getDriver().objectInfo(bucket, objectName));
	
				meta = getDriver().getObjectMetadataInternal(bucket, objectName, false);
				
				headVersion = meta.version;
				
				op = getJournalService().deleteObject(bucket.getId(), objectName,	headVersion);
				
				backupMetadata(bucket, meta.objectName);
				
				drive.deleteObjectMetadata(bucket.getId(), objectName);
				
				done=op.commit();
				
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				isMainException = true;
				throw e1;
			
			} catch (Exception e) {
				done=false;
				isMainException = true;
				throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
			}
			finally {

				try {
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							if (!isMainException)
								throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
							else
								logger.error(e, getDriver().objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
						}
					}
					else if (done) { 
						try {
							postObjectDeleteCommit(meta, headVersion);
						} catch (Exception e) {
							logger.error(e, getDriver().objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
						} 
					}
				}
				finally {
					getLockService().getBucketLock(bucket.getId()).readLock().unlock();
				}
			}
		} finally {
			getLockService().getObjectLock(bucket.getId(), objectName).writeLock().unlock();
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
		Check.requireNonNullArgument(meta.bucketId, "bucketId is null");
		Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketId.toString());

		boolean isMainExcetion = false;
		int headVersion = -1;		  
		boolean done = false;
		VFSOperation op = null;

		
		ServerBucket bucket = getDriver().getVFS().getBucketById(meta.bucketId);
		
		getLockService().getObjectLock(meta.bucketId, meta.objectName).writeLock().lock();
		
		try {
				getLockService().getBucketLock(meta.bucketId).readLock().lock();
			
				try {
						
					if (!getDriver().getReadDrive(bucket, meta.objectName).existsObjectMetadata(meta.bucketId, meta.objectName))													
						throw new OdilonObjectNotFoundException("object does not exist -> "+ getDriver().objectInfo(meta.bucketId.toString(), meta.objectName));
					
					headVersion = meta.version;
					
					/** It does not delete the head version, 
					 * only previous versions */
					
					if (meta.version==0)
						return;
											
					op = getJournalService().deleteObjectPreviousVersions(meta.bucketId, meta.objectName, headVersion);
					
					backupMetadata(bucket, meta.objectName);
		
					/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
					for (int version=0; version < headVersion; version++) 
						FileUtils.deleteQuietly(getDriver().getReadDrive(bucket, meta.objectName).getObjectMetadataVersionFile(meta.bucketId, meta.objectName, version));
		
					meta.addSystemTag("delete versions");
					meta.lastModified = OffsetDateTime.now();
		
					getDriver().getWriteDrive(bucket, meta.objectName).saveObjectMetadata(meta);
					
					done=op.commit();
					
				} catch (InternalCriticalException e) {
					done=false;
					isMainExcetion=true;
					throw e;
					
				} catch (Exception e) {
					done=false;
					isMainExcetion=true;
					throw new InternalCriticalException(e, getDriver().objectInfo(meta.bucketId.toString(), meta.objectName)); 
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
									logger.error(e, getDriver().objectInfo(meta.bucketId.toString(), meta.objectName), SharedConstant.NOT_THROWN );
							} catch (Exception e) {
								if (!isMainExcetion)
									throw new InternalCriticalException(e, getDriver().objectInfo(meta.bucketId.toString(), meta.objectName));
								else
									logger.error(e, getDriver().objectInfo(meta.bucketId.toString(), meta.objectName), SharedConstant.NOT_THROWN );
							}
						}
						else if (done) {
							try {
								
								postObjectPreviousVersionDeleteAllCommit(meta, headVersion);
								
							} catch (Exception e) {
								logger.error(e, getDriver().objectInfo(meta.bucketId.toString(), meta.objectName), SharedConstant.NOT_THROWN);
							}
						}
					}
					finally {
						getLockService().getBucketLock(meta.bucketId).readLock().unlock();
					}
				}
		} finally {
				getLockService().getObjectLock(meta.bucketId, meta.objectName).writeLock().unlock();
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
		Check.requireTrue(op.getOp()==VFSOp.DELETE_OBJECT || op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS, VFSOperation.class.getName() + " invalid -> op: " + op.getOp().getName());
		
		String objectName = op.getObjectName();

		Long bucketId = op.getBucketId();
		
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketId.toString());
		
		boolean done = false;

		
		ServerBucket bucket = getVFS().getBucketById(op.getBucketId());
		
		try {

			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			/** Rollback is the same for both operations ->
			 *  
			 * DELETE_OBJECT and  
			 * DELETE_OBJECT_PREVIOUS_VERSIONS
			 * 
			 * */
			if (op.getOp()==VFSOp.DELETE_OBJECT)
				restoreMetadata(bucket,objectName);
			
			else if (op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
				restoreMetadata(bucket,objectName);
			
			done=true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error("Rollback " +  getDriver().opInfo(op), SharedConstant.NOT_THROWN );
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + op.toString());
			else
				logger.error("Rollback " +  getDriver().opInfo(op), SharedConstant.NOT_THROWN );
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
	public void deleteBucketAllPreviousVersions(ServerBucket bucket) {
			getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName(), bucket.getId()));
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

		String objectName = meta.objectName;
		Long bucketId=meta.bucketId;
				
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketId.toString());
		
		ServerBucket bucket = getDriver().getVFS().getBucketById(bucketId);
		
		try {
			/** delete data versions(1..n-1). keep headVersion **/
			for (int n=0; n<headVersion; n++)
				FileUtils.deleteQuietly(((SimpleDrive) getWriteDrive(bucket, objectName)).getObjectDataVersionFile(bucketId, objectName, n));	
						
			/** delete backup Metadata */
			FileUtils.deleteQuietly(new File(getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucketId) + File.separator + objectName));
			
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
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
		
		Long bucketId=meta.bucketId;
		String objectName = meta.objectName;
		
		ServerBucket bucket = getDriver().getVFS().getBucketById(bucketId);
		
		/** delete data versions(1..n-1) **/
		for (int n=0; n<=headVersion; n++)	
			FileUtils.deleteQuietly(((SimpleDrive) getWriteDrive(bucket, objectName)).getObjectDataVersionFile(bucketId, objectName, n));	
		
		/** delete metadata (head) */
		/** not required because it was done before commit */
		
		/** delete data (head) */
		FileUtils.deleteQuietly(((SimpleDrive) getWriteDrive(bucket, objectName)).getObjectDataFile(bucketId, objectName));
		
		/** delete backup Metadata */
		FileUtils.deleteQuietly(new File(getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucketId) + File.separator + objectName));
	}
	
	
	/**
	 * copy metadata directory
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ServerBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "meta is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		try {
			String objectMetadataDirPath = getDriver().getWriteDrive(bucket, objectName).getObjectMetadataDirPath(bucket.getId(), objectName);
			String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucket.getId()) + File.separator + objectName;
			FileUtils.copyDirectory(new File(objectMetadataDirPath), new File(objectMetadataBackupDirPath));
		} catch (IOException e) {
			throw new InternalCriticalException(e, "backupMetadata 	|  " + getDriver().objectInfo(bucket, objectName));
		}
	}

	/**
	 * 
	 * restore metadata directory
	 * 
	 * @param bucketName
	 * @param objectName
	 */					
	private void restoreMetadata(ServerBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "meta is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucket.getId()) + File.separator + objectName;
		String objectMetadataDirPath = getDriver().getWriteDrive(bucket, objectName).getObjectMetadataDirPath(bucket.getId(), objectName);
		try {
			FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
		} catch (InternalCriticalException e) {
			throw e;
		
		} catch (IOException e) {
			throw new InternalCriticalException(e, "restoreMetadata | " + getDriver().objectInfo(bucket, objectName));
		}
	}

	/**
	 * 
	 * <p>This method is called after the TRX commit. 
	 * It is used to clean temp files, if the system crashes 
	 * those temp files will be removed on system startup</p>
	 */
	private void onAfterCommit(VFSOperation op, ObjectMetadata meta, int headVersion) {
		
		if ((op==null) || (meta==null)) {
			logger.error("op or meta is null, should not happen", SharedConstant.NOT_THROWN);
			return;
		}
		
		try {
			if (op.getOp()==VFSOp.DELETE_OBJECT || op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), meta, headVersion));
			
		} catch (Exception e) {
			logger.error(e, " onAfterCommit | " + getDriver().opInfo(op), SharedConstant.NOT_THROWN);
		}
	}

}
