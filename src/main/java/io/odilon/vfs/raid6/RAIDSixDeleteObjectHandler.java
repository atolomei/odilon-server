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

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

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
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;


/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixDeleteObjectHandler extends RAIDSixHandler implements RAIDDeleteObjectHandler {

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
	public void delete(@NonNull VFSBucket bucket, @NonNull String objectName) {
		
		 Check.requireNonNullArgument(bucket, "bucket is null");
		 Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		if (!getDriver().exists(bucket, objectName))
			throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
		
		String bucketName = bucket.getName();
		
		VFSOperation op = null;
		boolean done = false;
		int headVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			try {
				
				meta = getDriver().getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadata(bucket.getName(), objectName);
				
				headVersion = meta.version;
			
				op = getJournalService().deleteObject(bucketName, objectName, headVersion);
				
				backupMetadata(meta);
				
				getVFS().getObjectCacheService().remove(bucketName, meta.objectName);
				
				for (Drive drive: getDriver().getDrivesEnabled()) 
					drive.deleteObjectMetadata(bucket.getName(), objectName);
				
				done = op.commit();
				
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				logger.error(e1);
				throw e1;
				
			} catch (Exception e) {
				done=false;
				throw new InternalCriticalException(e, "op:" + op.getOp().getName() +	", b:"  + bucketName + ", o:" 	+ objectName);
			}
			finally {
				
				try {
					
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							throw new InternalCriticalException(e, "op:" + op.getOp().getName() +	", b:"  + bucketName + ", o:" 	+ objectName);
						}
					}
					else if (done) {
						postObjectDeleteCommit(meta, headVersion);
					}
	
					/**  DATA CONSISTENCY
					 *   ----------------
						 If The system crashes before Commit or Cancel -> next time the system starts up it will REDO all stored operations.
						 Also, the if there are error buckets in the drives, they will be normalized when the system starts. 
					 */
					
				} catch (Exception e) {
					logger.error(e, "op:" + op.getOp().getName() +	", b:"  + bucketName +", o:" 	+ objectName, ServerConstant.NOT_THROWN);
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
	
	public void wipeAllPreviousVersions() {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().
				getBean(DeleteBucketObjectPreviousVersionServiceRequest.class));
	}

	public void deleteBucketAllPreviousVersions(VFSBucket bucket) {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().
				getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName()));
	}

	/**
	 *
	 * 
	 */
	@Override
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta) {
	
		VFSOperation op = null;  
		boolean done = false;
		
		int headVersion = -1;
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
				getLockService().getBucketLock(bucketName).readLock().lock();
			
				try {
				
					if (!getDriver().getObjectMetadataReadDrive(bucketName, objectName).existsObjectMetadata(bucketName, objectName))
						throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucketName+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
					
					meta = getDriver().getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadata(bucketName, objectName);
					headVersion = meta.version;
					
					/** It does not delete the head version, 
					 * only previous versions */
					if (meta.version==0)
						return;
					
					op = getJournalService().deleteObjectPreviousVersions(bucketName, objectName, headVersion);
					
					backupMetadata(meta);
		
					/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
					for (int version=0; version < headVersion; version++)
						FileUtils.deleteQuietly(getDriver().getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadataVersionFile(bucketName, objectName, version));
		
					meta.addSystemTag("delete versions");
					meta.lastModified = OffsetDateTime.now();
											
					for (Drive drive: getDriver().getDrivesEnabled())
						drive.saveObjectMetadata(meta);
					
					getVFS().getObjectCacheService().remove(bucketName, meta.objectName);
					done=op.commit();
					
				
				} catch (OdilonObjectNotFoundException e1) {
					done=false;
					throw (e1);
				
				} catch (Exception e) {
					done=false;
					throw new InternalCriticalException(e, "b:"  + bucketName +", o:" 	+ objectName);
				}
				
				finally {
		
					try {
					
						if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
							} catch (Exception e) {
								throw new InternalCriticalException(e, "b:" + bucketName + ", o:" + objectName);
							}
						}
						else if (done) {
							postObjectPreviousVersionDeleteAllCommit(meta, headVersion);
						}
					}
					finally {
						getLockService().getBucketLock(bucketName).readLock().unlock();
					}
				}
		} finally {
			getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();
		}

		if (done)
			onAfterCommit(op, meta, headVersion);
		
	}

	/** not used by de moment */
	protected void postObjectDelete(ObjectMetadata meta, int headVersion) 						{}
	protected void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion) 	{}
	
	
	private void postObjectPreviousVersionDeleteAllCommit(@NonNull ObjectMetadata meta, int headVersion) {
					
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
	
	/**
	 * 
	 */
	@Override
	public void rollbackJournal(@NonNull VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");
		
		/** checked by the calling driver */
		Check.requireTrue(	op.getOp()==VFSop.DELETE_OBJECT ||op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS, "invalid -> op: " + op.getOp().getName());
			
		String objectName = op.getObjectName();
		String bucketName = op.getBucketName();
		
		Check.requireNonNullStringArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
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
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
		}
		finally {
			if (done || recoveryMode) 
				op.cancel();
		}
	}
	
	/**
	 * Sync
	 *  no need to locks
	 *  
	 * @param bucketName 
	 * @param objectName
	 * @param headVersion newest version of the Object just deleted
	 */
	private void postObjectDeleteCommit(@NonNull ObjectMetadata meta, int headVersion) {
	
		Check.requireNonNullArgument(meta, "meta is null");

		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		try {
			/** delete data versions(0..head-1) */
			for (int n=0; n<headVersion; n++)
				getDriver().getObjectDataFiles(meta, Optional.of(n)).forEach(item->FileUtils.deleteQuietly(item));
			
			/** delete data (head) */
			getDriver().getObjectDataFiles(meta, Optional.empty()).forEach(item->FileUtils.deleteQuietly(item));
			
			/** delete backup Metadata */
			for (Drive drive:getDriver().getDrivesAll())
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketName), objectName));
			}
		catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
		
	}
		

	
	
	/**
	 * copy metadata directory
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ObjectMetadata meta) {
	
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
	private void onAfterCommit(VFSOperation op, ObjectMetadata meta, int headVersion) {
		try {
			if (op.getOp()==VFSop.DELETE_OBJECT || op.getOp()==VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS)
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), meta, headVersion));
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
	
	/**
	 *  restore metadata directory 
	 * */
	private void restoreMetadata(String bucketName, String objectName) {
		for (Drive drive: getDriver().getDrivesEnabled()) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucketName) + File.separator + objectName;
			String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucketName, objectName);
			try {
				FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
			} catch (IOException e) {
				throw new InternalCriticalException(e, "b:" + bucketName + ", o:" + objectName);
			}
		}
	}




}
