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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AfterDeleteObjectServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSOp;

/**
 * <p>RAID 6. Delete Object handler</p>
 * <p>Auxiliary class used by {@link RaidSixHandler}</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
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
	protected void delete(@NonNull ServerBucket bucket, @NonNull String objectName) {
		
		 Check.requireNonNullArgument(bucket, "bucket is null");
		 Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		if (!getDriver().exists(bucket, objectName))
			throw new IllegalArgumentException("object does not exist -> " + getDriver().objectInfo(bucket, objectName));
		
		Long bucketId = bucket.getId();
		
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		int headVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucketId, objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucketId).readLock().lock();
			
			try {
				
				meta = getDriver().getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucketId, objectName);
				
				headVersion = meta.version;
			
				op = getJournalService().deleteObject(bucketId, objectName, headVersion);
				
				backupMetadata(meta);
				
				for (Drive drive: getDriver().getDrivesAll()) 
					drive.deleteObjectMetadata(bucket.getId(), objectName);
				
				done = op.commit();
				
			} catch (Exception e) {
				done=false;
				isMainException = true;
				throw new InternalCriticalException(e, "op:" + op.getOp().getName() +	getDriver().objectInfo(bucket, objectName));
			}
			finally {
				
				try {
					
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							if (!isMainException)
								throw new InternalCriticalException(e, "op:" + op.getOp().getName() +	" , "  + getDriver().objectInfo(bucket, objectName));
							else
								logger.error(e, "op:" + op.getOp().getName() +	","  + getDriver().objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
						}
					}
					else if (done) {
						/** inside the thread */
						postObjectDeleteCommit(meta, headVersion);
					}
	
					/**  DATA CONSISTENCY
					 *   ----------------
						 If The system crashes before Commit or Cancel -> next time the system starts up it will REDO all stored operations.
						 Also, if there are error buckets in the drives, they will be normalized when the system starts. 
					 */
					
				} catch (Exception e) {
					logger.error(e, "op:" + op.getOp().getName() +	", "  + getDriver().objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
				}
				finally {
					getLockService().getBucketLock(bucketId).readLock().unlock();
				}
			}
		} finally {
			getLockService().getObjectLock(bucketId, objectName).writeLock().unlock();
		}
		
		if(done) {
			onAfterCommit(op, meta, headVersion);
		}
	
	}
	
	protected void wipeAllPreviousVersions() {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class));
	}

	protected void deleteBucketAllPreviousVersions(ServerBucket bucket) {
		getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName(), bucket.getId()));
	}

	/**
	 *
	 * 
	 */
	protected void deleteObjectAllPreviousVersions(ObjectMetadata headMeta) {
	
	    Check.requireNonNullArgument(headMeta, "ObjectMetadata is null");
		 
		VFSOperation op = null;  
		boolean done = false;
		boolean isMainException = false;
		
		int headVersion = -1;
		
		String bucketName = headMeta.bucketName;
		String objectName = headMeta.objectName;
		Long bucketId = headMeta.bucketId;
		
		
		final ServerBucket bucket = getVFS().getBucketById(headMeta.bucketId);
		
		
		getLockService().getObjectLock(bucketId, objectName).writeLock().lock();
		
		try {
				getLockService().getBucketLock(bucketId).readLock().lock();
			
				try {
				
					if (!getDriver().getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucketId, objectName))
						throw new OdilonObjectNotFoundException("object does not exist -> " + getDriver().objectInfo(bucketId, objectName));
					
					headVersion = headMeta.version;
					
					/** It does not delete the head version, only previous versions */
					if (headVersion==0)
						return;
					
					op = getJournalService().deleteObjectPreviousVersions(bucketId, objectName, headVersion);
					
					backupMetadata(headMeta);
		
					/** remove all "objectmetadata.json.vn" Files, but keep -> "objectmetadata.json" **/  
					for (int version=0; version < headVersion; version++) { 
						for (Drive drive: getDriver().getDrivesAll()) {
							FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucketId, objectName, version));
						}
					}

					/** update head metadata with the tag */
					headMeta.addSystemTag("delete versions");
					headMeta.lastModified = OffsetDateTime.now();
					
					 final List<Drive> drives = getDriver().getDrivesAll();
					 final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
					
					 getDriver().getDrivesAll().forEach(d -> list.add(d.getObjectMetadata(bucketId, objectName)));
					 getDriver().saveObjectMetadataToDisk(drives, list, true);
					 
					 done=op.commit();
					 
				
				} catch (OdilonObjectNotFoundException e1) {
					done=false;
					isMainException = true;
					throw (e1);
				
				} catch (Exception e) {
					done=false;
					isMainException = true;
					throw new InternalCriticalException(e, getDriver().objectInfo(bucketId, objectName));
				}
				
				finally {
		
					try {
					
						if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
							} catch (Exception e) {
								if (!isMainException)
									throw new InternalCriticalException(e, getDriver().objectInfo(bucketName, objectName));
								else
									logger.error(e, getDriver().objectInfo(bucketName, objectName), SharedConstant.NOT_THROWN);
							}
						}
						else if (done) {
							postObjectPreviousVersionDeleteAllCommit(headMeta, headVersion);
						}
					}
					finally {
						getLockService().getBucketLock(bucketId).readLock().unlock();
					}
				}
		} finally {
			getLockService().getObjectLock(bucketId, objectName).writeLock().unlock();
		}

		if (done) {
			onAfterCommit(op, headMeta, headVersion);
		}
		
	}

	/** not used by de moment */
	protected void postObjectDelete(ObjectMetadata meta, int headVersion) 						{}
	protected void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion) 	{}
	
	
	private void postObjectPreviousVersionDeleteAllCommit(@NonNull ObjectMetadata meta, int headVersion) {
					
		Check.requireNonNullArgument(meta, "meta is null");
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		Long bucketId = meta.bucketId;
		
		Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		try {
			/** delete data versions(0..headVersion-1). keep headVersion **/
			for (int n=0; n<headVersion; n++) {
					getDriver().getObjectDataFiles(meta, Optional.of(n)).forEach(item->
					{
						FileUtils.deleteQuietly(item);
					});
			}

			/** delete backup Metadata */
			for (Drive drive:getDriver().getDrivesAll())
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketId), objectName));
			
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}
	
	/**
	 * 
	 */
	@Override
	protected void rollbackJournal(@NonNull VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");
		
		/** checked by the calling driver */
		Check.requireTrue(op.getOp()==VFSOp.DELETE_OBJECT ||op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS, "invalid -> op: " + op.getOp().getName());
			
		String objectName = op.getObjectName();
		String bucketName = op.getBucketName();
		
		
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketName);
		
		boolean done = false;
	 
		ServerBucket bucket = getDriver().getVFS().getBucketById(op.getBucketId());
		
		try {

			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			/** rollback is the same for both operations */
			if (op.getOp()==VFSOp.DELETE_OBJECT)
				restoreMetadata(bucket,objectName);
			
			else if (op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
				restoreMetadata(bucket,objectName);
			
			done=true;
			
		} catch (InternalCriticalException e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			if (!recoveryMode)
				throw(e);
			else
				logger.error(msg, SharedConstant.NOT_THROWN);
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			else
				logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"), SharedConstant.NOT_THROWN);
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
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(meta.bucketId), objectName));
			}
		catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
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
			for (Drive drive: getDriver().getDrivesAll()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketId, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(meta.bucketId) + File.separator + meta.objectName;
				File src = new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
		} catch (IOException e) {
			throw new InternalCriticalException(e, getDriver().objectInfo(meta.bucketId,meta.objectName));
		}
	}

	/**
	 * @param op
	 * @param headVersion
	 */
	private void onAfterCommit(VFSOperation op, ObjectMetadata meta, int headVersion) {
		
		try {
			if (op.getOp()==VFSOp.DELETE_OBJECT || op.getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS) {
				getVFS().getSchedulerService().enqueue(getVFS().getApplicationContext().getBean(AfterDeleteObjectServiceRequest.class, op.getOp(), meta, headVersion));
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}
	
	/**
	 *  restore metadata directory 
	 * */
	private void restoreMetadata(ServerBucket bucket, String objectName) {
		for (Drive drive: getDriver().getDrivesAll()) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket.getId()) + File.separator + objectName;
			String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket.getId(), objectName);
			try {
				if ((new File(objectMetadataBackupDirPath)).exists())
					FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
			} catch (IOException e) {
				throw new InternalCriticalException(e, getDriver().objectInfo(bucket,objectName));
			}
		}
	}


}
