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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.OdilonVersion;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.util.ODFileUtils;
import io.odilon.vfs.RAIDUpdateObjectHandler;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * 
 * 
 */
@ThreadSafe
public class RAIDZeroUpdateObjectHandler extends RAIDZeroHandler  implements  RAIDUpdateObjectHandler {
			
	private static Logger logger = Logger.getLogger(RAIDZeroUpdateObjectHandler.class.getName());
	
	/**
	 * <p>Used internally by {@code RAIDZeroDriver}</p>
	 */
	protected RAIDZeroUpdateObjectHandler(RAIDZeroDriver driver) {
		super(driver);
	}
	
	/**
	 * <p>
	 * throws IllegalArgumentException
	 *  
	 * . The Object does not have a previous version (ie. version=0)
	 * . The Object previous versions were deleted by a {@code deleteObjectAllPreviousVersions}
	 * 
	 * </p>
	 */
	public ObjectMetadata restorePreviousVersion(VFSBucket bucket, String objectName) {

		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		
		try {

			getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
		
			ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, true);
			
			if (meta.version==0)
				throw new IllegalArgumentException(	"Object does not have any previous version | " + "b:" + 
													(Optional.ofNullable(bucket).isPresent() ? (bucket.getName())  :"null") +
						 							", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null"));
			
			beforeHeadVersion = meta.version;
			List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
			
			for (int version=0; version<beforeHeadVersion; version++) {
				ObjectMetadata mv = getDriver().getReadDrive(bucket.getName(), objectName).getObjectMetadataVersion(bucket.getName(), objectName, version);
				if (mv!=null)
					metaVersions.add(mv);
			}

			if (metaVersions.isEmpty()) 
				throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
			
			op = getJournalService().restoreObjectPreviousVersion(bucket.getName(), objectName, beforeHeadVersion);
			
			/** save current head version MetadataFile .vN  and data File vN - no need to additional backup */
			saveVersionObjectDataFile(bucket, objectName,  meta.version);
			saveVersionObjectMetadata(bucket, objectName,  meta.version);

			/** save previous version as head */
			ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size()-1);

			if (!restoreVersionObjectDataFile(metaToRestore.bucketName, metaToRestore.objectName, metaToRestore.version))
				throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
			
			if (!restoreVersionObjectMetadata(metaToRestore.bucketName, metaToRestore.objectName, metaToRestore.version))
				throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
			
			getVFS().getObjectCacheService().remove(bucket.getName(), meta.objectName);
			done = op.commit();
			
			return null;
			
		} catch (OdilonObjectNotFoundException e1) {
			done=false;
			logger.error(e1);
			 e1.setErrorMessage( e1.getErrorMessage() + " | " + 
				"b:" 		+ (Optional.ofNullable(bucket).isPresent() ? (bucket.getName())  			:"null") +
				", o:"		+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       		:"null"));

			
			throw e1;
			
		} catch (Exception e) {
			done=false;
			String msg = "b:"  	+ (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName())  :"null") + 
						 ", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null");  
			logger.error(msg);
			throw new InternalCriticalException(e, msg);
			
		} finally {
			
			try {
				boolean requiresRollback = (!done) && (op!=null);
				
				if (requiresRollback) {
					
					try {

						rollbackJournal(op, false);
						
					} catch (Exception e) {
						String msg =  	"b:"   + (Optional.ofNullable(bucket).isPresent()    	? (bucket.getName()) 	:"null") + 
										", o:" + (Optional.ofNullable(objectName).isPresent() 	? (objectName)       	:"null");   
						
						logger.error(e, msg);
						throw new InternalCriticalException(e);
					}
				}
				else {
					/** -------------------------
					 TODO AT ->
					 Sync by the moment
					 see how to make it Async
					------------------------ */
					if(op!=null)
						cleanUpRestoreVersion(bucket, objectName, beforeHeadVersion);
				}
			} finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
			}
		}
	}
	
	/**
	 * 
	 * 
	 */
	@Override
	public void update(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType) {

		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		
		try {

			getLockService().getObjectLock( bucket.getName(), objectName).writeLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
		
			boolean exists = getDriver().getWriteDrive(bucket.getName(), objectName).existsObjectMetadata(bucket.getName(), objectName);
			
			if (!exists)
				throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
			
			ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, true);
									
			beforeHeadVersion = meta.version;							
			
			op = getJournalService().updateObject(bucket.getName(), objectName, meta.version);
			
			/** backup current head version */
			saveVersionObjectDataFile(bucket, objectName,  meta.version);
			saveVersionObjectMetadata(bucket, objectName,  meta.version);
			
			/** copy new version  head version */
			afterHeadVersion = meta.version+1;
			saveObjectDataFile(bucket, objectName, stream, srcFileName, meta.version+1);
			saveObjectMetadata(bucket, objectName, srcFileName, contentType, meta.version+1);
			
			getVFS().getObjectCacheService().remove(bucket.getName(), meta.objectName);
			done = op.commit();
		
		} catch (OdilonObjectNotFoundException e1) {
			done=false;
			logger.error(e1);
			throw e1;
			
		} catch (Exception e) {
			done=false;
			String msg = "b:"  	+ (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName())  :"null") + 
						 ", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
						 ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null");
				
			logger.error(msg);
			throw new InternalCriticalException(e, msg);
			
		} finally {
			
			try {
				try {
					if (stream!=null) 
						stream.close();
				} catch (IOException e) {
					logger.error(e, ServerConstant.NOT_THROWN);
				}
				
				boolean requiresRollback = (!done) && (op!=null);
				
				if (requiresRollback) {
					try {
						rollbackJournal(op, false);
						
					} catch (Exception e) {
						String msg =  	"b:"   + (Optional.ofNullable(bucket).isPresent()    	? (bucket.getName()) 	:"null") + 
										", o:" + (Optional.ofNullable(objectName).isPresent() 	? (objectName)       	:"null") +  
										", f:" + (Optional.ofNullable(srcFileName).isPresent() 	? (srcFileName)     	:"null");
						logger.error(e, msg);
						throw new InternalCriticalException(e);
					}
				}
				else {
					/** -------------------------
					 TODO AT ->
					 Sync by the moment
					 see how to make it Async
					------------------------ */
					if(op!=null)
						cleanUpUpdate(bucket, objectName, beforeHeadVersion, afterHeadVersion);
				}
			} finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
			}
		}
	}

	/**
	 * <p>This update does not generate a new Version of the ObjectMetadata. It maintains the same ObjectMetadata version.<br/>
	 * The only way to version Object is when the Object Data is updated</p>
	 * 
	 * @param meta
	 */
	@Override
	public void updateObjectMetadata(ObjectMetadata meta) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		VFSOperation op = null;
		boolean done = false;
		
		try {
			
			getLockService().getObjectLock( meta.bucketName, meta.objectName).writeLock().lock();
			getLockService().getBucketLock( meta.bucketName).readLock().lock();

			op = getJournalService().updateObjectMetadata(meta.bucketName, meta.objectName, meta.version);
			
			backupMetadata(meta);
			saveObjectMetadata(meta);
			
			getVFS().getObjectCacheService().remove(meta.bucketName, meta.objectName);
			done = op.commit();
			
		} catch (Exception e) {
			done=false;
			throw new InternalCriticalException(e,  "b:"   + (Optional.ofNullable(meta.bucketName).isPresent() ? (meta.bucketName) :"null") + 
													", o:" + (Optional.ofNullable(meta.objectName).isPresent() ? meta.objectName   :"null")); 
			
		} finally {
			
			try {
				boolean requiresRollback = (!done) && (op!=null);
				
				if (requiresRollback) {
					
						try {

							rollbackJournal(op, false);
							
						} catch (Exception e) {
							logger.error(e);
							throw new InternalCriticalException(e,  "b:"   + (Optional.ofNullable(meta.bucketName).isPresent()    ? (meta.bucketName) :"null")); 
							
						}
				}
				else {
					/** -------------------------
					 TODO AT ->
					 Sync by the moment
					 see how to make it Async
					------------------------ */
					cleanUpBackupMetadataDir(meta.bucketName, meta.objectName);
				}
			} finally {
				getLockService().getBucketLock(meta.bucketName).readLock().unlock();
				getLockService().getObjectLock(meta.bucketName, meta.objectName).writeLock().unlock();
			}
		}
	}

	/**
	 *
	 * 
	 */
	@Override
	public void onAfterCommit(VFSBucket bucket, String objectName, int previousVersion, int currentVersion) {
	}


	
	/**
	 * <p>The procedure is the same for Version Control </p>
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue( (op.getOp()==VFSop.UPDATE_OBJECT || 
							op.getOp()==VFSop.UPDATE_OBJECT_METADATA ||
							op.getOp()==VFSop.RESTORE_OBJECT_PREVIOUS_VERSION
							), "VFSOperation can not be  ->  op: " + op.getOp().getName());

		if (op.getOp()==VFSop.UPDATE_OBJECT)
			rollbackJournalUpdate(op, recoveryMode);

		else if (op.getOp()==VFSop.UPDATE_OBJECT_METADATA)
			rollbackJournalUpdateMetadata(op, recoveryMode);
		
		else if (op.getOp()==VFSop.RESTORE_OBJECT_PREVIOUS_VERSION) 
			rollbackJournalUpdate(op, recoveryMode);
	}

	
	/**
	 * 
	 * @param op
	 * @param recoveryMode
	 */
	private void rollbackJournalUpdate(VFSOperation op, boolean recoveryMode) {
		
		boolean done = false;
				
		try {

			if (getVFS().getServerSettings().isStandByEnabled()) 
				getVFS().getReplicationService().cancel(op);
			
			restoreVersionObjectDataFile(op.getBucketName(), op.getObjectName(),  op.getVersion());
			restoreVersionObjectMetadata(op.getBucketName(), op.getObjectName(),  op.getVersion());
			
			done = true;
		
		} catch (InternalCriticalException e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			logger.error(msg);
			if (!recoveryMode)
				throw(e);
			
		} catch (Exception e) {
			String msg = "Rollback: | " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			logger.error(msg);
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
		}
		finally {
			if (done || recoveryMode) {
				op.cancel();
			}
		}
	}

	/**
	 * <p>The procedure is the whether there is Version Control or not</p> 
	 * 
	 * @param op
	 * @param recoveryMode
	 */
	private void rollbackJournalUpdateMetadata(VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");

		boolean done = false;
		
		try {

			if (getVFS().getServerSettings().isStandByEnabled()) 
				getVFS().getReplicationService().cancel(op);
			
			if (op.getOp()==VFSop.UPDATE_OBJECT_METADATA)
				restoreMetadata(op.getBucketName(), op.getObjectName());
			
			done = true;
		
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
			if (done || recoveryMode) {
				op.cancel();
			}
		}
	}
	
 
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param newVersion
	 */
	private void saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, int newVersion) {
		
		byte[] buf = new byte[ VirtualFileSystemService.BUFFER_SIZE ];

		BufferedOutputStream out = null;
		InputStream sourceStream = null;
		
		boolean isMainException = false;
		
		try {
				sourceStream = isEncrypt() ? getVFS().getEncryptionService().encryptStream(stream) : stream;
				out = new BufferedOutputStream(new FileOutputStream( ((SimpleDrive) getWriteDrive(bucket.getName(), objectName)).getObjectDataFilePath(bucket.getName(), objectName)), VirtualFileSystemService.BUFFER_SIZE);
				int bytesRead;
				while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0)
					out.write(buf, 0, bytesRead);
				
			} catch (Exception e) {
				isMainException = true;
				logger.error(e);
				throw new InternalCriticalException(e);		
	
			} finally {
				IOException secEx = null;
				try {
						
					if (out!=null)
						out.close();
						
					} catch (IOException e) {
						String msg ="b:"   + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) :"null") + 
								", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
								", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null"); 
						logger.error(e, msg + (isMainException ? ServerConstant.NOT_THROWN :""));
						secEx=e;
					}
				
				try {
					
					if (sourceStream!=null) 
						sourceStream.close();
					
				} catch (IOException e) {
					String msg ="b:" + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) :"null") + 
								", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
								", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null");
					logger.error(e, msg + (isMainException ? ServerConstant.NOT_THROWN :""));
					secEx=e;
				}
				if (!isMainException && (secEx!=null)) 
				 		throw new InternalCriticalException(secEx);
			}
	}

	
	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private void saveObjectMetadata(VFSBucket bucket, String objectName, String srcFileName, String contentType, int version) {
		
		OffsetDateTime now =  OffsetDateTime.now();
		Drive drive=getWriteDrive(bucket.getName(), objectName);
		File file=((SimpleDrive) drive).getObjectDataFile(bucket.getName(), objectName);
		
		try {
				String sha256 = ODFileUtils.calculateSHA256String(file);
				ObjectMetadata meta = new ObjectMetadata(bucket.getName(), objectName);
				meta.fileName=srcFileName;
				meta.appVersion=OdilonVersion.VERSION;
				meta.contentType=contentType;
				meta.encrypt=getVFS().isEncrypt();
				meta.vault=getVFS().isUseVaultNewFiles();
				meta.creationDate = now;
				meta.version=version;
				meta.versioncreationDate = meta.creationDate;
				meta.length=file.length();
				meta.etag=sha256; // sha256 is calculated on the encrypted file
				meta.integrityCheck = now;
				meta.sha256=sha256;
				meta.status=ObjectStatus.ENABLED;
				meta.drive=drive.getName();
				meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
				drive.saveObjectMetadata(meta);
			
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
	}

	
	/**
	 * 
	 * 
	 */
	private void saveVersionObjectMetadata(VFSBucket bucket, String objectName,	int version) {
		try {
			Drive drive=getWriteDrive(bucket.getName(), objectName);
			File file=drive.getObjectMetadataFile(bucket.getName(), objectName);
			drive.putObjectMetadataVersionFile(bucket.getName(), objectName, version, file);
			
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
		
	}

	/**
	 * 
	 */
	private void saveVersionObjectDataFile(VFSBucket bucket, String objectName, int version) {
		try {
			Drive drive=getWriteDrive(bucket.getName(), objectName);
			File file=((SimpleDrive) drive).getObjectDataFile(bucket.getName(), objectName);
			((SimpleDrive) drive).putObjectDataVersionFile(bucket.getName(), objectName, version, file);
			
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
	}
		
	
	/**
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param version
	 */
	private boolean restoreVersionObjectMetadata(String bucketName, String objectName, int versionToRestore) {
		try {
			Drive drive=getWriteDrive(bucketName, objectName);
			File file=drive.getObjectMetadataVersionFile(bucketName, objectName, versionToRestore);
			if (file.exists()) {
				drive.putObjectMetadataFile(bucketName, objectName, file);
				FileUtils.deleteQuietly(file);
				return true;
			}
			return false;
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
	}


	/**
	 *
	 * 
	 */
	private boolean restoreVersionObjectDataFile(String bucketName, String objectName, int version) {
		try {
			Drive drive=getWriteDrive(bucketName, objectName);
			File file= ((SimpleDrive) drive).getObjectDataVersionFile(bucketName, objectName,version);
			if (file.exists()) {
				((SimpleDrive) drive).putObjectDataFile(bucketName, objectName, file);
				FileUtils.deleteQuietly(file);
				return true;
			}
			return false;
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
	}
	

	/**
	 * 
	 * 
	 */
	private void backupMetadata(ObjectMetadata meta) {
	
		/** copy metadata directory */
		try {
			String objectMetadataDirPath = getDriver().getWriteDrive(meta.bucketName, meta.objectName).getObjectMetadataDirPath(meta.bucketName, meta.objectName);
			String objectMetadataBackupDirPath = getDriver().getWriteDrive(meta.bucketName, meta.objectName).getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName;
			File src=new File(objectMetadataDirPath);
			if (src.exists())
				FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			
		} catch (IOException e) {
			throw new InternalCriticalException(e);
		}
	}

	
	/**
	 *
	 * 
	 * 
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
							", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)    :"null");  
			throw new InternalCriticalException(e, msg);
		}
	}
	
	/**
	 * <p>This clean up is sync by the moment<br/>
	 * <b>TODO AT</b> -> <i>This method should be Async. We have to analyze how</i><br/>
	 * 
	 * <h3>Version Control</h3>
	 * <ul> 
	 * <li>do not remove previous version Metadata</li>
	 * <li>do not remove previous version Data</li>
	 * </ul>
	 * 
	 * <h3>No Version Control</h3>
	 * <ul> 
	 * <li>remove previous version Metadata</li>
	 * <li>remove previous version Data</li>
	 * </ul>
	 * </p>
	 * 
	 * @param bucket
	 * @param objectName
	 * @param previousVersion
	 * @param currentVersion
	 */
	private void cleanUpUpdate(VFSBucket bucket, String objectName, int previousVersion, int currentVersion) {
		try {
			if (!getVFS().getServerSettings().isVersionControl()) {
				File metadata = getDriver().getWriteDrive(bucket.getName(), objectName).getObjectMetadataVersionFile(bucket.getName(), objectName, previousVersion);
				if (metadata.exists())
					FileUtils.deleteQuietly(metadata);
				
				File data=  ((SimpleDrive) getDriver().getWriteDrive(bucket.getName(), objectName)).getObjectDataVersionFile(bucket.getName(), objectName, previousVersion);
				if (data.exists())
					FileUtils.deleteQuietly(data);
				
			}
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}


	/**
	 *
	 * 
	 */
	private void cleanUpRestoreVersion(VFSBucket bucket, String objectName, int versionDiscarded) {
		
		try {
				if (versionDiscarded<0)
					return;
				
				File metadata = getDriver().getWriteDrive(bucket.getName(), objectName).getObjectMetadataVersionFile(bucket.getName(), objectName,  versionDiscarded);
				if (metadata.exists())
					FileUtils.deleteQuietly(metadata);
				
				File data=((SimpleDrive) getDriver().getWriteDrive(bucket.getName(), objectName)).getObjectDataVersionFile(bucket.getName(), objectName,  versionDiscarded);
				if (data.exists())
					FileUtils.deleteQuietly(data);
				
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
	

	/**
	 *
	 * 
	 */
	private void saveObjectMetadata(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		Drive drive=getWriteDrive(meta.bucketName, meta.objectName);
		drive.saveObjectMetadata(meta);
	}

	/**
	 *
	 * 
	 */
	private void cleanUpBackupMetadataDir(String bucketName, String objectName) {
		try {
			/** delete backup Metadata */
			String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName;
			File omb = new File(objectMetadataBackupDirPath);
			if (omb.exists())
				FileUtils.deleteQuietly(omb);
		} catch (Exception e) {
			logger.error(e);
		}
	}
	
}
