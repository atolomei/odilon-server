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
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

@ThreadSafe
public class RAIDOneUpdateObjectHandler extends RAIDOneHandler implements  RAIDUpdateObjectHandler {
			
	private static Logger logger = Logger.getLogger(RAIDOneUpdateObjectHandler.class.getName());
	
	/**
	 *  Instances of this class are used
	 * internally by {@link RAIDOneDriver}
	 * 
	 * @param driver
	 */

	protected RAIDOneUpdateObjectHandler(RAIDOneDriver driver) {
		super(driver);
	}

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
			
			getVFS().getObjectCacheService().remove(bucket.getName(), objectName);
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
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
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

			boolean exists = getDriver().getReadDrive(bucket.getName(), objectName).existsObject(bucket.getName(), objectName);
			
			if (!exists)
				throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
			
			ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, true);
			beforeHeadVersion = meta.version;							
			
			op = getJournalService().updateObject(bucket.getName(), objectName, beforeHeadVersion);
			
			/** backup current head version */
			saveVersionObjectDataFile(bucket, objectName,  meta.version);
			saveVersionObjectMetadata(bucket, objectName,  meta.version);
			
			/** copy new version as head version */
			afterHeadVersion = meta.version+1;
			saveObjectDataFile(bucket, objectName, stream, srcFileName, meta.version+1);
			saveObjectMetadata(bucket, objectName, srcFileName, contentType, meta.version+1);
			
			getVFS().getObjectCacheService().remove(bucket.getName(), objectName);
			done = op.commit();
		

		} catch (OdilonObjectNotFoundException e1) {
			done=false;
			logger.error(e1);
			throw e1;
			
		} catch (Exception e) {
			done=false;
			String msg = "b:"  	+ (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName())  :"null") + 
						 ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
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
					/**
					 TODO AT -> Sync by the moment. see how to make it Async
					 */
					if (op!=null)
						cleanUpUpdate(bucket, objectName, beforeHeadVersion, afterHeadVersion);
				}
			} finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
			}
		}
	}
	
	
	/**
	 * 
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
			
			getVFS().getObjectCacheService().remove(meta.bucketName,meta.objectName);
			done = op.commit();
			
		} catch (Exception e) {
			done=false;
			throw new InternalCriticalException(e,  "b:"   + (Optional.ofNullable(meta.bucketName).isPresent()    ? (meta.bucketName) :"null") + 
													", o:" + (Optional.ofNullable(meta.objectName).isPresent() ? meta.objectName  	  :"null")); 
			
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
	 * 7
	 */
	@Override
	public void onAfterCommit(VFSBucket bucket, String objectName, int previousVersion, int currentVersion) {
	}
	
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
	 * 
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

									
	private void rollbackJournalUpdateMetadata(VFSOperation op, boolean recoveryMode) {
		
		boolean done = false;
		
		try {

			if (getVFS().getServerSettings().isStandByEnabled()) 
				getVFS().getReplicationService().cancel(op);
			
			restoreVersionObjectMetadata(op.getBucketName(), op.getObjectName(),  op.getVersion());
			
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
	
	private void saveVersionObjectMetadata(VFSBucket bucket, String objectName,	int version) {
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				File file=drive.getObjectMetadataFile(bucket.getName(), objectName);
				drive.putObjectMetadataVersionFile(bucket.getName(), objectName, version, file);
			}
			
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
		
	}

	private void saveVersionObjectDataFile(VFSBucket bucket, String objectName, int version) {
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				File file=drive.getObjectDataFile(bucket.getName(), objectName);
				drive.putObjectDataVersionFile(bucket.getName(), objectName, version, file);
			}
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
	}
	
	
private void saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, int newVersion) {
		
		int total_drives = getDriver().getDrivesAll().size();
		byte[] buf = new byte[ VirtualFileSystemService.BUFFER_SIZE ];

		BufferedOutputStream out[] = new BufferedOutputStream[total_drives];
		InputStream sourceStream = null;
		
		boolean isMainException = false;
		
		try {

			sourceStream = isEncrypt() ? getVFS().getEncryptionService().encryptStream(stream) : stream;
			
			int n_d=0;
			for (Drive drive: getDriver().getDrivesAll()) { 
				String sPath = drive.getObjectDataFilePath(bucket.getName(), objectName);
				out[n_d++] = new BufferedOutputStream(new FileOutputStream(sPath), VirtualFileSystemService.BUFFER_SIZE);
			}
			int bytesRead;
			
			
			while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0)
				for (int bytes=0; bytes<total_drives; bytes++) {
					 out[bytes].write(buf, 0, bytesRead);
				 }
				
			} catch (Exception e) {
				isMainException = true;
				logger.error(e);
				throw new InternalCriticalException(e,  "b:"   + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName())  :"null") + 
														", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
														", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null"));		
	
			} finally {
				IOException secEx = null;
						
					if (out!=null) { 
						try {
								for (int n=0; n<total_drives; n++) {
									if (out[n]!=null)
										out[n].close();
								}
							} catch (IOException e) {
								String msg ="b:"   	+ (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) :"null") + 
										", o:" 		+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
										", f:" 		+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null"); 
								logger.error(e, msg + (isMainException ? ServerConstant.NOT_THROWN :""));
								secEx=e;
							}	
					}
						
					try {
						if (sourceStream!=null) 
							sourceStream.close();
					} catch (IOException e) {
						String msg ="b:" 		+ (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) :"null") + 
									", o:" 		+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
									", f:" 		+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null");
						logger.error(e, msg + (isMainException ? ServerConstant.NOT_THROWN :""));
						secEx=e;
					}
				if (!isMainException && (secEx!=null)) 
				 		throw new InternalCriticalException(secEx);
			}	
	}


	private void saveObjectMetadata(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		for (Drive drive: getDriver().getDrivesAll()) {
			drive.saveObjectMetadata(meta);
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
		String sha=null;
		String basedrive=null;
		
		try {

			for (Drive drive: getDriver().getDrivesAll()) {
				
					File file = drive.getObjectDataFile(bucket.getName(),  objectName);
				
					try {
					
						String sha256 = ODFileUtils.calculateSHA256String(file);
						
						if (sha==null) {
							sha=sha256;
							basedrive=drive.getName();
						}
						else {
							if (!sha256.equals(sha))
								throw new InternalCriticalException("SHA 256 are not equal for drives -> " + basedrive+":" + sha + " vs " + drive.getName()+ ":" + sha256);
						}
						
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
						meta.etag=sha256; /** sha256 is calculated on the encrypted file */
						meta.integrityCheck = now;
						meta.sha256=sha256;
						meta.status=ObjectStatus.ENABLED;
						meta.drive=drive.getName();
						meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
						drive.saveObjectMetadata(meta);
					} catch (Exception e) {
						String msg =  	"b:"   + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) :"null") + 
										", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
										", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null");
						
						logger.error(e,msg);
						throw new InternalCriticalException(e, msg);
					}
			}
			
		} catch (Exception e) {
				String msg =  	"b:"   + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) :"null") + 
								", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
								", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null");
				logger.error(e);
				throw new InternalCriticalException(e,msg);
		}
	}
	
	private boolean restoreVersionObjectMetadata(String bucketName, String objectName, int version) {
		try {

			boolean success = true;
			for (Drive drive: getDriver().getDrivesAll()) {
				File file=drive.getObjectMetadataVersionFile(bucketName, objectName, version);
				if (file.exists()) {
					drive.putObjectMetadataFile(bucketName, objectName, file);
					FileUtils.deleteQuietly(file);
				}
				else
					success=false;
			}
			return success;
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
	}


	private boolean restoreVersionObjectDataFile(String bucketName, String objectName, int version) {
		try {
			boolean success = true;
			for (Drive drive: getDriver().getDrivesAll()) {
				File file= drive.getObjectDataVersionFile(bucketName, objectName,version);
				if (file.exists()) {
					drive.putObjectDataFile(bucketName, objectName, file);
					FileUtils.deleteQuietly(file);
				}
				else
					success=false;
			}
			return success;
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
	}
	
	
	private void cleanUpRestoreVersion(VFSBucket bucket, String objectName, int versionDiscarded) {
		
		try {
				if (versionDiscarded<0)
					return;
	
				for (Drive drive: getDriver().getDrivesAll()) {
					
					File metadata = drive.getObjectMetadataVersionFile(bucket.getName(), objectName,  versionDiscarded);
					if (metadata.exists())
						FileUtils.deleteQuietly(metadata);

					File data=drive.getObjectDataVersionFile(bucket.getName(), objectName,  versionDiscarded);
					if (data.exists())
						FileUtils.deleteQuietly(data);
				}
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}

	
	
	/**
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ObjectMetadata meta) {
		/* copy metadata directory */
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketName, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName;
				File src=new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
			
		} catch (IOException e) {
			throw new InternalCriticalException(e);
		}
	}

	
	private void cleanUpUpdate(VFSBucket bucket, String objectName, int previousVersion, int currentVersion) {
		try {
			if (!getVFS().getServerSettings().isVersionControl()) {
				for (Drive drive: getDriver().getDrivesAll()) {
				File metadata = drive.getObjectMetadataVersionFile(bucket.getName(), objectName, previousVersion);
				if (metadata.exists())
					FileUtils.deleteQuietly(metadata);
				
				File data=drive.getObjectDataVersionFile(bucket.getName(), objectName, previousVersion);
				if (data.exists())
					FileUtils.deleteQuietly(data);
				}
				
			}
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
	
	
	private void cleanUpBackupMetadataDir(String bucketName, String objectName) {
		try {
			/** delete backup Metadata */
			for (Drive drive: getDriver().getDrivesAll()) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucketName) + File.separator + objectName;
			File omb = new File(objectMetadataBackupDirPath);
			if (omb.exists())
				FileUtils.deleteQuietly(omb);
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}
	
}
