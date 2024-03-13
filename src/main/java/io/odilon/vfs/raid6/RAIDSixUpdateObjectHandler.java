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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixUpdateObjectHandler extends RAIDSixHandler {
			
	
private static Logger logger = Logger.getLogger(RAIDSixCreateObjectHandler.class.getName());

	/**
	* <p>Instances of this class are used
	* internally by {@link RAIDSixDriver}<p>
	* 
	* @param driver
	*/
	protected RAIDSixUpdateObjectHandler(RAIDSixDriver driver) {
	super(driver);
	}
	
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
	 */
	public void update(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType) {
	
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		String bucketName = bucket.getName();
		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucketName).readLock().lock();

			if (!getDriver().getObjectMetadataReadDrive(bucketName, objectName).existsObjectMetadata(bucketName, objectName))
				throw new OdilonObjectNotFoundException("b:" + bucketName + " o:"+ objectName);
			
			meta = getDriver().getObjectMetadataInternal(bucketName, objectName, true);
			beforeHeadVersion = meta.version;							
														
			op = getJournalService().updateObject(bucketName, objectName, beforeHeadVersion);
			
			getVFS().getObjectCacheService().remove(bucketName, objectName);
			
			/** backup current head version */
			backupVersionObjectDataFile(meta, meta.version);
			backupVersionObjectMetadata(bucket, objectName,  meta.version);
			
			/** copy new version as head version */
			afterHeadVersion = meta.version+1;
			RSFileBlocks ei = saveObjectDataFile(bucket,objectName, stream);
			saveObjectMetadata(bucket, objectName, ei, srcFileName, contentType, afterHeadVersion);
			
			done = op.commit();
		

		} catch (OdilonObjectNotFoundException e1) {
			done=false;
			logger.error(e1);
			throw e1;
			
		} catch (Exception e) {
			done=false;
			throw new InternalCriticalException(e, "b:"+ bucket.getName() + " o:"  + objectName +	 ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName) :"null"));
			
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
						throw new InternalCriticalException(e, "b:"+ bucketName + " o:"  + objectName +	 ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName) :"null"));
					}
				}
				else {
					/**
					 TODO AT -> Sync by the moment. see how to make it Async
					 */
					if ((op!=null) && (meta!=null))
						cleanUpUpdate(meta, beforeHeadVersion, afterHeadVersion);
				}
			} finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
			}
		}
	}

	
	public void updateObjectMetadataHeadVersion(ObjectMetadata meta) {
		updateObjectMetadata(meta, true); 
	}
	
	/**
	 * @param meta
	 */
	 public void updateObjectMetadata(ObjectMetadata meta, boolean isHead) {

		Check.requireNonNullArgument(meta, "meta is null");
		Check.requireNonNullArgument(meta.bucketName, "bucketName is null");
		Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketName);
		
		VFSOperation op = null;
		
		boolean done = false;
		
		getLockService().getObjectLock( meta.bucketName, meta.objectName).writeLock().lock();		
			
		try {
				getLockService().getBucketLock( meta.bucketName).readLock().lock();
				try {
						op = getJournalService().updateObjectMetadata(meta.bucketName, meta.objectName, meta.version);
						getVFS().getObjectCacheService().remove(meta.bucketName,meta.objectName);
			
						backupMetadata(meta);
						saveObjectMetadata(meta, isHead);
						
						done = op.commit();
						
					} catch (Exception e) {
						done=false;
						throw new InternalCriticalException(e, "b:" + meta.bucketName + ", o:" + meta.objectName); 
						
					} finally {
						try {
							if ((!done) && (op!=null)) {
									try {
										rollbackJournal(op, false);
									} catch (Exception e) {
										throw new InternalCriticalException(e, "b:" + (Optional.ofNullable(meta.bucketName).isPresent() ? (meta.bucketName) :"null")); 
									}
							}
							else {
								/** TODO AT -> Sync by the moment. TODO see how to make it Async */
								cleanUpBackupMetadataDir(meta.bucketName, meta.objectName);
							}
						} finally {
							getLockService().getBucketLock(meta.bucketName).readLock().unlock();
						}
					}
		} finally {
				getLockService().getObjectLock(meta.bucketName, meta.objectName).writeLock().unlock();
		}
	}

	/**
	 * 
	 * 
	 * @param bucket
	 * @param objectName
	 * @return
	 */
	public ObjectMetadata restorePreviousVersion(VFSBucket bucket, String objectName) {
		
		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();

		try {
			
			getLockService().getBucketLock(bucket.getName()).readLock().lock();
			
			getVFS().getObjectCacheService().remove(bucket.getName(), objectName);
		
			meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, true);

			if (meta.version==0)
				throw new IllegalArgumentException(	"Object does not have any previous version | " + "b:" + 
													(Optional.ofNullable(bucket).isPresent() ? (bucket.getName())  :"null") +
						 							", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null"));
			
			
			beforeHeadVersion = meta.version;
			
			List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
			
			for (int version=0; version<beforeHeadVersion; version++) {
			
				ObjectMetadata mv = getDriver().getObjectMetadataReadDrive(bucket.getName(), objectName).getObjectMetadataVersion(bucket.getName(), objectName, version);
				
				if (mv!=null)
					metaVersions.add(mv);
			}

			if (metaVersions.isEmpty()) 
				throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
			
			op = getJournalService().restoreObjectPreviousVersion(bucket.getName(), objectName, beforeHeadVersion);
			
			/** save current head version MetadataFile .vN  and data File vN - no need to additional backup */
			backupVersionObjectDataFile( meta,  meta.version);
			backupVersionObjectMetadata(bucket, objectName,  meta.version);

			/** save previous version as head */
			ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size()-1);
			
			if (!restoreVersionObjectDataFile(metaToRestore, metaToRestore.version))
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
					if((op!=null) && (meta!=null))
						cleanUpRestoreVersion(meta, beforeHeadVersion);
				}
			} finally {
				getLockService().getBucketLock(bucket.getName()).readLock().unlock();
				getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
			}
		}
		
	}


	private void cleanUpRestoreVersion(ObjectMetadata meta, int versionDiscarded) {
		
		try {
				if (versionDiscarded<0)
					return;
	
				for (Drive drive: getDriver().getDrivesEnabled()) {
					File metadata = drive.getObjectMetadataVersionFile(meta.bucketName, meta.objectName,  versionDiscarded);
					FileUtils.deleteQuietly(metadata);
				}
				
				List<File> files = getDriver().getObjectDataFiles(meta, Optional.of(versionDiscarded));
				files.forEach( file -> {
					if (file.exists())
						FileUtils.deleteQuietly(file);
				}
				);
				
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}

	
	
	
	private void backupVersionObjectMetadata(VFSBucket bucket, String objectName,	int version) {
		try {
			for (Drive drive: getDriver().getDrivesEnabled()) {
				File file=drive.getObjectMetadataFile(bucket.getName(), objectName);
				if (file.exists())
					drive.putObjectMetadataVersionFile(bucket.getName(), objectName, version, file);
			}
			
		} catch (Exception e) {
				throw new InternalCriticalException(e, "backupVersionObjectMetadata");
		}
		
	}

	
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 * @param version
	 */
	
	/** backup current head version */
	
	private void backupVersionObjectDataFile(ObjectMetadata meta, int version) {
			Map<Drive, List<String>> map = getDriver().getObjectDataFilesNames(meta, Optional.empty());
			
			map.forEach((drive,fileNames) -> {
				fileNames.forEach(fileName -> {
					File current = new File(drive.getBucketObjectDataDirPath(meta.bucketName), fileName);
					String suffix = ".v"+ String.valueOf(version);
					File backupFile = new File(drive.getBucketObjectDataDirPath(meta.bucketName) + File.separator + VirtualFileSystemService.VERSION_DIR, fileName + suffix);
					try {
						Files.copy(current.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
					} catch (IOException e) {
						throw new InternalCriticalException(e, "src: " + current.getName() + " | back:" + backupFile.getName() );
					}
				});
			});
	}
	
	
	private void saveObjectMetadata(VFSBucket bucket, String objectName, RSFileBlocks ei, String srcFileName, String contentType, int version) {
		
		List<String> shaBlocks = new ArrayList<String>();
		StringBuilder etag_b = new StringBuilder();
		
		ei.encodedBlocks.forEach(item -> {
			try {
				shaBlocks.add(ODFileUtils.calculateSHA256String(item));
			} catch (Exception e) {
				throw new InternalCriticalException(e, "saveObjectMetadata" + "b:" + bucket.getName() + " o:" 	+ objectName + ", f:" + (Optional.ofNullable(item).isPresent() ? (item.getName()):"null"));
			}
		});
		shaBlocks.forEach(item->etag_b.append(item));
		String etag = null;
		try {
			etag = ODFileUtils.calculateSHA256String(etag_b.toString());
		} catch (NoSuchAlgorithmException | IOException e) {
				throw new InternalCriticalException(e, "saveObjectMetadata etag" + "b:" + bucket.getName() + " o:" 	+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null"));
		} 
		
		for (Drive drive: getDriver().getDrivesEnabled()) {
			
			try {
				ObjectMetadata meta = new ObjectMetadata(bucket.getName(), objectName);
				
				meta.fileName=srcFileName;
				meta.appVersion=OdilonVersion.VERSION;
				meta.contentType=contentType;
				meta.creationDate = OffsetDateTime.now();
				meta.version=version;
				meta.versioncreationDate = meta.creationDate;
				meta.length=ei.fileSize;
				meta.totalBlocks=ei.encodedBlocks.size();
				meta.sha256Blocks=shaBlocks;
				meta.etag=etag;
				meta.encrypt=getVFS().isEncrypt();
				meta.integrityCheck=meta.creationDate;
				meta.status=ObjectStatus.ENABLED;
				meta.drive=drive.getName();
				meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
				drive.saveObjectMetadata(meta);
	
			} catch (Exception e) {
				throw new InternalCriticalException(e, "saveObjectMetadata" + "b:" + bucket.getName() + " o:" 	+ objectName);
			}
		}
	}
	

	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private RSFileBlocks saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream) {
		
		InputStream sourceStream = null;
		boolean isMainException = false;
		try {
				sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream;
				RSEncoder encoder = new RSEncoder(getDriver());
				return encoder.encodeHead(sourceStream, bucket.getName(), objectName);
			
			} catch (Exception e) {
				isMainException = true;
				logger.error(e);
				throw new InternalCriticalException(e);		
	
			} finally {
				
				IOException secEx = null;
				
				try {
					if (sourceStream!=null) 
						sourceStream.close();
					
				} catch (IOException e) {
					String msg ="b:" + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getName()) : "null") + 
								", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)    : "null");   
					logger.error(e, msg + (isMainException ? ServerConstant.NOT_THROWN :""));
					secEx=e;
				}
				if (!isMainException && (secEx!=null)) 
					throw new InternalCriticalException(secEx);
			}
	}

	/**
	 * 
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue((	op.getOp()==VFSop.UPDATE_OBJECT 			|| 
							op.getOp()==VFSop.UPDATE_OBJECT_METADATA 	||
							op.getOp()==VFSop.RESTORE_OBJECT_PREVIOUS_VERSION), 
							"VFSOperation can not be  ->  op: " + op.getOp().getName());

		
		getVFS().getObjectCacheService().remove(op.getBucketName(), op.getObjectName());
		
		switch (op.getOp()) {
					case UPDATE_OBJECT: 
					{	rollbackJournalUpdate(op, recoveryMode);
						break;
					}
					case  UPDATE_OBJECT_METADATA: 
					{	rollbackJournalUpdateMetadata(op, recoveryMode);
						break;
					}
					case RESTORE_OBJECT_PREVIOUS_VERSION: 
					{
						rollbackJournalUpdate(op, recoveryMode);
						break;
					}
					default: {
						break;	
					}
		}
	}

	
	/**
	 * <p></p>
	 * @param op
	 * @param recoveryMode
	 */
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

	/**
	 * 
	 * <p>copy metadata directory <br/>. 
	 * back up the full metadata directory (ie. ObjectMetadata for all versions)</p>
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ObjectMetadata meta) {
		try {
			for (Drive drive: getDriver().getDrivesEnabled()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketName, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName;
				File src=new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:"+ meta.bucketName +" o:" + meta.objectName);
		}
	}
	
	/**
	 *  <p>delete backup Metadata</p> 
	 * 
	 * @param bucketName
	 * @param objectName
	 */
	private void cleanUpBackupMetadataDir(String bucketName, String objectName) {
		
		try {
				for (Drive drive: getDriver().getDrivesEnabled()) {
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucketName) + File.separator + objectName;
				FileUtils.deleteQuietly(new File(objectMetadataBackupDirPath));
			}
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
	
	/**
	 * @param meta
	 * @param previousVersion
	 * @param currentVersion
	 */
	private void cleanUpUpdate(ObjectMetadata meta, int previousVersion, int currentVersion) {
		Check.requireNonNullArgument(meta, "meta is null");
		try {
			if (!getVFS().getServerSettings().isVersionControl()) {
				for (Drive drive: getDriver().getDrivesEnabled()) {
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(meta.bucketName, meta.objectName, previousVersion));
					List<File> files = getDriver().getObjectDataFiles(meta, Optional.of(previousVersion));
					files.forEach( file -> {FileUtils.deleteQuietly(file);});
				}
			}
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
	
	
	private void saveObjectMetadata(ObjectMetadata meta, boolean isHead) {
		Check.requireNonNullArgument(meta, "meta is null");
		for (Drive drive: getDriver().getDrivesEnabled()) {
			if (isHead)
				drive.saveObjectMetadata(meta);
			else
				drive.saveObjectMetadataVersion(meta);
		}
	}
	
	/**
	 * 
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param version
	 * @return
	 */
	private boolean restoreVersionObjectMetadata(String bucketName, String objectName, int version) {
		
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);

		try {
			boolean success = true;
			for (Drive drive: getDriver().getDrivesEnabled()) {
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

	
	
	private boolean restoreVersionObjectDataFile(ObjectMetadata meta, int version) {

		Check.requireNonNullArgument(meta.bucketName, "bucketName is null");
		Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketName);

		try {
			boolean success = true;
	
			Map<Drive, List<String>> versionToRestore = getDriver().getObjectDataFilesNames(meta, Optional.of(version));
			
			versionToRestore.forEach( (drive, fileNames) -> {
					fileNames.forEach( file -> {
								String arr[] =file.split(".v");
								String headFileName = arr[0];
								try {
									Files.copy( (new File(drive.getBucketObjectDataDirPath(meta.bucketName)+File.separator+VirtualFileSystemService.VERSION_DIR,  file)).toPath(), 
												(new File(drive.getBucketObjectDataDirPath(meta.bucketName), headFileName)).toPath(), 
												StandardCopyOption.REPLACE_EXISTING);
								} catch (IOException e) {
									logger.error(e);
									throw new InternalCriticalException(e);
								}
					});
			});
			
			
			return success;
		} catch (Exception e) {
				logger.error(e);
				throw new InternalCriticalException(e);
		}
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
				
			ObjectMetadata meta = getDriver().getObjectMetadataReadDrive(op.getBucketName(), op.getObjectName()).getObjectMetadata(op.getBucketName(), op.getObjectName());
			
			if (meta!=null) {
				getVFS().getObjectCacheService().remove(meta.bucketName, meta.objectName);
				restoreVersionObjectDataFile(meta,  op.getVersion());
				restoreVersionObjectMetadata(op.getBucketName(), op.getObjectName(),  op.getVersion());
			}
			
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

	
	
}
