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

import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixUpdateObjectHandler extends RAIDSixHandler {
			
			
private static Logger logger = Logger.getLogger(RAIDSixUpdateObjectHandler.class.getName());

	/**
	* <p>Instances of this class are used
	* internally by {@link RAIDSixDriver}</p>
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
		boolean isMainException = false;
		
		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
		
				try (stream) {
					
					getLockService().getBucketLock(bucketName).readLock().lock();
		
					if (!getDriver().getObjectMetadataReadDrive(bucketName, objectName).existsObjectMetadata(bucketName, objectName))
						throw new IllegalArgumentException(" object not found -> b:" + bucketName + " o:"+ objectName);
					
					meta = getDriver().getObjectMetadataInternal(bucketName, objectName, false);
					beforeHeadVersion = meta.version;							
																
					op = getJournalService().updateObject(bucketName, objectName, beforeHeadVersion);
					
					
					/** backup current head version */
					backupVersionObjectDataFile(meta, meta.version);
					backupVersionObjectMetadata(bucket, objectName,  meta.version);
					
					/** copy new version as head version */
					afterHeadVersion = meta.version+1;
					RAIDSixBlocks ei = saveObjectDataFile(bucket,objectName, stream);
					saveObjectMetadata(bucket, objectName, ei, srcFileName, contentType, afterHeadVersion, meta.creationDate);

					
					// cache
					//
					getVFS().getObjectCacheService().remove(bucketName, objectName);
					getVFS().getFileCacheService().remove(bucketName, objectName, Optional.empty());
					getVFS().getFileCacheService().remove(bucketName, objectName, Optional.of(beforeHeadVersion));
					
					done = op.commit();
				
				} catch (Exception e) {
					done=false;
					isMainException = true;
					throw new InternalCriticalException(e, "b:"+ bucket.getName() + " o:"  + objectName +	 ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName) :"null"));
					
				} finally {
					
					try {
						if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
							} catch (Exception e) {
								if (isMainException)
									throw new InternalCriticalException(e, "b:"+ bucketName + " o:"  + objectName +	 ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName) :"null"));
								else
									logger.error("b:"+ bucketName + " o:"  + objectName +	 ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName) :"null"), ServerConstant.NOT_THROWN);
							}
						}
						else {
							/** TODO AT -> Sync by the moment. see how to make it Async */
							if ((op!=null) && (meta!=null))
								cleanUpUpdate(meta, beforeHeadVersion, afterHeadVersion);
						}
					} finally {
						getLockService().getBucketLock(bucket.getName()).readLock().unlock();
					}
				}
		
		} finally {
			getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
		}
	}

	
	public void updateObjectMetadataHeadVersion(ObjectMetadata meta) {
		updateObjectMetadata(meta, true); 
	}
	
	
	/**
	 * 
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
			
					backupMetadata(meta);
					saveObjectMetadata(meta, isHead);
					
					// cache
					//
					getVFS().getObjectCacheService().remove(meta.bucketName,meta.objectName);
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
	 * @param bucket
	 * @param objectName
	 * @return
	 */
	public ObjectMetadata restorePreviousVersion(VFSBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		String bucketName = bucket.getName();
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);

		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		ObjectMetadata meta = null;
		boolean isMainException = false;
		
		
		getLockService().getObjectLock(bucket.getName(), objectName).writeLock().lock();

			try {
				
				getLockService().getBucketLock(bucketName).readLock().lock();
			
				try {
					
					meta = getDriver().getObjectMetadataInternal(bucketName, objectName, false);
		
					if (meta.getVersion()==0)
						throw new IllegalArgumentException(	"Object does not have any previous version | " + "b:" +	 bucketName + " o:" + objectName);
					
					
					beforeHeadVersion = meta.version;
					
					
					List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
					
					for (int version=0; version<beforeHeadVersion; version++) {
						ObjectMetadata mv = getDriver().getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadataVersion(bucketName, objectName, version);
						if (mv!=null)
							metaVersions.add(mv);
					}
		
					if (metaVersions.isEmpty()) 
						throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
					
					op = getJournalService().restoreObjectPreviousVersion(bucketName, objectName, beforeHeadVersion);
					
					/** save current head version MetadataFile .vN  and data File vN - no need to additional backup */
					backupVersionObjectDataFile(meta,  meta.version);
					backupVersionObjectMetadata(bucket, objectName,  meta.version);
		
					/** save previous version as head */
					ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size()-1);
					
					
					if (!restoreVersionObjectDataFile(metaToRestore, metaToRestore.version))
						throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
					
					if (!restoreVersionObjectMetadata(metaToRestore.bucketName, metaToRestore.objectName, metaToRestore.version))
						throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
					
					
					// cache
					//
					getVFS().getObjectCacheService().remove(bucketName, objectName);
					getVFS().getFileCacheService().remove(bucketName, objectName, Optional.empty());
					getVFS().getFileCacheService().remove(bucketName, objectName, Optional.of(beforeHeadVersion));
					
					done = op.commit();
					
					return null;
					
				} catch (Exception e) {
					done=false;
					isMainException = true;
					logger.error(e);
					throw new InternalCriticalException(e, "b:" + bucketName + ", o:" + objectName);
					
				} finally {
					
					try {
						
						if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
							} catch (InternalCriticalException e) {
								if (isMainException)
									throw new InternalCriticalException(e);
								else
									logger.error(e, "b:" + bucketName + " o:" + objectName, ServerConstant.NOT_THROWN);
								
							} catch (Exception e) {
								if (isMainException)
									throw new InternalCriticalException(e, "b:"   +  bucketName +  " o:" + objectName);
								else
									logger.error(e, "b:" + bucketName + " o:" + objectName, ServerConstant.NOT_THROWN);	
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
					}
				}
			} finally {
				getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
			}
	}


	/**
	 * 
	 * 
	 * @param meta
	 * @param versionDiscarded
	 */
	private void cleanUpRestoreVersion(ObjectMetadata meta, int versionDiscarded) {
		
		try {
				if (versionDiscarded < 0)
					return;
	
				for (Drive drive: getDriver().getDrivesAll())
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(meta.bucketName, meta.objectName,  versionDiscarded));
				
				List<File> files = getDriver().getObjectDataFiles(meta, Optional.of(versionDiscarded));
				files.forEach( file -> FileUtils.deleteQuietly(file));
				
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}

	
	
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 * @param version
	 */
	private void backupVersionObjectMetadata(VFSBucket bucket, String objectName,	int version) {
		
		String bucketName = bucket.getName();
		
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				File file=drive.getObjectMetadataFile(bucket.getName(), objectName);
				if (file.exists())
					drive.putObjectMetadataVersionFile(bucket.getName(), objectName, version, file);
			}
		
		} catch (InternalCriticalException e) {
			throw e;
		} catch (Exception e) {
				throw new InternalCriticalException(e, "backupVersionObjectMetadata | b:" + bucketName + " o:" + objectName);
		}
	}

	
	/**
	 * backup current head version 
	 * 
	 * @param bucket
	 * @param objectName
	 * @param version
	 */

	private void backupVersionObjectDataFile(ObjectMetadata meta, int headVersion) {
			
		Map<Drive, List<String>> map = getDriver().getObjectDataFilesNames(meta, Optional.empty());
		
		for (Drive drive: map.keySet()) {
			for (String filename: map.get(drive)) {
				File current = new File(drive.getBucketObjectDataDirPath(meta.bucketName), filename);
				String suffix = ".v"+ String.valueOf(headVersion);
				File backupFile = new File(drive.getBucketObjectDataDirPath(meta.bucketName) + File.separator + VirtualFileSystemService.VERSION_DIR, filename + suffix);
				try {
					
					if(current.exists())
						Files.copy(current.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
					
				} catch (IOException e) {
					throw new InternalCriticalException(e, "src: " + current.getName() + " | back:" + backupFile.getName() );
				}
			}
		}
	}
	
	
	/**
	 * 
	 * 
	 * @param bucket
	 * @param objectName
	 * @param ei
	 * @param srcFileName
	 * @param contentType
	 * @param version
	 * @param headCreationDate
	 */
	private void saveObjectMetadata(VFSBucket bucket, String objectName, RAIDSixBlocks ei, String srcFileName, String contentType, int version, OffsetDateTime headCreationDate) {
		
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
				logger.error(e);
				throw new InternalCriticalException(e, "saveObjectMetadata etag" + "b:" + bucket.getName() + " o:" 	+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null"));
		} 
		
		
		OffsetDateTime versionCreationDate = OffsetDateTime.now(); 
	
		for (Drive drive: getDriver().getDrivesAll()) {
			
			try {
				ObjectMetadata meta = new ObjectMetadata(bucket.getName(), objectName);
				
				meta.fileName=srcFileName;
				meta.appVersion=OdilonVersion.VERSION;
				meta.contentType=contentType;
				meta.creationDate = headCreationDate;
				meta.version=version;
				meta.versioncreationDate = versionCreationDate;
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
	private RAIDSixBlocks saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream) {
		
		InputStream sourceStream = null;
		boolean isMainException = false;
		try {
				sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream;
				RAIDSixEncoder encoder = new RAIDSixEncoder(getDriver());
				return encoder.encodeHead(sourceStream, bucket.getName(), objectName);
			
			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, "saveObjectDataFile");		
	
			} finally {
				
				IOException secEx = null;
				
				try {
					if (sourceStream!=null) 
						sourceStream.close();
					
				} catch (IOException e) {
					logger.error(e, ("b:" + bucket.getName() + ", o:" + objectName) + (isMainException ? ServerConstant.NOT_THROWN :""));
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
						throw new IllegalArgumentException(VFSOperation.class.getSimpleName() + " can not be  ->  op: " + op.getOp().getName());
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
			if (!recoveryMode)
				throw(e);
			else
				logger.error(msg, ServerConstant.NOT_THROWN);
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			else
				logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"), ServerConstant.NOT_THROWN);
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
			for (Drive drive: getDriver().getDrivesAll()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketName, meta.objectName);
				File src=new File(objectMetadataDirPath);
				if (src.exists()) 
					FileUtils.copyDirectory(src, new File(drive.getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName));
				
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
				for (Drive drive: getDriver().getDrivesAll()) {
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketName) + File.separator + objectName));
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
				
				for (Drive drive: getDriver().getDrivesAll()) {
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
		for (Drive drive: getDriver().getDrivesAll()) {
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
			
			ObjectMetadata versionMeta = getDriver().getObjectMetadataVersion(bucketName, objectName, version);
			
			for (Drive drive: getDriver().getDrivesAll()) {
				drive.saveObjectMetadata(versionMeta);
			}
			return success;
			
		} catch (InternalCriticalException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalCriticalException(e, "b:"+ bucketName +" o:" + objectName);
		}
	}

	
	
	private boolean restoreVersionObjectDataFile(ObjectMetadata meta, int versiontoRestore) {

		Check.requireNonNullArgument(meta.bucketName, "bucketName is null");
		Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketName);

		try {
	
			Map<Drive, List<String>> versionToRestore = getDriver().getObjectDataFilesNames(meta, Optional.of(versiontoRestore));

			for (Drive drive: versionToRestore.keySet()) {
				for (String name: versionToRestore.get(drive)) {
					String arr[] =name.split(".v");
					String headFileName = arr[0];
					try {
						if (new File(drive.getBucketObjectDataDirPath(meta.bucketName)+File.separator+VirtualFileSystemService.VERSION_DIR, name).exists()) {
							Files.copy( (new File(drive.getBucketObjectDataDirPath(meta.bucketName)+File.separator+VirtualFileSystemService.VERSION_DIR,  name)).toPath(), 
									(new File(drive.getBucketObjectDataDirPath(meta.bucketName), headFileName)).toPath(), 
									StandardCopyOption.REPLACE_EXISTING);
						}
					} catch (IOException e) {
						throw new InternalCriticalException(e, "b:"+ meta.bucketName +" o:" + meta.objectName);
					}
				}
			
			}
			return true;
			
		} catch (InternalCriticalException e) {
			throw e;
			
		} catch (Exception e) {
				throw new InternalCriticalException(e, "b:"+ meta.bucketName +" o:" + meta.objectName);
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
				getVFS().getFileCacheService().remove(meta.bucketName, meta.objectName, Optional.empty());
				
				restoreVersionObjectDataFile(meta, op.getVersion());
				restoreVersionObjectMetadata(op.getBucketName(), op.getObjectName(),  op.getVersion());
			}
			
			done = true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"), ServerConstant.NOT_THROWN);
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			else
				logger.error(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"), ServerConstant.NOT_THROWN);	
		}
		finally {
			if (done || recoveryMode) {
				op.cancel();
			}
		}
	}

	
	
}
