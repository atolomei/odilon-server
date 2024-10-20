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
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VFSOperation;

import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * <p>RAID 6 Update Object handler</p>
 * <p>Auxiliary class used by {@link RaidSixHandler}</p>
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
	* @param driver can not be null
	*/
	protected RAIDSixUpdateObjectHandler(RAIDSixDriver driver) {
	super(driver);
	}
	
	/**
	 * 
	 * @param bucket			can not be null
	 * @param objectName		can not be null
	 * @param stream			can not be null
	 * @param srcFileName
	 * @param contentType
	 * @param customTags 
	 */
	protected void update(ServerBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType, Optional<List<String>> customTags) {
	
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		Check.requireNonNullArgument(stream, "stream is null");

		String bucketName = bucket.getName();
		Long bucketId = bucket.getId();
				
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		ObjectMetadata meta = null;
		
		getLockService().getObjectLock(bucketId, objectName).writeLock().lock();
		
		try {
		
				getLockService().getBucketLock(bucketId).readLock().lock();
			
				try (stream) {
		
					if (!getDriver().getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucketId, objectName))
						throw new IllegalArgumentException(" object not found -> b:" + bucketName + " o:"+ objectName);
					
					meta = getDriver().getObjectMetadataInternal(bucket, objectName, false);
					beforeHeadVersion = meta.version;							
																
					op = getJournalService().updateObject(bucketId, objectName, beforeHeadVersion);
					
					/** backup current head version */
					backupVersionObjectDataFile(meta, meta.version);
					backupVersionObjectMetadata(bucket, objectName,  meta.version);
					
					/** copy new version as head version */
					afterHeadVersion = meta.version+1;
					RAIDSixBlocks ei = saveObjectDataFile(bucket,objectName, stream);
					saveObjectMetadata(bucket, objectName, ei, srcFileName, contentType, afterHeadVersion, meta.creationDate, customTags);

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
									logger.error("b:"+ bucketName + " o:"  + objectName +	 ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName) :"null"), SharedConstant.NOT_THROWN);
							}
						}
						else {
							/** TODO AT -> Sync by the moment. see how to make it Async */
							if ((op!=null) && (meta!=null))
								cleanUpUpdate(meta, beforeHeadVersion, afterHeadVersion);
						}
					} finally {
						getLockService().getBucketLock(bucketId).readLock().unlock();
					}
				}
		
		} finally {
			getLockService().getObjectLock(bucketId, objectName).writeLock().unlock();
		}
	}

	
	protected void updateObjectMetadataHeadVersion(ObjectMetadata meta) {
		updateObjectMetadata(meta, true); 
	}
	
	
	/**
	 * 
	 * @param meta can not be null
	 * 
	 */
	protected void updateObjectMetadata(ObjectMetadata meta, boolean isHead) {

		Check.requireNonNullArgument(meta, "meta is null");
		Check.requireNonNullArgument(meta.bucketName, "bucketName is null");
		Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketName);
		
		VFSOperation op = null;
		
		boolean done = false;
		
		getLockService().getObjectLock( meta.bucketId, meta.objectName).writeLock().lock();		
			
		try {
				getLockService().getBucketLock( meta.bucketId).readLock().lock();
				
				try {
						
					op = getJournalService().updateObjectMetadata(meta.bucketId, meta.objectName, meta.version);
			
					backupMetadata(meta);
					saveObjectMetadata(meta, isHead);
					
					done = op.commit();
						
				} catch (Exception e) {
						done=false;
						throw new InternalCriticalException(e, "b:" + meta.bucketId + ", o:" + meta.objectName); 
						
				} finally {
						try {
							if ((!done) && (op!=null)) {
									try {
										rollbackJournal(op, false);
									} catch (Exception e) {
										throw new InternalCriticalException(e, "b:" + (Optional.ofNullable(meta.bucketId).isPresent() ? (meta.bucketName) :"null")); 
									}
							}
							else {
								/** TODO AT -> Sync by the moment. TODO see how to make it Async */
								cleanUpBackupMetadataDir(meta.bucketId, meta.objectName);
							}
						} finally {
							getLockService().getBucketLock(meta.bucketId).readLock().unlock();
						}
				}
		} finally {
				getLockService().getObjectLock(meta.bucketId, meta.objectName).writeLock().unlock();
		}
	}

	 
	/**
	 * 
	 * @param bucket 		can not be null
	 * @param objectName	can not be null
	 * 
	 * @return 				ObjectMetadata of the restored object
	 * 
	 */
	 
	 
	 protected ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		String bucketName = bucket.getName();
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketName);

		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		
		boolean isMainException = false;
		
		
		ObjectMetadata metaHeadToRemove = null;
		ObjectMetadata metaToRestore = null;
		
		getLockService().getObjectLock(bucket.getId(), objectName).writeLock().lock();

			try {
				
				getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
				try {
					
					metaHeadToRemove = getDriver().getObjectMetadataInternal(bucket, objectName, false);
		
					if (metaHeadToRemove.getVersion()==0)
						throw new IllegalArgumentException(	"Object does not have any previous version | " + "b:" +	 bucketName + " o:" + objectName);
					
					
					beforeHeadVersion = metaHeadToRemove.version;
					
					
					List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
					
					for (int version=0; version<beforeHeadVersion; version++) {
						ObjectMetadata mv = getDriver().getObjectMetadataReadDrive(bucket, objectName).getObjectMetadataVersion(bucket.getId(), objectName, version);
						if (mv!=null)
							metaVersions.add(mv);
					}
		
					if (metaVersions.isEmpty()) 
						throw new OdilonObjectNotFoundException(Optional.of(metaHeadToRemove.systemTags).orElse("previous versions deleted"));
					
					op = getJournalService().restoreObjectPreviousVersion(bucket.getId(), objectName, beforeHeadVersion);
					
					/** save current head version MetadataFile .vN  and data File vN - no need to additional backup */
					backupVersionObjectDataFile(metaHeadToRemove,  metaHeadToRemove.version);
					backupVersionObjectMetadata(bucket, objectName,  metaHeadToRemove.version);
		
					/** save previous version as head */
					metaToRestore = metaVersions.get(metaVersions.size()-1);
					
					if (!restoreVersionObjectDataFile(metaToRestore, metaToRestore.version))
						throw new OdilonObjectNotFoundException(Optional.of(metaHeadToRemove.systemTags).orElse("previous versions deleted"));
					
					if (!restoreVersionObjectMetadata(bucket, metaToRestore.objectName, metaToRestore.version))
						throw new OdilonObjectNotFoundException(Optional.of(metaHeadToRemove.systemTags).orElse("previous versions deleted"));
					
					
					done = op.commit();
					
					return metaToRestore;
					
					
				} catch (Exception e) {
					done=false;
					isMainException = true;
					logger.error(e, SharedConstant.NOT_THROWN);
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
									logger.error(e, "b:" + bucketName + " o:" + objectName, SharedConstant.NOT_THROWN);
								
							} catch (Exception e) {
								if (isMainException)
									throw new InternalCriticalException(e, "b:"   +  bucketName +  " o:" + objectName);
								else
									logger.error(e, "b:" + bucketName + " o:" + objectName, SharedConstant.NOT_THROWN);	
							}
						}
						else {
							/** -------------------------
							 TODO AT ->
							 Sync by the moment
							 see how to make it Async
							------------------------ */
							if((op!=null) && (metaHeadToRemove!=null) && (metaToRestore!=null))
								cleanUpRestoreVersion(metaHeadToRemove, beforeHeadVersion, metaToRestore);
						}
					} finally {
						getLockService().getBucketLock(bucket.getId()).readLock().unlock();
					}
				}
			} finally {
				getLockService().getObjectLock(bucket.getId(), objectName).writeLock().unlock();
			}
	}


	/**
	 * 
	 */
	@Override
	protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {
			
		Check.requireNonNullArgument(op, "op is null");
		
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
	 * 
	 * 
	 * @param meta
	 * @param versionDiscarded
	 */
	private void cleanUpRestoreVersion(ObjectMetadata metaHeadRemoved, int versionDiscarded, ObjectMetadata metaNewHeadRestored) {
		
		try {
				if (versionDiscarded < 0)
					return;
	
				
				String objectName = metaHeadRemoved.objectName;
				Long bucketId  = metaHeadRemoved.bucketId;
				
				// Metadata
				for (Drive drive: getDriver().getDrivesAll()) {
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucketId, objectName,  versionDiscarded));
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucketId, objectName,  metaNewHeadRestored.version));
				}
				
				{
					List<File> files = getDriver().getObjectDataFiles(metaHeadRemoved, Optional.of(versionDiscarded));
					files.forEach(file -> FileUtils.deleteQuietly(file));
				}
				
				{
					List<File> files = getDriver().getObjectDataFiles(metaHeadRemoved, Optional.of(metaNewHeadRestored.version));
					files.forEach(file -> FileUtils.deleteQuietly(file));
				}
				
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	
	
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 * @param version
	 */
	private void backupVersionObjectMetadata(ServerBucket bucket, String objectName,	int version) {
		
		String bucketName = bucket.getName();
		Long bucketId = bucket.getId();
		
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				File file=drive.getObjectMetadataFile(bucketId, objectName);
				if (file.exists())
					drive.putObjectMetadataVersionFile(bucketId, objectName, version, file);
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
				File current = new File(drive.getBucketObjectDataDirPath(meta.bucketId), filename);
				String suffix = ".v"+ String.valueOf(headVersion);
				File backupFile = new File(drive.getBucketObjectDataDirPath(meta.bucketId) + File.separator + VirtualFileSystemService.VERSION_DIR, filename + suffix);
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
	private void saveObjectMetadata(ServerBucket bucket, String objectName, RAIDSixBlocks ei, String srcFileName, String contentType, int version, OffsetDateTime headCreationDate, Optional<List<String>> customTags) {
		
		List<String> shaBlocks = new ArrayList<String>();
		StringBuilder etag_b = new StringBuilder();
		
		ei.encodedBlocks.forEach(item -> {
			try {
				shaBlocks.add(OdilonFileUtils.calculateSHA256String(item));
			} catch (Exception e) {
				throw new InternalCriticalException(e, "saveObjectMetadata" + "b:" + bucket.getName() + " o:" 	+ objectName + ", f:" + (Optional.ofNullable(item).isPresent() ? (item.getName()):"null"));
			}
		});
		
		shaBlocks.forEach(item->etag_b.append(item));
		String etag = null;
		
		try {
			etag = OdilonFileUtils.calculateSHA256String(etag_b.toString());
		} catch (NoSuchAlgorithmException | IOException e) {
				throw new InternalCriticalException(e, "saveObjectMetadata etag" + "b:" + bucket.getName() + " o:" 	+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null"));
		} 
		
		
		OffsetDateTime versionCreationDate = OffsetDateTime.now(); 
	
		
		final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
		for (Drive drive: getDriver().getDrivesAll()) {
			
			try {
				ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
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
				if (customTags.isPresent()) 
					meta.customTags=customTags.get();
				list.add(meta);
	
			} catch (Exception e) {
				throw new InternalCriticalException(e, "saveObjectMetadata" + "b:" + bucket.getName() + " o:" 	+ objectName);
			}
		}
			
		getDriver().saveObjectMetadataToDisk(getDriver().getDrivesAll(), list, true);
	}
	
	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private RAIDSixBlocks saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream) {
		
		InputStream sourceStream = null;
		boolean isMainException = false;
		try {
				sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream;
				RAIDSixEncoder encoder = new RAIDSixEncoder(getDriver());
				return encoder.encodeHead(sourceStream, bucket.getId(), objectName);
			
			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, "saveObjectDataFile");		
	
			} finally {
				
				IOException secEx = null;
				
				try {
					if (sourceStream!=null) 
						sourceStream.close();
					
				} catch (IOException e) {
					logger.error(e, ("b:" + bucket.getName() + ", o:" + objectName) + (isMainException ? SharedConstant.NOT_THROWN :""));
					secEx=e;
				}
				if (!isMainException && (secEx!=null)) 
					throw new InternalCriticalException(secEx);
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
			
			restoreVersionObjectMetadata(getVFS().getBucketById(op.getBucketId()), op.getObjectName(),  op.getVersion());
			
			done = true;
		
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
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketId, meta.objectName);
				File src=new File(objectMetadataDirPath);
				if (src.exists()) 
					FileUtils.copyDirectory(src, new File(drive.getBucketWorkDirPath(meta.bucketId) + File.separator + meta.objectName));
				
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
	private void cleanUpBackupMetadataDir(Long bucketId, String objectName) {
		
		try {
				for (Drive drive: getDriver().getDrivesAll()) {
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucketId) + File.separator + objectName));
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
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
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(meta.bucketId, meta.objectName, previousVersion));
					List<File> files = getDriver().getObjectDataFiles(meta, Optional.of(previousVersion));
					files.forEach( file -> {FileUtils.deleteQuietly(file);});
				}
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}
	
	
	private void saveObjectMetadata(ObjectMetadata meta, boolean isHead) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		
		final List<Drive> drives = getDriver().getDrivesAll();
		final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
		
		getDriver().getDrivesAll().forEach( d -> list.add(meta));

		getDriver().saveObjectMetadataToDisk(drives, list, isHead);
		/**
		for (Drive drive: getDriver().getDrivesAll()) {
			if (isHead)
				drive.saveObjectMetadata(meta);
			else
				drive.saveObjectMetadataVersion(meta);
		}
		**/
	}
	
	/**
	 * 
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param version
	 * @return
	 */
	private boolean restoreVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		try {
			boolean success = true;
			
			ObjectMetadata versionMeta = getDriver().getObjectMetadataVersion(bucket, objectName, version);
			
			for (Drive drive: getDriver().getDrivesAll()) {
				versionMeta.drive=drive.getName();
				drive.saveObjectMetadata(versionMeta);
			}
			return success;
			
		} catch (InternalCriticalException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalCriticalException(e, "b:"+ bucket.getName() +" o:" + objectName);
		}
	}

	
	
	private boolean restoreVersionObjectDataFile(ObjectMetadata meta, int version) {

		Check.requireNonNullArgument(meta.bucketName, "bucketName is null");
		Check.requireNonNullArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketName);

		try {
	
			Map<Drive, List<String>> versionToRestore = getDriver().getObjectDataFilesNames(meta, Optional.of(version));

			for (Drive drive: versionToRestore.keySet()) {
				for (String name: versionToRestore.get(drive)) {
					String arr[] =name.split(".v");
					String headFileName = arr[0];
					try {
						if (new File(drive.getBucketObjectDataDirPath(meta.bucketId)+File.separator+VirtualFileSystemService.VERSION_DIR, name).exists()) {
							Files.copy((new File(drive.getBucketObjectDataDirPath(meta.bucketId)+File.separator+VirtualFileSystemService.VERSION_DIR,  name)).toPath(), 
									(new File(drive.getBucketObjectDataDirPath(meta.bucketId), headFileName)).toPath(), 
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
		
		final ServerBucket bucket = getVFS().getBucketById(op.getBucketId());
		
		try {
		
			if (getVFS().getServerSettings().isStandByEnabled()) 
				getVFS().getReplicationService().cancel(op);
				
			ObjectMetadata meta = getDriver().getObjectMetadataReadDrive( bucket, op.getObjectName()).getObjectMetadata(op.getBucketId(), op.getObjectName());
			
			if (meta!=null) {
				restoreVersionObjectDataFile(meta, op.getVersion());
				restoreVersionObjectMetadata(getVFS().getBucketById(op.getBucketId()), op.getObjectName(),  op.getVersion());
			}
			
			done = true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"), SharedConstant.NOT_THROWN);
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			else
				logger.error(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"), SharedConstant.NOT_THROWN);	
		}
		finally {
			if (done || recoveryMode) {
				op.cancel();
			}
		}
	}

	
	
}
