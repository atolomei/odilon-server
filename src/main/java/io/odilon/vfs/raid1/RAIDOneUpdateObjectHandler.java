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
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.ODFileUtils;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.ODBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSOp;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 *	<p>RAID 1. Update Handler</p>
 * 
 *	<ul>
 *	<li>VFSop.UPDATE_OBJECT</li>
 *	<li>VFSop.UPDATE_OBJECT_METADATA</li>
 *	<li>VFSop.RESTORE_OBJECT_PREVIOUS_VERSION</li>
 *	<ul>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneUpdateObjectHandler extends RAIDOneHandler {
			
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
	
	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
	 */
	
	protected void update(ODBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType) {

		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		boolean isMainException = false;
		
		getLockService().getObjectLock(bucket.getId(), objectName).writeLock().lock();

		try  {
			
				getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
				try {
							
					if (!getDriver().getReadDrive(bucket, objectName).existsObjectMetadata(bucket.getId(), objectName))
						throw new IllegalArgumentException("object does not exist -> b:" +bucket.getId()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
					
					ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket, objectName, false);
					beforeHeadVersion = meta.version;							
					
					op = getJournalService().updateObject(bucket.getId(), objectName, beforeHeadVersion);
					
					/** backup current head version */
					saveVersionObjectDataFile(bucket, objectName,  meta.version);
					saveVersionObjectMetadata(bucket, objectName,  meta.version);
					
					/** copy new version as head version */
					afterHeadVersion = meta.version+1;
					saveObjectDataFile(bucket, objectName, stream, srcFileName, meta.version+1);
					saveObjectMetadata(bucket, objectName, srcFileName, contentType, meta.version+1);
					
					done = op.commit();
					
				} catch (Exception e) {
					done=false;
					isMainException = true;
					throw new InternalCriticalException(e, "b:" +bucket.getId() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
					
				} finally {
					
					try {
						try {
							if (stream!=null) 
								stream.close();
						} catch (IOException e) {
							logger.error(e, SharedConstant.NOT_THROWN);
						}

						if (!done) {
							try {
								rollbackJournal(op, false);
								
							} catch (Exception e) {
								String msg = "b:" +bucket.getId() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null");
								if (!isMainException)
									throw new InternalCriticalException(e, msg);
								else
									logger.error(e,msg + SharedConstant.NOT_THROWN);
							}
						}
						else {
							/** TODO AT -> this is after commit, Sync by the moment. see how to make it Async */
							cleanUpUpdate(op, bucket, objectName, beforeHeadVersion, afterHeadVersion);
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
	 * 
	 */
	protected ObjectMetadata restorePreviousVersion(ODBucket bucket, String objectName) {
	
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		int beforeHeadVersion = -1;
		
		getLockService().getObjectLock(bucket.getId(), objectName).writeLock().lock();
		
		try {
			
				getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
				try {
					
					ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket, objectName, false);
		
					if (meta.version==0)
						throw new IllegalArgumentException(	"Object does not have any previous version | " + "b:" + 
															(Optional.ofNullable(bucket).isPresent() ? (bucket.getId())  :"null") +
								 							", o:"	+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null"));
					
					
					beforeHeadVersion = meta.version;
					List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
					
					for (int version=0; version<beforeHeadVersion; version++) {
					
						ObjectMetadata mv = getDriver().getReadDrive(bucket, objectName).getObjectMetadataVersion(bucket.getId(), objectName, version);
						
						if (mv!=null)
							metaVersions.add(mv);
					}
		
					if (metaVersions.isEmpty()) 
						throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
					
					op = getJournalService().restoreObjectPreviousVersion(bucket.getId(), objectName, beforeHeadVersion);
					
					/** save current head version MetadataFile .vN  and data File vN - no need to additional backup */
					saveVersionObjectDataFile(bucket, objectName,  meta.version);
					saveVersionObjectMetadata(bucket, objectName,  meta.version);
		
					/** save previous version as head */
					ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size()-1);
					
					if (!restoreVersionObjectDataFile(bucket, metaToRestore.objectName, metaToRestore.version))
						throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
					
					if (!restoreVersionObjectMetadata(bucket, metaToRestore.objectName, metaToRestore.version))
						throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
					
					done = op.commit();
					
					return metaToRestore;
					
					
				} catch (OdilonObjectNotFoundException e1) {
					done=false;
					isMainException=true;
					 e1.setErrorMessage( e1.getErrorMessage() + " | " +  "b:" +bucket.getId() + " o:"+ objectName );
					throw e1;
					
				} catch (Exception e) {
					done=false;
					isMainException=true;
					throw new InternalCriticalException(e, "b:" + bucket.getName() + " o:"+ objectName);
					
				} finally {
					
					try {
						if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
								
							} catch (Exception e) {
								String msg = "b:" + bucket.getName() + " o:"+ objectName;						
								if (!isMainException)
									throw new InternalCriticalException(e, msg);
								else
									logger.error(e, msg, SharedConstant.NOT_THROWN);
							}
						}
						else {
							/** this is after commit, Sync by the moment see how to make it Async */
							cleanUpRestoreVersion(op, bucket, objectName, beforeHeadVersion);
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
	 * <p>This update does not generate a new Version of the ObjectMetadata. It maintains the same ObjectMetadata version.<br/>
	 * The only way to version Object is when the Object Data is updated</p>
	 * 
	 * @param meta
	 */
	protected void updateObjectMetadata(ObjectMetadata meta) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		ODBucket bucket = getVFS().getBucketById(op.getBucketId());
		
		
		getLockService().getObjectLock(bucket.getId(), meta.objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
			try {
			
				op = getJournalService().updateObjectMetadata(bucket.getId(), meta.objectName, meta.version);
				
				backupMetadata(meta);
				saveObjectMetadata(meta);
				
				done = op.commit();
				
			} catch (Exception e) {
				done=false;
				String msg = "b:" + bucket.getName() + " o:"+ meta.objectName;
				isMainException = true;
				throw new InternalCriticalException(e,  msg); 
				
			} finally {
				
				try {
					if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
							} catch (Exception e) {
								if (!isMainException)
									throw new InternalCriticalException(e,   "b:" + meta.bucketId.toString() + " o:"+ meta.objectName );
								else
									logger.error(e, "b:" + meta.bucketId.toString() + " o:"+ meta.objectName, SharedConstant.NOT_THROWN);
							}
					}
					else {
						/** -------------------------
						 TODO AT -> Sync by the moment  see how to make it Async
						------------------------ */
						cleanUpBackupMetadataDir(bucket, meta.objectName);
					}
					
				} finally {
					getLockService().getBucketLock(bucket.getId()).readLock().unlock();
				}
			} 
		} 
		finally {
			getLockService().getObjectLock(bucket.getId(), meta.objectName).writeLock().unlock();
		}

	}

	/**
	 *
	 *
	 */
	
	protected void onAfterCommit(ODBucket bucket, String objectName, int previousVersion, int currentVersion) {}
	
	
	protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue( (op.getOp()==VFSOp.UPDATE_OBJECT || 	op.getOp()==VFSOp.UPDATE_OBJECT_METADATA ||	op.getOp()==VFSOp.RESTORE_OBJECT_PREVIOUS_VERSION ), VFSOperation.class.getSimpleName() + " can not be  ->  op: " + op.getOp().getName());

		if (op.getOp()==VFSOp.UPDATE_OBJECT)
			rollbackJournalUpdate(op, recoveryMode);

		else if (op.getOp()==VFSOp.UPDATE_OBJECT_METADATA)
			rollbackJournalUpdateMetadata(op, recoveryMode);
		
		else if (op.getOp()==VFSOp.RESTORE_OBJECT_PREVIOUS_VERSION) 
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
			
			ODBucket bucket = getVFS().getBucketById(op.getBucketId());
			
			restoreVersionObjectDataFile(bucket, op.getObjectName(),  op.getVersion());
			restoreVersionObjectMetadata(bucket, op.getObjectName(),  op.getVersion());
			
			done = true;
			
		} catch (InternalCriticalException e) {
			logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			if (!recoveryMode)
				throw(e);
			
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

									
	private void rollbackJournalUpdateMetadata(VFSOperation op, boolean recoveryMode) {
		
		boolean done = false;
		
		try {

			if (getVFS().getServerSettings().isStandByEnabled()) 
				getVFS().getReplicationService().cancel(op);
			
			ODBucket bucket = getVFS().getBucketById(op.getBucketId());
			
			restoreVersionObjectMetadata(bucket, op.getObjectName(),  op.getVersion());
			
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
	
	private void saveVersionObjectMetadata(ODBucket bucket, String objectName,	int version) {
		try {
		
			for (Drive drive: getDriver().getDrivesAll())
				drive.putObjectMetadataVersionFile(bucket.getId(), objectName, version, drive.getObjectMetadataFile(bucket.getId(), objectName));
			
		} catch (Exception e) {
				throw new InternalCriticalException(e);
		}
		
	}

	private void saveVersionObjectDataFile(ODBucket bucket, String objectName, int version) {
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				File file= ((SimpleDrive) drive).getObjectDataFile(bucket.getId(), objectName);
				((SimpleDrive) drive).putObjectDataVersionFile(bucket.getId(), objectName, version, file);
			}
		} catch (Exception e) {
				throw new InternalCriticalException(e);
		}
	}
	
	
	private void saveObjectDataFile(ODBucket bucket, String objectName, InputStream stream, String srcFileName, int newVersion) {
		
		int total_drives = getDriver().getDrivesAll().size();
		byte[] buf = new byte[ VirtualFileSystemService.BUFFER_SIZE ];

		BufferedOutputStream out[] = new BufferedOutputStream[total_drives];
		InputStream sourceStream = null;
		
		boolean isMainException = false;
		
		try {

			sourceStream = isEncrypt() ? getVFS().getEncryptionService().encryptStream(stream) : stream;
			
			int n_d=0;
			for (Drive drive: getDriver().getDrivesAll()) { 
				String sPath = ((SimpleDrive) drive).getObjectDataFilePath(bucket.getId(), objectName);
				out[n_d++] = new BufferedOutputStream(new FileOutputStream(sPath), VirtualFileSystemService.BUFFER_SIZE);
			}
			int bytesRead;
			
			while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0)
				for (int bytes=0; bytes<total_drives; bytes++) {
					 out[bytes].write(buf, 0, bytesRead);
				 }
				
			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e,  "b:"   + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getId())  :"null") + 
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
								String msg ="b:"   	+ (Optional.ofNullable(bucket).isPresent()    ? (bucket.getId()) :"null") + 
										", o:" 		+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
										", f:" 		+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null"); 
								logger.error(e, msg + (isMainException ? SharedConstant.NOT_THROWN :""));
								secEx=e;
							}	
					}
						
					try {
						if (sourceStream!=null) 
							sourceStream.close();
					} catch (IOException e) {
						String msg ="b:" 		+ (Optional.ofNullable(bucket).isPresent()    ? (bucket.getId()) :"null") + 
									", o:" 		+ (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
									", f:" 		+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null");
						logger.error(e, msg + (isMainException ? SharedConstant.NOT_THROWN :""));
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
	private void saveObjectMetadata(ODBucket bucket, String objectName, String srcFileName, String contentType, int version) {
		
		OffsetDateTime now =  OffsetDateTime.now();
		String sha=null;
		String basedrive=null;
		
		try {

			for (Drive drive: getDriver().getDrivesAll()) {
				
					File file = ((SimpleDrive) drive).getObjectDataFile(bucket.getId(),  objectName);
				
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
						
						ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
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
						String msg =  	"b:"   + (Optional.ofNullable(bucket).isPresent()    ? (bucket.getId()) :"null") + 
										", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)       :"null") +  
										", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)     :"null");
						
						logger.error(e,msg);
						throw new InternalCriticalException(e, msg);
					}
			}
			
		} catch (Exception e) {
				throw new InternalCriticalException(e,"b:" + bucket.getName() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
		}
	}
	
	private boolean restoreVersionObjectMetadata(ODBucket bucket, String objectName, int version) {
		try {

			boolean success = true;
			for (Drive drive: getDriver().getDrivesAll()) {
				File file=drive.getObjectMetadataVersionFile(bucket.getId(), objectName, version);
				if (file.exists()) {
					drive.putObjectMetadataFile(bucket.getId(), objectName, file);
					FileUtils.deleteQuietly(file);
				}
				else
					success=false;
			}
			return success;
		} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucket.getName() + " o:"+ objectName);
		}
	}


	private boolean restoreVersionObjectDataFile(ODBucket bucket, String objectName, int version) {
		try {
			boolean success = true;
			for (Drive drive: getDriver().getDrivesAll()) {
				File file= ((SimpleDrive) drive).getObjectDataVersionFile(bucket.getId(), objectName,version);
				if (file.exists()) {
					((SimpleDrive) drive).putObjectDataFile(bucket.getId(), objectName, file);
					FileUtils.deleteQuietly(file);
				}
				else
					success=false;
			}
			return success;
		} catch (Exception e) {
				throw new InternalCriticalException(e);
		}
	}
	
	
	
	/**
	 * 
	 * 
	 * @param op					can be null
	 * @param bucket				not null
	 * @param objectName			not null
	 * @param versionDiscarded		if<0 do nothing  
	 */
	private void cleanUpRestoreVersion(VFSOperation op, ODBucket bucket, String objectName, int versionDiscarded) {
		
		if ((op==null) || (versionDiscarded<0))
			return;
		
		try {
				for (Drive drive: getDriver().getDrivesAll()) {
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket.getId(), objectName,  versionDiscarded));
					FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataVersionFile(bucket.getId(), objectName,  versionDiscarded));
				}
		} catch (Exception e) {
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
		Check.requireNonNullArgument(meta, "meta is null");
		try {
			for (Drive drive: getDriver().getDrivesAll()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketId, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(meta.bucketId) + File.separator + meta.objectName;
				File src=new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, meta.toString());
		}
	}

	/**
	 * 
	 * @param op 				can be null (do nothing)
	 * @param bucket			not null
	 * @param objectName		not null
	 * @param previousVersion	>=0
	 * @param currentVersion	> 0
	 */
	private void cleanUpUpdate(VFSOperation op, ODBucket bucket, String objectName, int previousVersion, int currentVersion) {
		
		if (op==null)
			return;
		
		try {
		
			Check.requireNonNullArgument(bucket, "meta is null");
			
			if (!getVFS().getServerSettings().isVersionControl()) {
				for (Drive drive: getDriver().getDrivesAll()) {
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket.getId(), objectName, previousVersion));
					FileUtils.deleteQuietly(((SimpleDrive) drive).getObjectDataVersionFile(bucket.getId(), objectName, previousVersion));
				}
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}
	
	
	private void cleanUpBackupMetadataDir(ODBucket bucket, String objectName) {
		
		try {
			/** delete backup Metadata */
			for (Drive drive: getDriver().getDrivesAll()) {
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket.getId()) + File.separator + objectName));
			}
		} catch (Exception e) {
			logger.error(e, bucket.getName() +" o: " +objectName, SharedConstant.NOT_THROWN);
		}
	}
	
}
