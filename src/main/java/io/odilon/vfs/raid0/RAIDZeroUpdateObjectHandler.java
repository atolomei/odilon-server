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
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.ODFileUtils;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>RAID 0. Update Handler</p>
 * <p>All {@link RAIDHandler} are used internally by the corresponding RAID Driver</p>
 * 
 *  @author atolomei@novamens.com (Alejandro Tolomei)  
 */
@ThreadSafe
public class RAIDZeroUpdateObjectHandler extends RAIDZeroHandler {
			
	private static Logger logger = Logger.getLogger(RAIDZeroUpdateObjectHandler.class.getName());
	
	/**
	 * <p>All {@link RAIDHandler} are used internally by the 
	 * corresponding RAID Driver. in this case by {@code RAIDZeroDriver}</p>
	 */
	protected RAIDZeroUpdateObjectHandler(RAIDZeroDriver driver) {
		super(driver);
	}
	
	/**
	 * 
	 * 
	 */

	protected void update(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		
		String bucketName = bucket.getName();	
		
		VFSOperation op = null;
		boolean done = false;

		boolean isMaixException = false;
		
		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			try (stream) {
			
				if (!getDriver().getWriteDrive(bucketName, objectName).existsObjectMetadata(bucketName, objectName))
					throw new IllegalArgumentException("object does not exist -> b:" + bucketName + " o:"+ objectName);
				
				ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, false);
										
				beforeHeadVersion = meta.version;							
				
				op = getJournalService().updateObject(bucketName, objectName, meta.version);
				
				/** backup current head version */
				saveVersionObjectDataFile(bucketName, objectName,  meta.version);
				saveVersionObjectMetadata(bucketName, objectName,  meta.version);
				
				/** copy new version  head version */
				afterHeadVersion = meta.version+1;
				saveObjectDataFile(bucket, objectName, stream, srcFileName, meta.version+1);
				saveObjectMetadataHead(bucket, objectName, srcFileName, contentType, meta.version+1);
				
				done = op.commit();
			
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				isMaixException=true;
				throw e1;
				
			} catch (Exception e) {
				done=false;
				isMaixException=true;
				throw new InternalCriticalException(e, "b:" + bucketName + ", o:"	+ objectName + ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName): "null"));
				
			} finally {
				
				try {
					
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							if (!isMaixException)
								throw new InternalCriticalException("b:" + bucketName + ", o:"	+ objectName + ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName): "null"));
							else
								logger.error(e, "b:" + bucketName + ", o:"	+ objectName + ", f:"	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName): "null")+ SharedConstant.NOT_THROWN);
						}
					}
					else {
						/** TODO AT -> This is Sync by the moment, see how to make it Async */
						cleanUpUpdate(op, bucket, objectName, beforeHeadVersion, afterHeadVersion);
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
	 * <p>
	 * . The Object does not have a previous version (ie. version=0)
	 * . The Object previous versions were deleted by a {@code deleteObjectAllPreviousVersions}
	 * </p>
	 */
	protected ObjectMetadata restorePreviousVersion(VFSBucket bucket, String objectName) {

		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		
		String bucketName = bucket.getName();	

		int beforeHeadVersion = -1;
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			try {
				
				ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucketName, objectName, false);
				
				if (meta.version==0)
					throw new IllegalArgumentException(	"Object does not have any previous version | " + "b:" + bucketName + ", o:"	+ objectName);
				
				beforeHeadVersion = meta.version;
				List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();
				
				for (int version=0; version<beforeHeadVersion; version++) {
					ObjectMetadata mv = getDriver().getReadDrive(bucketName, objectName).getObjectMetadataVersion(bucketName, objectName, version);
					if (mv!=null)
						metaVersions.add(mv);
				}
	
				if (metaVersions.isEmpty()) 
					throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
				
				op = getJournalService().restoreObjectPreviousVersion(bucketName, objectName, beforeHeadVersion);
				
				/** save current head version MetadataFile .vN  and data File vN - no need to additional backup */
				saveVersionObjectDataFile(bucketName, objectName,  meta.version);
				saveVersionObjectMetadata(bucketName, objectName,  meta.version);
	
				/** save previous version as head */
				ObjectMetadata metaToRestore = metaVersions.get(metaVersions.size()-1);
	
				if (!restoreVersionObjectDataFile(metaToRestore.bucketName, metaToRestore.objectName, metaToRestore.version))
					throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
				
				if (!restoreVersionObjectMetadata(metaToRestore.bucketName, metaToRestore.objectName, metaToRestore.version))
					throw new OdilonObjectNotFoundException(Optional.of(meta.systemTags).orElse("previous versions deleted"));
				
				done = op.commit();
				
				return metaToRestore;
				
				
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				isMainException = true;
				e1.setErrorMessage( e1.getErrorMessage() + " | " + "b:" + bucketName + ", o:"	+ objectName);
				throw e1;
				
			} catch (Exception e) {
				done=false;
				isMainException = true;
				throw new InternalCriticalException(e, "b:" + bucketName + ", o:"	+ objectName);
				
			} finally {
				
				try {
					
					if ((!done) && (op!=null)) {
						
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							if (!isMainException)
								throw new InternalCriticalException(e, "b:" + bucketName + ", o:"	+ objectName);
							else
								logger.error(e, " finally | b:" + bucketName +	" o:" 	+ objectName +  SharedConstant.NOT_THROWN);
						}
					}
					else {
						/** 
						 TODO AT ->Sync by the moment see how to make it Async */
						if ((op!=null) && ((beforeHeadVersion>=0))) {
							try {
									FileUtils.deleteQuietly(getDriver().getWriteDrive(bucket.getName(), objectName).getObjectMetadataVersionFile(bucket.getName(), objectName,  beforeHeadVersion));
									FileUtils.deleteQuietly(((SimpleDrive) getDriver().getWriteDrive(bucket.getName(), objectName)).getObjectDataVersionFile(bucket.getName(), objectName,  beforeHeadVersion));
							} catch (Exception e) {
							logger.error(e, SharedConstant.NOT_THROWN);
							}
						}
					}	
				} finally {
					getLockService().getBucketLock(bucketName).readLock().unlock();
				}
			}
		}
		finally {
			getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();
		}
	}
	

	/**
	 * <p>This update does not generate a new Version of the ObjectMetadata. 
	 * It maintains the same ObjectMetadata version.<br/>
	 * The only way to version Object is when the Object Data is updated</p>
	 * 
	 * @param meta
	 */
	
	protected void updateObjectMetadata(ObjectMetadata meta) {
		
		Check.requireNonNullArgument(meta, "meta is null");
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		getLockService().getObjectLock( meta.bucketName, meta.objectName).writeLock().lock();
		
		try {

			getLockService().getBucketLock( meta.bucketName).readLock().lock();

			try {
	
				op = getJournalService().updateObjectMetadata(meta.bucketName, meta.objectName, meta.version);
				backupMetadata(meta);
				getWriteDrive(meta.bucketName, meta.objectName).saveObjectMetadata(meta);
				done = op.commit();
				
			} catch (Exception e) {
				isMainException = true;
				done=false;
				throw new InternalCriticalException(e, "b:"  + meta.bucketName + ", o:" + meta.objectName); 
				
			} finally {
				
				try {
					if ((!done) && (op!=null)) {
							try {
								rollbackJournal(op, false);
							} catch (Exception e) {
								
								if (!isMainException)
									throw new InternalCriticalException(e, "b:"  + meta.bucketName + ", o:" + meta.objectName);
								else
									logger.error(e, " finally | b:" + meta.bucketName +	" o:" 	+ meta.objectName +  SharedConstant.NOT_THROWN);
							}
					}
					else {
						/** TODO AT ->  Delete backup Metadata. Sync by the moment see how to make it Async. */
						try {
							FileUtils.deleteQuietly(new File(getDriver().getWriteDrive(meta.bucketName, meta.objectName).getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName));
						} catch (Exception e) {
							logger.error(e, SharedConstant.NOT_THROWN);
						}
						
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
	 * </p>There is nothing to do here by the moment</p>
	 */
	
	protected void onAfterCommit(VFSBucket bucket, String objectName, int previousVersion, int currentVersion) {
	}


	
	/**
	 * <p>The procedure is the same for Version Control </p>
	 */
	
	protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue( (op.getOp()==VFSop.UPDATE_OBJECT ||
							op.getOp()==VFSop.UPDATE_OBJECT_METADATA ||
							op.getOp()==VFSop.RESTORE_OBJECT_PREVIOUS_VERSION
							), "VFSOperation can not be  ->  op: " + op.getOp().getName());

		switch (op.getOp()) {

			case UPDATE_OBJECT: {
					rollbackJournalUpdate(op, recoveryMode);
					break;
			}
			
			case UPDATE_OBJECT_METADATA: {
					rollbackJournalUpdateMetadata(op, recoveryMode);
					break;
			}
			
			case RESTORE_OBJECT_PREVIOUS_VERSION: { 
					rollbackJournalUpdate(op, recoveryMode);
					break;
			}
			default: {
				break;
			}
		}
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
			if (!recoveryMode)
				throw(e);
			else
				logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: | " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			else
				logger.error("Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
		}
		finally {
			if (done || recoveryMode) {
				op.cancel();
			}
		}
	}

	/**
	 * <p>The </p> 
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
			if (!recoveryMode)
				throw(e);
			else
				logger.error(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"));
			
		} catch (Exception e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
			else
				logger.error(e, msg);
		}
		finally {
			if (done || recoveryMode) {
				op.cancel();
			}
		}
	}
	
 
	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param newVersion
	 */
	private void saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, int newVersion) {
		
		byte[] buf = new byte[ VirtualFileSystemService.BUFFER_SIZE ];

		BufferedOutputStream out = null;
		
		boolean isMainException = false;
		
		try (InputStream sourceStream = isEncrypt() ? getVFS().getEncryptionService().encryptStream(stream) : stream) {
			
			out = new BufferedOutputStream(new FileOutputStream( ((SimpleDrive) getWriteDrive(bucket.getName(), objectName)).getObjectDataFilePath(bucket.getName(), objectName)), VirtualFileSystemService.BUFFER_SIZE);
			int bytesRead;
			while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0) {
				out.write(buf, 0, bytesRead);
			}
			
		} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e);		
	
		} finally {
			IOException secEx = null;
			try {
						
				if (out!=null)
					out.close();
						
				} catch (IOException e) {
					String msg ="b:"  + (Optional.ofNullable(bucket).isPresent() ? (bucket.getName()) :"null") + ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName):"null") + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null"); 
					logger.error(e, msg + (isMainException ? SharedConstant.NOT_THROWN :""));
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
	private void saveObjectMetadataHead(VFSBucket bucket, String objectName, String srcFileName, String contentType, int version) {
		
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
				meta.etag=sha256; /** sha256 is calculated on the encrypted file */
				meta.integrityCheck = now;
				meta.sha256=sha256;
				meta.status=ObjectStatus.ENABLED;
				meta.drive=drive.getName();
				meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
				drive.saveObjectMetadata(meta);
			
		} catch (Exception e) {
				throw new InternalCriticalException(e);
		}
	}

	/**
	 * 
	 */
	private void saveVersionObjectMetadata(String bucketName , String objectName,	int version) {
		try {
			Drive drive=getWriteDrive(bucketName, objectName);
			File file=drive.getObjectMetadataFile(bucketName, objectName);
			drive.putObjectMetadataVersionFile(bucketName, objectName, version, file);
		} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucketName +	" o:"+ objectName);
		}
		
	}

	/**
	 * 
	 */									
	private void saveVersionObjectDataFile(String bucketName, String objectName, int version) {
		try {
			Drive drive=getWriteDrive(bucketName, objectName);
			File file=((SimpleDrive) drive).getObjectDataFile(bucketName, objectName);
			((SimpleDrive) drive).putObjectDataVersionFile(bucketName, objectName, version, file);
		} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucketName +	" o:"+ objectName);
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
				throw new InternalCriticalException(e, "b:" + bucketName +	" o:"+ objectName);
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
				throw new InternalCriticalException(e,  " restoreVersionObjectDataFile | b:" + bucketName +	" o:" 	+ objectName);
		}
	}

	/**
	 * 
	 * copy metadata directory
	 */
	private void backupMetadata(ObjectMetadata meta) {
		try {
			String objectMetadataDirPath = getDriver().getWriteDrive(meta.bucketName, meta.objectName).getObjectMetadataDirPath(meta.bucketName, meta.objectName);
			String objectMetadataBackupDirPath = getDriver().getWriteDrive(meta.bucketName, meta.objectName).getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName;
			File src=new File(objectMetadataDirPath);
			if (src.exists())
				FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, "backupMetadata | b:" +  meta.bucketName +	" o:" 	+ meta.objectName);
		}
	}

	
	/**
	 *
	 * restore metadata directory 
	 * 
	 */
	private void restoreMetadata(String bucketName, String objectName) {

		String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucketName, objectName).getBucketWorkDirPath(bucketName) + File.separator + objectName;
		String objectMetadataDirPath = getDriver().getWriteDrive(bucketName, objectName).getObjectMetadataDirPath(bucketName, objectName);
		try {
			FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:"   + (Optional.ofNullable(bucketName).isPresent()    ? (bucketName) :"null") + ", o:" + (Optional.ofNullable(objectName).isPresent() ? (objectName)    :"null"));
		}
	}
	
	/**
	 * <p>This clean up is executed after the commit by the transaction thread, and therefore
	 * all locks are still applied. Also it is required to be fast<br/>
	 * 
	 * <b>TODO AT</b> -> <i>This method should be Async</i><br/>
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
	 * @param op				can be null (no need to do anything)  
	 * @param bucket			not null
	 * @param objectName		not null
	 * @param previousVersion	>=0
	 * @param currentVersion    >0 
	 */
	private void cleanUpUpdate(VFSOperation op, VFSBucket bucket, String objectName, int previousVersion, int currentVersion) {
		
		if (op==null)
			return;
		
		try {
			if (!getVFS().getServerSettings().isVersionControl()) {
				FileUtils.deleteQuietly(getDriver().getWriteDrive(bucket.getName(), objectName).getObjectMetadataVersionFile(bucket.getName(), objectName, previousVersion));
				FileUtils.deleteQuietly(((SimpleDrive) getDriver().getWriteDrive(bucket.getName(), objectName)).getObjectDataVersionFile(bucket.getName(), objectName, previousVersion));
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}
	
}
