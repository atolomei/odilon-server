package io.odilon.vfs.raid6;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.Optional;

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
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;

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
	 * 
	 * @param meta
	 */
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
	 *
	 * 
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		throw new RuntimeException("not done");

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
	
		VFSOperation op = null;
		boolean done = false;
		
		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		
		try {

			getLockService().getObjectLock( bucket.getName(), objectName).writeLock().lock();
			getLockService().getBucketLock(bucket.getName()).readLock().lock();

			boolean exists = getDriver().getObjectMetadataReadDrive(bucket.getName(), objectName).existsObjectMetadata(bucket.getName(), objectName);
			
			if (!exists)
				throw new OdilonObjectNotFoundException("object does not exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
			
			ObjectMetadata meta = getDriver().getObjectMetadataInternal(bucket.getName(), objectName, true);
			beforeHeadVersion = meta.version;							
			
			op = getJournalService().updateObject(bucket.getName(), objectName, beforeHeadVersion);
			
			/** backup current head version */
			//saveVersionObjectDataFile(bucket, objectName,  meta.version);
			//saveVersionObjectMetadata(bucket, objectName,  meta.version);
			
			/** copy new version as head version */
			afterHeadVersion = meta.version+1;
			//saveObjectDataFile(bucket, objectName, stream, srcFileName, meta.version+1);
			//saveObjectMetadata(bucket, objectName, srcFileName, contentType, meta.version+1);
			
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

	public ObjectMetadata restorePreviousVersion(VFSBucket bucket, String objectName) {
		throw new RuntimeException("not done");
		
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
				
					File file = ((SimpleDrive) drive).getObjectDataFile(bucket.getName(),  objectName);
				
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

	private void cleanUpUpdate(VFSBucket bucket, String objectName, int previousVersion, int currentVersion) {
		try {
			if (!getVFS().getServerSettings().isVersionControl()) {
				for (Drive drive: getDriver().getDrivesAll()) {
				File metadata = drive.getObjectMetadataVersionFile(bucket.getName(), objectName, previousVersion);
				if (metadata.exists())
					FileUtils.deleteQuietly(metadata);
				
				File data=((SimpleDrive) drive).getObjectDataVersionFile(bucket.getName(), objectName, previousVersion);
				if (data.exists())
					FileUtils.deleteQuietly(data);
				}
				
			}
		} catch (Exception e) {
			logger.error(e, ServerConstant.NOT_THROWN);
		}
	}
}
