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
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.OdilonVersion;
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

/**
 * <b>RAID 6</b> 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@ThreadSafe
public class RAIDSixCreateObjectHandler extends RAIDSixHandler {
																
	private static Logger logger = Logger.getLogger(RAIDSixCreateObjectHandler.class.getName());
	
	/**
	 * <p>Instances of this class are used
	 * internally by {@link RAIDSixDriver}<p>
	 * 
	 * @param driver
	 */
	protected RAIDSixCreateObjectHandler(RAIDSixDriver driver) {
		super(driver);
	}

	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
	 */
	public void create(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType) {
	
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		
		String bucketName = bucket.getName();

		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		try {
			
			getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
			
			try (stream) {
				
					getLockService().getBucketLock(bucketName).readLock().lock();
			
					if (getDriver().getObjectMetadataReadDrive(bucketName, objectName).existsObjectMetadata(bucket.getName(), objectName))											
						throw new IllegalArgumentException("Object already exist -> b:" + bucketName+ " o:"+ objectName);
					
					int version = 0;
					
					op = getJournalService().createObject(bucketName, objectName);
					
					RAIDSixBlocks ei = saveObjectDataFile(bucketName, objectName, stream);
					saveObjectMetadata(bucketName, objectName, ei, srcFileName, contentType, version);
					
					// cache 
					//
					// getVFS().getObjectCacheService().rem ove(bucketName, objectName);
					
					// getVFS().getFileCacheService().rem ove(bucketName, objectName, Optional.empty());
					// getVFS().getFileCacheService().rem ove(bucketName, objectName, Optional.of(version));
					
					done = op.commit();
					
			
				} catch (InternalCriticalException e) {
						done=false;
						isMainException=true;
						throw e;
				} catch (Exception e) {
						done=false;
						isMainException=true;
						throw new InternalCriticalException(e, "b:" + bucketName + " o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
				} finally {
						try {
								if ((!done) && (op!=null)) {
									try {
										rollbackJournal(op, false);
									} catch (Exception e) {
										if (isMainException)
											logger.error("b:" + bucketName + " o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"), ServerConstant.NOT_THROWN);
										else
											throw new InternalCriticalException(e, 	"b:"+ bucketName + " o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
									}
								}
						}
						finally {
							getLockService().getBucketLock(bucketName).readLock().unlock();
						}
				}
		} finally {
			getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();	
		}
	}

	/**
	 * <p>VFSop.CREATE_OBJECT</p>
	 * 
	 */
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");
		Check.checkTrue(op.getOp()==VFSop.CREATE_OBJECT, "Invalid op ->  " + op.getOp().getName());
		
		String objectName = op.getObjectName();
		String bucketName = op.getBucketName();
		
		boolean done = false;
				
		try {
			
			//getVFS().getObjectCacheService().rem ove(bucketName, objectName);
			
			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			ObjectMetadata meta = null;

			/** remove metadata dir on all drives */
			for (Drive drive: getDriver().getDrivesAll()) {
				File f_meta = drive.getObjectMetadataFile(bucketName, objectName);
				if ((meta==null) && (f_meta!=null)) {
					try {
						meta=drive.getObjectMetadata(bucketName, objectName);
					} catch (Exception e) {
						logger.warn("can not load meta -> d: " + drive.getName() + ServerConstant.NOT_THROWN);
					}
				}
				FileUtils.deleteQuietly(new File(drive.getObjectMetadataDirPath(bucketName, objectName)));
			}
			
			/** remove data dir on all drives */			
			if (meta!=null)
				getDriver().getObjectDataFiles(meta, Optional.empty()).forEach(file -> {FileUtils.deleteQuietly(file);});
			
			done=true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error(e, "Rollback: " + op.toString() + ServerConstant.NOT_THROWN);
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + op.toString() + ServerConstant.NOT_THROWN);
			else
				logger.error(e, "Rollback: " + op.toString() + ServerConstant.NOT_THROWN);
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
	 */
	private RAIDSixBlocks saveObjectDataFile(String bucketName, String objectName, InputStream stream) {
		
			try (InputStream sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream) {
				
				return (new RAIDSixEncoder(getDriver())).encodeHead(sourceStream, bucketName, objectName);
				
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucketName + " o:" + objectName);		
			}
	}

	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * 
	 * 
	 * todo en el object metadata o cada file por separado
	 * 
	 */
	private void saveObjectMetadata(String bucketName, String objectName, RAIDSixBlocks ei, String srcFileName, String contentType, int version) {
		
		List<String> shaBlocks = new ArrayList<String>();
		StringBuilder etag_b = new StringBuilder();
		
		ei.encodedBlocks.forEach(item -> {
			try {
				shaBlocks.add(ODFileUtils.calculateSHA256String(item));
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:"+ bucketName + " o:" + objectName+ ", f:" + (Optional.ofNullable(item).isPresent() ? (item.getName()) : "null"));
			}
		});

		shaBlocks.forEach(item->etag_b.append(item));
		 		
		String etag = null;
		
		try {
			etag = ODFileUtils.calculateSHA256String(etag_b.toString());
		} catch (NoSuchAlgorithmException | IOException e) {
   			throw new InternalCriticalException(e, "b:"+ bucketName + " o:" + objectName+ ", f:" + "| etag");
		} 

		OffsetDateTime creationDate=OffsetDateTime.now();
		
		for (Drive drive: getDriver().getDrivesAll()) {

			try {
				ObjectMetadata meta = new ObjectMetadata(bucketName, objectName);
				meta.fileName=srcFileName;
				meta.appVersion=OdilonVersion.VERSION;
				meta.contentType=contentType;
				meta.creationDate = creationDate;
				meta.version=version;
				meta.versioncreationDate = meta.creationDate;
				meta.length=ei.fileSize;
				meta.sha256Blocks=shaBlocks;
				meta.totalBlocks=ei.encodedBlocks.size();
				meta.etag=etag;
				meta.encrypt=getVFS().isEncrypt();
				meta.integrityCheck=meta.creationDate;
				meta.status=ObjectStatus.ENABLED;
				meta.drive=drive.getName();
				meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
				drive.saveObjectMetadata(meta);
	
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:"+ bucketName + " o:" + objectName+", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null"));
			}
		}
	}

}
