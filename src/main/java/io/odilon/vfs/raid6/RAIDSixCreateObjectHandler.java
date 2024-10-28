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
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSOp;

/**
 * <b>RAID 6 Object creation</b> 
 * <p>Auxiliary class used by {@link RaidSixHandler}</p>
 * 
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
	 * @param customTags 
	 */
	protected void create(ServerBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType, Optional<List<String>> customTags) {
	
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		Long bucketId = bucket.getId();
				
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		try {
			
			getLockService().getObjectLock(bucket.getId(), objectName).writeLock().lock();
			
			try (stream) {
				
					getLockService().getBucketLock(bucket.getId()).readLock().lock();
			
					if (getDriver().getObjectMetadataReadDrive(bucket, objectName).existsObjectMetadata(bucket.getId(), objectName))											
						throw new IllegalArgumentException("Object already exist -> " + getDriver().objectInfo(bucket, objectName));
					
					int version = 0;
					
					op = getJournalService().createObject(bucketId, objectName);
					
					RAIDSixBlocks ei = saveObjectDataFile(bucketId, objectName, stream);
					saveObjectMetadata(bucket, objectName, ei, srcFileName, contentType, version, customTags);
					done = op.commit();
			
				} catch (InternalCriticalException e) {
						done=false;
						isMainException=true;
						throw e;
				} catch (Exception e) {
						done=false;
						isMainException=true;
						throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName,srcFileName));
				} finally {
						try {
								if ((!done) && (op!=null)) {
									try {
										rollbackJournal(op, false);
									} catch (Exception e) {
										if (isMainException)
											logger.error(getDriver().objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
										else
											throw new InternalCriticalException(e, 	getDriver().objectInfo(bucket, objectName, srcFileName));
									}
								}
						}
						finally {
							getLockService().getBucketLock(bucketId).readLock().unlock();
						}
				}
		} finally {
			getLockService().getObjectLock(bucketId, objectName).writeLock().unlock();	
		}
	}

	/**
	 * <p>VFSop.CREATE_OBJECT</p>
	 * 
	 */
	@Override
	protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");
		Check.checkTrue(op.getOp()==VFSOp.CREATE_OBJECT, "Invalid op ->  " + op.getOp().getName());
		
		String objectName = op.getObjectName();
		Long bucketId = op.getBucketId();
		
		boolean done = false;
				
		try {
			
			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			ObjectMetadata meta = null;

			/** remove metadata dir on all drives */
			for (Drive drive: getDriver().getDrivesAll()) {
				File f_meta = drive.getObjectMetadataFile(bucketId, objectName);
				if ((meta==null) && (f_meta!=null)) {
					try {
						meta=drive.getObjectMetadata(bucketId, objectName);
					} catch (Exception e) {
						logger.warn("can not load meta -> d: " + drive.getName() + SharedConstant.NOT_THROWN);
					}
				}
				FileUtils.deleteQuietly(new File(drive.getObjectMetadataDirPath(bucketId, objectName)));
			}
			
			/** remove data dir on all drives */			
			if (meta!=null)
				getDriver().getObjectDataFiles(meta, Optional.empty()).forEach(file -> {FileUtils.deleteQuietly(file);});
			
			done=true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error(e, "Rollback: " + op.toString() + SharedConstant.NOT_THROWN);
			
		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, "Rollback: " + op.toString() + SharedConstant.NOT_THROWN);
			else
				logger.error(e, "Rollback: " + op.toString() + SharedConstant.NOT_THROWN);
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
	private RAIDSixBlocks saveObjectDataFile(Long bucketId, String objectName, InputStream stream) {
			Check.requireNonNullArgument(bucketId, "bucketId is null");			
			try (InputStream sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream) {
				return (new RAIDSixEncoder(getDriver())).encodeHead(sourceStream, bucketId, objectName);
			} catch (Exception e) {
				throw new InternalCriticalException(e, getDriver().objectInfo(bucketId.toString(), objectName));		
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
	private void saveObjectMetadata(ServerBucket bucket, String objectName, RAIDSixBlocks ei, String srcFileName, String contentType, int version, Optional<List<String>> customTags) {
		
		List<String> shaBlocks = new ArrayList<String>();
		StringBuilder etag_b = new StringBuilder();
		
		ei.encodedBlocks.forEach(item -> {
			try {
				shaBlocks.add(OdilonFileUtils.calculateSHA256String(item));
			} catch (Exception e) {
				throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName) + ", f:" + (Optional.ofNullable(item).isPresent() ? (item.getName()) : "null"));
			}
		});

		shaBlocks.forEach(item->etag_b.append(item));
		 		
		String etag = null;
		
		try {
			etag = OdilonFileUtils.calculateSHA256String(etag_b.toString());
		} catch (NoSuchAlgorithmException | IOException e) {
   			throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName,srcFileName) + ", f:" + "| etag");
		} 

		OffsetDateTime creationDate=OffsetDateTime.now();
		
		 final List<Drive> drives = getDriver().getDrivesAll();
		 final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
		 
		 for (Drive drive: getDriver().getDrivesAll()) {

				try {
					ObjectMetadata meta = new ObjectMetadata(bucket.getId(), objectName);
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
					if (customTags.isPresent()) 
						meta.customTags=customTags.get();
					
					list.add(meta);
		
				} catch (Exception e) {
					throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName,srcFileName));
				}
		 }
		 
		 /** save in parallel */
		 getDriver().saveObjectMetadataToDisk(drives, list, true);
	}

}
