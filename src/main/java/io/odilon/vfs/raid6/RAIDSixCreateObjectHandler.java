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

/**
 * 
 */
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
	
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		try {
				getLockService().getObjectLock( bucket.getName(), objectName).writeLock().lock();
				getLockService().getBucketLock(bucket.getName()).readLock().lock();
		
				boolean exists = getDriver().getObjectMetadataReadDrive(bucket.getName(), objectName).existsObjectMetadata(bucket.getName(), objectName);
				
				if (exists)											
					throw new OdilonObjectNotFoundException("Object already exist -> b:" + bucket.getName()+ " o:"+ objectName);
				
				int version = 0;
				
				op = getJournalService().createObject(bucket.getName(), objectName);
				
				EncodedInfo ei = saveObjectDataFile(bucket,objectName, stream);
				saveObjectMetadata(bucket, objectName, ei, srcFileName, contentType, version);
				
				getVFS().getObjectCacheService().remove(bucket.getName(), objectName);
				done = op.commit();
		
			} catch (OdilonObjectNotFoundException e1) {
					done=false;
					isMainException=true;
					throw e1;
			} catch (InternalCriticalException e) {
					done=false;
					isMainException=true;
					throw e;
			} catch (Exception e) {
					done=false;
					isMainException=true;
					throw new InternalCriticalException(e, 	"b:" + bucket.getName() + " o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
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
									if (isMainException)
										logger.error("b:" + bucket.getName() + " o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"), ServerConstant.NOT_THROWN);
									else
										throw new InternalCriticalException(e, 	"b:"+ bucket.getName() + " o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
								}
							}
					}
					finally {
						getLockService().getBucketLock(bucket.getName()).readLock().unlock();
						getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();	
					}
			}
	}

	/**
	 * 
	 * 
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
			
			getVFS().getObjectCacheService().remove(bucketName, objectName);
			
			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			ObjectMetadata meta = null;
			
			for (Drive drive: getDriver().getDrivesEnabled()) {
				File f_meta = drive.getObjectMetadataFile(bucketName, objectName);
				if ((meta==null) && (f_meta!=null))
						meta=drive.getObjectMetadata(bucketName, objectName);
				FileUtils.deleteQuietly(f_meta);
			}
			
			if (meta!=null) {
				getDriver().getObjectDataFiles(meta, Optional.empty()).forEach(file -> FileUtils.deleteQuietly(file));
			}
			
			done=true;
			
		} catch (InternalCriticalException e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			if (!recoveryMode)
				throw(e);
			
		} catch (Exception e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
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
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private EncodedInfo saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream) {
		
		InputStream sourceStream = null;
		boolean isMainException = false;
		try {

			sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream;
			RSEncoder encoder = new RSEncoder(getDriver());
			return encoder.encode(stream, bucket.getName(), objectName);
				
			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e);		
	
			} finally {
				
				IOException secEx = null;
				
				try {
					
					if (sourceStream!=null) 
						sourceStream.close();
					
				} catch (IOException e) {
					logger.error(e, "b:" + bucket.getName()+", o:" + objectName + (isMainException ? ServerConstant.NOT_THROWN :""));
					secEx=e;
				}
				if ((!isMainException) && (secEx!=null)) 
					throw new InternalCriticalException(secEx);
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
	private void saveObjectMetadata(VFSBucket bucket, String objectName, EncodedInfo ei, String srcFileName, String contentType, int version) {
		
		long start = System.currentTimeMillis();
		
		List<String> shaBlocks = new ArrayList<String>();
		StringBuilder etag_b = new StringBuilder();
		
		ei.encodedBlocks.forEach(item -> {
			try {
				shaBlocks.add(ODFileUtils.calculateSHA256String(item));
			} catch (Exception e) {
				String msg =  "b:"+ bucket.getName() + " o:" + objectName+ ", f:" + (Optional.ofNullable(item).isPresent() ? (item.getName()) : "null");
			throw new InternalCriticalException(e, msg);
			}
		});

		shaBlocks.forEach(item->etag_b.append(item));
		 		
		String etag = null;
		
		try {
			etag = ODFileUtils.calculateSHA256String(etag_b.toString());
		} catch (NoSuchAlgorithmException | IOException e) {
   			throw new InternalCriticalException(e, "b:"+ bucket.getName() + " o:" 	+ objectName+ ", f:" + "| etag");
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
				String msg = "b:"+ bucket.getName() + " o:" + objectName+", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null");
				throw new InternalCriticalException(e, msg);
			}
		}
		logger.debug( String.valueOf(System.currentTimeMillis() - start) + " ms");
	}

}
