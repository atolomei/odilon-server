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
import io.odilon.util.ODFileUtils;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.ODBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * <p>RAID 1 Handler <br/>  
 * Creates new Objects ({@link VFSop.CREATE_OBJECT})</p>

 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDOneCreateObjectHandler extends RAIDOneHandler {

	private static Logger logger = Logger.getLogger(RAIDOneCreateObjectHandler.class.getName());

	/**
	 * <p>Instances of this class are used
	 * internally by {@link RAIDOneDriver}<p>
	 * 
	 * @param driver
	 */
	protected RAIDOneCreateObjectHandler(RAIDOneDriver driver) {
		super(driver);
	}

	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
	 */
	protected void create(ODBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType) {
					
		Check.requireNonNullArgument(bucket, "bucket is null");
		//String bucketName = bucket.getName();
		Long bucketId=bucket.getId();
		
		
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketId.toString());
		
		VFSOperation op = null;
		boolean done = false;
		boolean isMainException = false;
		
		getLockService().getObjectLock(bucketId, objectName).writeLock().lock();
		
		try  {
		
			getLockService().getBucketLock(bucketId).readLock().lock();
			
			try (stream) {
				
					if (getDriver().getReadDrive(bucketId, objectName).existsObjectMetadata(bucketId, objectName))											
						throw new IllegalArgumentException("object already exist -> b:" + bucketId.toString() + " o:"+objectName);
					
					int version = 0;
					
					op = getJournalService().createObject(bucket.getId(), objectName);
					
					saveObjectDataFile(bucket.getId(), objectName, stream, srcFileName);
					saveObjectMetadata(bucket.getId(), objectName, srcFileName, contentType, version);
					
					done = op.commit();
							
				} catch (Exception e) {
						done=false;
						isMainException=true;
						throw new InternalCriticalException(e, "b:" + bucket.getId().toString() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
				} finally {
						try {
								if ((!done) && (op!=null)) {
									try {
										rollbackJournal(op, false);
									} catch (Exception e) {
										if (!isMainException)
											throw new InternalCriticalException(e, "b:" + bucket.getId().toString() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
										else
											logger.error(e, " finally | b:" + bucketId.toString() +	" o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null") +  SharedConstant.NOT_THROWN);
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
	 * 
	 * <p>The only operation that should be called here by {@link RAIDSixDriver} 
	 * is {@link VFSop.CREATE_OBJECT}</p>
	 * 
	 */
	@Override
	protected void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		
		Check.requireNonNullArgument(op, "op is null");
		Check.checkTrue(op.getOp()==VFSop.CREATE_OBJECT, "Invalid op ->  " + op.getOp().getName());
		
		String objectName = op.getObjectName();
		Long  bucketId = op.getBucketId();
		
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucketId.toString());
		
		boolean done = false;
		
		try {
			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);

			for (Drive drive: getDriver().getDrivesAll()) {
				drive.deleteObjectMetadata(bucketId, objectName);
				FileUtils.deleteQuietly(new File (drive.getRootDirPath(), bucketId.toString() + File.separator + objectName));
			}
			done=true;
			
		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				throw(e);
			else
				logger.error(e, "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null"), SharedConstant.NOT_THROWN);
			
		} catch (Exception e) {
			String msg = "Rollback: " + (Optional.ofNullable(op).isPresent()? op.toString():"null");
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
			else
				logger.error(e, msg + SharedConstant.NOT_THROWN);
		}
		finally {
			if (done || recoveryMode) 
				op.cancel();
		}
	}
	
		
	/**
	 * <p>This method is not ThreadSafe. Locks must be applied by the caller </p>
	 * 
	 * @param bucket 
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private void saveObjectDataFile(Long bucketId, String objectName, InputStream stream, String srcFileName) {
		
		int total_drives = getDriver().getDrivesAll().size();
		byte[] buf = new byte[ VirtualFileSystemService.BUFFER_SIZE ];

		BufferedOutputStream out[] = new BufferedOutputStream[total_drives];
		
		boolean isMainException = false;
		
		try (InputStream sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream) {
				;
				
				int n_d=0;
				for (Drive drive: getDriver().getDrivesAll()) { 
					String sPath = ((SimpleDrive) drive).getObjectDataFilePath(bucketId, objectName);
					out[n_d++] = new BufferedOutputStream(new FileOutputStream(sPath), VirtualFileSystemService.BUFFER_SIZE);
				}
				int bytesRead;
				
				/**	TODO AT -> this step can be in parallel and using out of heap buffers */ 
				while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0) {
					for (int bytes=0; bytes<total_drives; bytes++) {
						 out[bytes].write(buf, 0, bytesRead);
					 }
				}
				
			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, "b:" + bucketId.toString() + " o:"+ objectName);		
	
			} finally {
				IOException secEx = null;
				
				if (out!=null) {
						try {
							for (int n=0; n<total_drives; n++) {
								if (out[n]!=null)
									out[n].close();
							}
						} catch (IOException e) {
							logger.error(e, "b:" + bucketId.toString() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null") + (isMainException ? SharedConstant.NOT_THROWN :""));
							secEx=e;
						}
				}
				
				if (!isMainException && (secEx!=null)) 
				 		throw new InternalCriticalException(secEx);
			}
				
	}

	/**
	 * <p>This method is not ThreadSafe. Locks must be applied by the caller </p>
	 * 
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private void saveObjectMetadata(Long bucketId, String objectName, String srcFileName, String contentType, int version) {
		
		String sha=null;
		String baseDrive=null;
			
		OffsetDateTime now = OffsetDateTime.now();
		
		
		for (Drive drive: getDriver().getDrivesAll()) {
			
			File file =((SimpleDrive) drive).getObjectDataFile(bucketId,  objectName);
			
			try {
				String sha256 = ODFileUtils.calculateSHA256String(file);
				if (sha==null) {
					sha=sha256;
					baseDrive=drive.getName();
				}
				else {
					if (!sha256.equals(sha))											
						throw new InternalCriticalException("SHA 256 are not equal for -> d:" + baseDrive+" ->" + sha + "  vs   d:" + drive.getName()+ " -> " + sha256);
				}
				
				ObjectMetadata meta = new ObjectMetadata(bucketId, objectName);
				meta.fileName=srcFileName;
				meta.appVersion=OdilonVersion.VERSION;
				meta.contentType=contentType;
				meta.creationDate =  now;
				meta.version=version;
				meta.versioncreationDate = meta.creationDate;
				meta.length=file.length();
				meta.etag=sha256;
				meta.encrypt=getVFS().isEncrypt();
				meta.sha256=sha256;
				meta.integrityCheck= now;
				meta.status=ObjectStatus.ENABLED;
				meta.drive=drive.getName();
				meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
				drive.saveObjectMetadata(meta);
	
			} catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucketId.toString() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
			}
		}
	}

}
