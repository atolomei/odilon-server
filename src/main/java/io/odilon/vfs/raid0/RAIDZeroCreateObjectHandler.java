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
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import io.odilon.OdilonVersion;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.util.ODFileUtils;
import io.odilon.vfs.RAIDCreateObjectHandler;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>RAID 0. Handler that creates new Objects</p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)	 
 */
@ThreadSafe
public class RAIDZeroCreateObjectHandler extends RAIDZeroHandler implements RAIDCreateObjectHandler {
		
	private static Logger logger = Logger.getLogger(RAIDZeroCreateObjectHandler.class.getName());
		
	/** 
	 * <p>Created by and used only from {@link RAIDZeroDriver}
	 * </p>
	 */
	protected RAIDZeroCreateObjectHandler(RAIDZeroDriver driver) {
		super(driver);
	}

	/**
	 * <p>The procedure is the same whether there is version control or not</p>
	 * 
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 * @param contentType
	 */
	public void create(@NonNull VFSBucket bucket, @NonNull String objectName, @NonNull InputStream stream,String srcFileName, String contentType) {
	
		VFSOperation op = null;
		boolean done = false;
		
		try {
			getLockService().getObjectLock( bucket.getName(), objectName).writeLock().lock();
				try {
						getLockService().getBucketLock(bucket.getName()).readLock().lock();

						if (getDriver().getWriteDrive(bucket.getName(), objectName).existsObjectMetadata(bucket.getName(), objectName))											
							throw new OdilonObjectNotFoundException("object already exist -> b:" + bucket.getName()+ " o:"+(Optional.ofNullable(objectName).isPresent() ? (objectName) :"null"));
						
						int version = 0;
						
						op = getJournalService().createObject(bucket.getName(), objectName);
						
						saveObjectDataFile(bucket,objectName, stream, srcFileName);
						saveObjectMetadata(bucket,objectName, srcFileName, contentType, version);
						
						getVFS().getObjectCacheService().remove(bucket.getName(), objectName);
						done = op.commit();
					
				} catch (OdilonObjectNotFoundException e1) {
					done=false;
					throw e1;
					
				} catch (Exception e) {
							done=false;
							throw new InternalCriticalException(e, "b:" + bucket.getName() + " o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null"));
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
										} catch (InternalCriticalException e) {
											throw e;
										} catch (Exception e) {
											throw new InternalCriticalException(e, " finally | b:" + bucket.getName() +	" o:" 	+ objectName + ", f:" 	+ (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null"));
										}
									}
							}
							finally {
								getLockService().getBucketLock(bucket.getName()).readLock().unlock();
							}
				}
		}
		finally {
			getLockService().getObjectLock(bucket.getName(), objectName).writeLock().unlock();
		}
	}

	/**
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
			if (getVFS().getServerSettings().isStandByEnabled())
				getVFS().getReplicationService().cancel(op);
			
			getWriteDrive(bucketName, objectName).deleteObjectMetadata(bucketName, objectName);
			File data = new File (getWriteDrive(bucketName, objectName).getRootDirPath(), bucketName + File.separator + objectName);
			FileUtils.deleteQuietly(data);
			// ((SimpleDrive) getWriteDrive(bucketName, objectName)).deleteObject(bucketName , objectName);
			done=true;
			
		} catch (InternalCriticalException e) {
			logger.error("Rollback | " + op.toString());
			if (!recoveryMode)
				throw(e);
			
		} catch (Exception e) {
			String msg = "Rollback | " + op.toString();
			logger.error(msg);
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
		}
		finally {
			if (done || recoveryMode) 
				op.cancel();
		}
	}
	
	
	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private void saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream, String srcFileName) {
		
		byte[] buf = new byte[VirtualFileSystemService.BUFFER_SIZE];

		BufferedOutputStream out = null;
		InputStream sourceStream = null;
		boolean isMainException = false;
		
		try {
				sourceStream = isEncrypt() ? getVFS().getEncryptionService().encryptStream(stream) : stream;
				out = new BufferedOutputStream(new FileOutputStream(((SimpleDrive) getWriteDrive(bucket.getName(), objectName)).getObjectDataFilePath(bucket.getName(), objectName)), VirtualFileSystemService.BUFFER_SIZE);
				int bytesRead;
				while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0) {
					out.write(buf, 0, bytesRead);
				}
			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, "b:" + bucket.getName() + "o:" + objectName);		
	
			} finally {
				IOException secEx = null;
				try {
						
					if (out!=null)
						out.close();
						
					} catch (IOException e) {
						logger.error(e, "b:"  + bucket.getName() + ", o:" + objectName + ", f:" + srcFileName + (isMainException ? ServerConstant.NOT_THROWN :""));
						secEx=e;
					}
				
				try {
					
					if (sourceStream!=null) 
						sourceStream.close();
					
				} catch (IOException e) {
					logger.error(e, "b:"  + bucket.getName() + ", o:" + objectName + ", f:" + srcFileName + (isMainException ? ServerConstant.NOT_THROWN :""));
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
	private void saveObjectMetadata(VFSBucket bucket, String objectName, String srcFileName, String contentType, int version) {
		
		OffsetDateTime now=OffsetDateTime.now();
		Drive drive=getWriteDrive(bucket.getName(), objectName);
		File file=((SimpleDrive)drive).getObjectDataFile(bucket.getName(), objectName);
		
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
				meta.etag=sha256; /** sha256 is calculated on the encrypted file  **/
				meta.integrityCheck = now;
				meta.sha256=sha256;
				meta.status=ObjectStatus.ENABLED;
				meta.drive=drive.getName();
				meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
				
				drive.saveObjectMetadata(meta);
			
		} catch (Exception e) {
				throw new InternalCriticalException(e, "b:"  + bucket.getName() + ", o:" + objectName + ", f:" + srcFileName);
		}
	}
}
