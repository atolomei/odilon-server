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
import io.odilon.vfs.model.VFSop;
import io.odilon.vfs.model.VirtualFileSystemService;

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
	public void create(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType) {
	
		VFSOperation op = null;
		boolean done = false;
		
		try {
				getLockService().getObjectLock( bucket.getName(), objectName).writeLock().lock();
				getLockService().getBucketLock(bucket.getName()).readLock().lock();
		
				if (getDriver().getReadDrive(bucket.getName(), objectName).existsObjectMetadata(bucket.getName(), objectName))											
					throw new OdilonObjectNotFoundException("object already exist -> b:" + bucket.getName()+ " o:"+objectName);
				
				int version = 0;
				
				op = getJournalService().createObject(bucket.getName(), objectName);
				
				saveObjectDataFile(bucket,objectName, stream, srcFileName);
				saveObjectMetadata(bucket,objectName, srcFileName, contentType, version);
				
				getVFS().getObjectCacheService().remove(bucket.getName(), objectName);
				done = op.commit();
		
			} catch (OdilonObjectNotFoundException e1) {
				done=false;
				logger.error(e1);
				throw e1;
		
			} catch (Exception e) {
					done=false;
					throw new InternalCriticalException(e,	"b:" + bucket.getName() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null"));
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
									String msg = "b:" + bucket.getName() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null");
									throw new InternalCriticalException(e, msg);
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

			for (Drive drive: getDriver().getDrivesAll())
				((SimpleDrive) drive).deleteObject(bucketName , objectName);
			
			done=true;
			
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
			if (done || recoveryMode) 
				op.cancel();
		}
	}
	
		
	/**
	 * 
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private void saveObjectDataFile(VFSBucket bucket, String objectName, InputStream stream, String srcFileName) {
		
		int total_drives = getDriver().getDrivesAll().size();
		byte[] buf = new byte[ VirtualFileSystemService.BUFFER_SIZE ];

		BufferedOutputStream out[] = new BufferedOutputStream[total_drives];
		
		InputStream sourceStream = null;
		boolean isMainException = false;
		
		try {
				sourceStream = isEncrypt() ? (getVFS().getEncryptionService().encryptStream(stream)) : stream;
				
						int n_d=0;
				for (Drive drive: getDriver().getDrivesAll()) { 
					String sPath = ((SimpleDrive) drive).getObjectDataFilePath(bucket.getName(), objectName);
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
				throw new InternalCriticalException(e);		
	
			} finally {
				IOException secEx = null;
				
				if (out!=null) {
						try {
							for (int n=0; n<total_drives; n++) {
								if (out[n]!=null)
									out[n].close();
							}
						} catch (IOException e) {
							logger.error(e, "b:" + bucket.getName() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName):"null") + (isMainException ? ServerConstant.NOT_THROWN :""));
							secEx=e;
						}
				}
				
				try {
					if (sourceStream!=null) 
						sourceStream.close();
				} catch (IOException e) {
					String msg = "b:" + bucket.getName() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null");
					logger.error(e, msg + (isMainException ? ServerConstant.NOT_THROWN :""));
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
		
		String sha=null;
		String baseDrive=null;
			
		for (Drive drive: getDriver().getDrivesAll()) {
			
			File file =((SimpleDrive) drive).getObjectDataFile(bucket.getName(),  objectName);
			
			try {

				String sha256 = ODFileUtils.calculateSHA256String(file);

				if (sha==null) {
					sha=sha256;
					baseDrive=drive.getName();
				}
				else
					if (!sha256.equals(sha))											
						throw new InternalCriticalException("SHA 256 are not equal for -> d:" + baseDrive+" ->" + sha + "  vs   d:" + drive.getName()+ " -> " + sha256);
				
				ObjectMetadata meta = new ObjectMetadata(bucket.getName(), objectName);
				meta.fileName=srcFileName;
				meta.appVersion=OdilonVersion.VERSION;
				meta.contentType=contentType;
				meta.creationDate = OffsetDateTime.now();
				meta.version=version;
				meta.versioncreationDate = meta.creationDate;
				meta.length=file.length();
				meta.etag=sha256;
				meta.encrypt=getVFS().isEncrypt();
				meta.sha256=sha256;
				meta.integrityCheck=OffsetDateTime.now();
				meta.status=ObjectStatus.ENABLED;
				meta.drive=drive.getName();
				meta.raid=String.valueOf(getRedundancyLevel().getCode()).trim();
				drive.saveObjectMetadata(meta, true);
	
			} catch (Exception e) {
				String msg = "b:" + bucket.getName() + " o:"+ objectName + ", f:" + (Optional.ofNullable(srcFileName).isPresent() ? (srcFileName)	:"null");
				throw new InternalCriticalException(e, msg);
			}
		}
	}

}
