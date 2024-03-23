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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixSyncObjectHandler extends RAIDSixHandler {
		
	private static Logger logger = Logger.getLogger(RAIDSixSyncObjectHandler.class.getName());
	
	@JsonIgnore
	private List<Drive> drives;
	
	protected RAIDSixSyncObjectHandler(RAIDSixDriver driver) {
		super(driver);
	}
	
	/**
	 * 
	 * @param meta
	 */
	public void sync(ObjectMetadata meta) {
							
		Check.requireNonNullArgument(meta, "meta is null");
		Check.requireNonNullStringArgument(meta.bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(meta.objectName, "objectName is null or empty | b:" + meta.bucketName);
		
		String bucketName = meta.bucketName;
		String objectName = meta.objectName;
		
		VFSOperation op = null;
		boolean done = false;
		
		try {
														
			getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
			
			try {
			
				getLockService().getBucketLock(bucketName).readLock().lock();
				
				/** backup metadata, there is no need to backup data because existing data files are not touched. **/
				backupMetadata(meta);
				
				op = getJournalService().syncObject(bucketName, objectName);
				
				
				/** PREVIOUS VERSIONS ---------------------------------------------------- */ 
				
				if (getDriver().getVFS().getServerSettings().isVersionControl()) {
					for (int version=0; version < meta.version; version++) {
						
						ObjectMetadata versionMeta = getDriver().getObjectMetadataReadDrive(bucketName, objectName).getObjectMetadataVersion(bucketName, objectName, version);
						
						/** Data (version) */
						RAIDSixDecoder decoder = new RAIDSixDecoder(getDriver());
						File file = decoder.decodeVersion(versionMeta);
						
						RAIDSixSDriveSyncEncoder driveEncoder = new RAIDSixSDriveSyncEncoder(getDriver(), getDrives());
						
						try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {
							
							/** encodes version without saving existing blocks, only the ones that go to the new drive/s */
							driveEncoder.encodeVersion(in, bucketName, objectName, versionMeta.version);
							
						} catch (FileNotFoundException e) {
				    		throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName);
						} catch (IOException e) {
							throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName);
						}
						
						/** Metadata (version) */
						/** changes the date of sync in order to prevent this object's sync if the process is re run */ 
						versionMeta.dateSynced=OffsetDateTime.now();
						for (Drive drive:getDrives()) {
							drive.saveObjectMetadataVersion(versionMeta);
						}
					}
				}
				 
				/** HEAD VERSION --------------------------------------------------------- */

				/** Data (head) */
				RAIDSixDecoder decoder = new RAIDSixDecoder(getDriver());
				File file = decoder.decodeHead(meta);
				
				RAIDSixSDriveSyncEncoder driveInitEncoder = new RAIDSixSDriveSyncEncoder(getDriver(), getDrives());
				
				try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {
					driveInitEncoder.encodeHead(in, bucketName, objectName);
				} catch (FileNotFoundException e) {
		    		throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName );
				} catch (IOException e) {
					throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName );
				}

				/** MetaData (head) */
				meta.dateSynced=OffsetDateTime.now();
				
				for (Drive drive:getDrives()) {
					drive.saveObjectMetadata(meta);
				}
				
				done = op.commit();
				
				//logger.debug("Sync -> " +  "b:" + meta.bucketName +  " | o:" + meta.objectName);
				
			} finally {
				
				try {
					if ((!done) && (op!=null)) {
						try {
							rollbackJournal(op, false);
						} catch (Exception e) {
							throw new InternalCriticalException(e, "b:"+ bucketName + " o:"  + objectName);
						}
					}
				}finally  {
					getLockService().getBucketLock(bucketName).readLock().unlock();
				}
			}
		} finally {
			getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();

		}
		
		
	}
	
	
	@Override
	public void rollbackJournal(VFSOperation op, boolean recoveryMode) {
		Check.requireNonNullArgument(op, "op is null");
		Check.requireTrue(op.getOp()==VFSop.SYNC_OBJECT_NEW_DRIVE,VFSOperation.class.getName() + "can not be  ->  op: " + op.getOp().getName());
		
		getVFS().getObjectCacheService().remove(op.getBucketName(), op.getObjectName());
		
		switch (op.getOp()) {
					case SYNC_OBJECT_NEW_DRIVE: 
					{	
						execRollback(op, recoveryMode);
						break;
					}
					default: {
						break;	
					}
		}
	}

	protected synchronized List<Drive> getDrives() {
		
		if (this.drives!=null)
			return this.drives;
		
		this.drives = new ArrayList<Drive>();
		
		getDriver().getDrivesAll().forEach( d -> drives.add(d));
		this.drives.sort( new Comparator<Drive>() {
			@Override
			public int compare(Drive o1, Drive o2) {
				try {
					return o1.getDriveInfo().getOrder()<o2.getDriveInfo().getOrder()?-1:1;
				} catch (Exception e) {
					return 0;
				}
			}
		});
		
		return this.drives;
		
	}

	
	
	/**
	 * 
	 * @param op
	 * @param recoveryMode
	 */
	private void execRollback(VFSOperation op, boolean recoveryMode) {
	
		boolean done = false;
		
		String bucketName = op.getBucketName();
		String objectName = op.getObjectName();
		
		getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
		
		try {
			getLockService().getBucketLock(bucketName).readLock().lock();
			
			try {
					restoreMetadata(bucketName, objectName);
					done = true;
				
			} catch (InternalCriticalException e) {
				if (!recoveryMode)
					throw(e);
				else
					logger.error("Rollback -> " + op.toString(), ServerConstant.NOT_THROWN);
		
			} catch (Exception e) {
				String msg = "Rollback -> " + op.toString();
				if (!recoveryMode)
					throw new InternalCriticalException(e, msg);
				else
					logger.error(e, msg + ServerConstant.NOT_THROWN);
			}
			finally {
				try {	
					if (done || recoveryMode) {
							op.cancel();
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
	 * <p>copy metadata directory <br/>. 
	 * back up the full metadata directory (ie. ObjectMetadata for all versions)</p>
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ObjectMetadata meta) {
		try {
			for (Drive drive: getDriver().getDrivesEnabled()) {
				File src	= new File(drive.getObjectMetadataDirPath(meta.bucketName, meta.objectName));
				File dest	= new File(drive.getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName);
				
				if (src.exists())
					FileUtils.copyDirectory(src, dest);
			}
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:"+ meta.bucketName +" o:" + meta.objectName);
		}
	}
				
	private void restoreMetadata(String bucketName, String objectName) {
		try {
			for (Drive drive: getDriver().getDrivesEnabled()) {
				File dest = new File(drive.getObjectMetadataDirPath(bucketName, objectName));
				File src  = new File(drive.getBucketWorkDirPath(bucketName) + File.separator + objectName);
				if (src.exists())
					FileUtils.copyDirectory(src, dest);
				else
					throw new InternalCriticalException("backup dir does not exist " + "b:"+ bucketName +" o:" + objectName + "dir:" + src.getAbsolutePath());
			}
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:"+ bucketName +" o:" + objectName);
		}
	}
	

}
