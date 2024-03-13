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
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VFSop;

@ThreadSafe
public class RAIDSixSyncObjectHandler extends RAIDSixHandler {
		
	private static Logger logger = Logger.getLogger(RAIDSixSyncObjectHandler.class.getName());
	
	@JsonIgnore
	private List<Drive> drives;
	
	protected RAIDSixSyncObjectHandler(RAIDSixDriver driver) {
		super(driver);
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
	public void sync(ObjectMetadata meta) {
		
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
				
				backupMetadata(meta);
				
				/** there is no need to backup data because 
				 *  existing data files are not touched. 
				 **/
				
				op = getJournalService().syncObject(bucketName, objectName);
				
				
				/** PREVIOUS VERSIONS ---------------------------------------------------- */ 
				
				for (int version=0; version < meta.version; version++) {
					
					ObjectMetadata versionMeta = getDriver().getDrivesEnabled().get(0).getObjectMetadataVersion(meta.bucketName, meta.objectName, version);
					
					/** Data (version) */
					RSDecoder decoder = new RSDecoder(getDriver());
					File file = decoder.decodeVersion(versionMeta);
					
					RSDriveInitializationEncoder driveInitEncoder = new RSDriveInitializationEncoder(getDriver(), getDrives());
					
					try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {
						
						driveInitEncoder.encodeVersion(in, bucketName, objectName, versionMeta.version);
						
					} catch (FileNotFoundException e) {
			    		throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName);
					} catch (IOException e) {
						throw new InternalCriticalException(e, "b:" + meta.bucketName +  " | o:" + meta.objectName);
					}
					
					/** Metadata (version) */
					versionMeta.dateSynced=OffsetDateTime.now();
					
					for (Drive drive:getDrives()) {
						drive.saveObjectMetadataVersion(versionMeta);
					}
				}
				
				 
				/** HEAD VERSION --------------------------------------------------------- */

				/** Data (head) */
				RSDecoder decoder = new RSDecoder(getDriver());
				File file = decoder.decodeHead(meta);
				
				RSDriveInitializationEncoder driveInitEncoder = new RSDriveInitializationEncoder(getDriver(), getDrives());
				
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
					drive.saveObjectMetadataVersion(meta);
				}
				
				done = op.commit();
				
			} finally {
				
				try {
					boolean requiresRollback = (!done) && (op!=null);
					if (requiresRollback) {
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
	
	/**
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param version
	 */
	private boolean restoreVersionObjectMetadata(String bucketName, String objectName, int versionToRestore) {
		try {
			for (Drive drive:getDrives()) {
				File file=drive.getObjectMetadataVersionFile(bucketName, objectName, versionToRestore);
				if (file.exists()) {
					drive.putObjectMetadataFile(bucketName, objectName, file);
					FileUtils.deleteQuietly(file);
			}
		}
			return true;
		} catch (Exception e) {
			throw new InternalCriticalException(e);
		}
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
		
		try {
				getLockService().getObjectLock(bucketName, objectName).writeLock().lock();
				getLockService().getBucketLock(bucketName).readLock().lock();
			
				restoreVersionObjectMetadata(op.getBucketName(), op.getObjectName(), op.getVersion());
				
				done = true;
			
		} catch (InternalCriticalException e) {
			String msg = "Rollback -> " + op.toString();
			logger.error(msg);
			if (!recoveryMode)
				throw(e);
	
		} catch (Exception e) {
			String msg = "Rollback -> " + op.toString();
			logger.error(msg);
			
			if (!recoveryMode)
				throw new InternalCriticalException(e, msg);
		}
		finally {
				if (done || recoveryMode) {
					op.cancel();
				}
				getLockService().getBucketLock(bucketName).readLock().unlock();
				getLockService().getObjectLock(bucketName, objectName).writeLock().unlock();
		}
	}

	/**
	 * 
	 * <p>copy metadata directory <br/>. 
	 * back up the full metadata directory (ie. ObjectMetadata for all versions)</p>
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backupMetadata(ObjectMetadata meta) {
		try {
			for (Drive drive: getDriver().getDrivesEnabled()) {
				String objectMetadataDirPath = drive.getObjectMetadataDirPath(meta.bucketName, meta.objectName);
				String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(meta.bucketName) + File.separator + meta.objectName;
				File src=new File(objectMetadataDirPath);
				if (src.exists())
					FileUtils.copyDirectory(src, new File(objectMetadataBackupDirPath));
			}
		} catch (IOException e) {
			throw new InternalCriticalException(e, "b:"+ meta.bucketName +" o:" + meta.objectName);
		}
	}


}
