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
package io.odilon.vfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.OdilonVersion;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.AbstractServiceRequest;
import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.service.util.ByteToString;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveBucket;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.ODBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.IODriver;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 *<p>
 * Base class for VirtualFileSystemDrivers ({@link IODriver}):
 * <br/>
 * RAID 0: {@link RAIDZeroDriver}, <br/>
 *  RAID 1: {@link RAIDOneDriver},  <br/>
 *  RAID 6: {@link RAIDSixDriver} <br/> 
 * </p>
 *
 * @see {@link RAIDZeroDriver} {@link RAIDOneDriver} {@link RAIDSixDriver}
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class BaseIODriver implements IODriver, ApplicationContextAware {
				
	private static Logger logger = Logger.getLogger(BaseIODriver.class.getName());
	static private Logger std_logger = Logger.getLogger("StartupLogger");
	
	@JsonIgnore
	static final public int MAX_CACHE_SIZE = 4000000;
	
	@JsonIgnore
	static private ObjectMapper mapper = new ObjectMapper();
	   
	static  {
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.registerModule(new Jdk8Module());
	}
	
	@JsonIgnore
	private VirtualFileSystemService VFS;

	@JsonIgnore
	private LockService vfsLockService;
	
	@JsonIgnore
	private List<Drive> drivesEnabled;
	
	@JsonIgnore
	private List<Drive> drivesAll;

	@JsonIgnore
	private ApplicationContext applicationContext;

	
	/**
	 * @param vfs
	 * @param vfsLockService
	 */
	public BaseIODriver(VirtualFileSystemService vfs, LockService vfsLockService) {
		this.VFS=vfs;
		this.vfsLockService=vfsLockService;
	}

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public ODBucket createBucket(String bucketName) {
	
		Check.requireNonNullArgument(bucketName, "bucketName is null");

		BucketMetadata meta = new BucketMetadata(bucketName);
		VFSOperation op = null;
		boolean done = false;

		meta.status = BucketStatus.ENABLED;
		meta.appVersion = OdilonVersion.VERSION;
		meta.id=getVFS().getNextBucketId();
		
		ODBucket bucket = new ODVFSBucket(meta);

		getLockService().getBucketLock(meta.id).writeLock().lock();
		
		try {

			if (getVFS().existsBucket(bucketName))
				throw new IllegalArgumentException("bucket already exist | b: " + bucketName);

			op = getJournalService().createBucket(meta.id);

			OffsetDateTime now = OffsetDateTime.now();

			meta.creationDate = now;
			meta.lastModified = now;

			for (Drive drive : getDrivesAll()) {
				try {
					
					drive.createBucket(meta);
					
				} catch (Exception e) {
					done = false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			
			done = op.commit();
			return bucket;
			
		} finally {
			try {
				if (done) {
					getVFS().addBucketCache(bucket);
				}
				else
					rollbackJournal(op);

			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				getLockService().getBucketLock(meta.id).writeLock().unlock();
			}
		}
	}


	
	/**
	 * 
	 * 
	 */
	@Override
	public ODBucket renameBucket(ODBucket bucket, String newBucketName) {
									
		Check.requireNonNullArgument(bucket, "bucket is null");
		
		VFSOperation op = null;
		boolean done = false;
		
		OffsetDateTime now = OffsetDateTime.now();
		
		
		getLockService().getBucketLock(bucket.getId()).writeLock().lock();
		
		BucketMetadata meta = null;
		
		String oldName = bucket.getName();
		
		try {
		
			if (getVFS().existsBucket(newBucketName))
					throw new IllegalArgumentException("bucketName already used | b: " + newBucketName);

			op = getJournalService().updateBucket(bucket);

			backupBucketMetadata(bucket);
			
			meta = bucket.getBucketMetadata();	
			meta.lastModified=now;
			meta.bucketName=newBucketName;

			for (Drive drive: getDrivesAll()) {
				try {
					drive.updateBucket(meta);
				} catch (Exception e) {
					done=false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			
			done=op.commit();
			return bucket;
		}
		finally {
			try {
				if (done) {
					getVFS().updateBucketCache(oldName, new ODVFSBucket(meta));
				}
				else {
					if (op!=null)
						rollbackJournal(op);
				}
				
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
			finally {
				getLockService().getBucketLock(bucket.getId()).writeLock().unlock();
			}
		}
		
	}

	
	/**
	 * 
	 * @param bucket
	 */
	protected void restoreBucketMetadata(ODBucket bucket) {
		try {
			for (Drive drive: getDrivesAll()) {
				String path=drive.getBucketWorkDirPath(bucket.getId()) + File.separator + "bucketmetadata-" + bucket.getId().toString() + ServerConstant.JSON;
				BucketMetadata meta = getObjectMapper().readValue(Paths.get(path).toFile(), BucketMetadata.class);
				drive.updateBucket(meta);
			}
		} catch (Exception e) {
			throw new InternalCriticalException(e, "b:" + bucket.getName());
		}
	}

	/**
	 * @param bucket
	 */
	protected void backupBucketMetadata(ODBucket bucket) {
		try {
			for (Drive drive: getDrivesAll()) {
				BucketMetadata meta = drive.getBucketMetadata(bucket.getId());
				String path=drive.getBucketWorkDirPath(bucket.getId()) + File.separator + "bucketmetadata-" + bucket.getId().toString() + ServerConstant.JSON;
				Files.writeString(Paths.get(path), getObjectMapper().writeValueAsString(meta));
			}
		} catch (Exception e) {
			throw new InternalCriticalException(e, "b:" + bucket.getName());
		}
	}

	
	
	
	
	
	
	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */

	@Override
	public void deleteBucket(ODBucket bucket) {
		getVFS().removeBucket(bucket);
	}


	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */

	
	@Override
	public boolean isEmpty(ODBucket bucket) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireTrue(existsBucketInDrives(bucket.getId()), "bucket does not exist in all drives -> b: " + bucket.getName());
		
		try {
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
			for (Drive drive: getDrivesEnabled()) {
				if (!drive.isEmpty(bucket.getId()))
					return false;
			}
			return true;
		} catch (Exception e) {
			String msg = "b:" + (Optional.ofNullable(bucket).isPresent() ? (bucket.getName()) : "null");
			logger.error(e, msg);
			throw new InternalCriticalException(e, msg);

		} finally {
			getLockService().getBucketLock(bucket.getId()).readLock().unlock();
		}
	}
	
	

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	public void rollbackJournal(VFSOperation op) {
		rollbackJournal(op, false);
	}
	
	
	

	/**
	 * <p>ObjectMetadata is copied to all drives as regular files. 
	 * Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public ObjectMetadata getObjectMetadataPreviousVersion(ODBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		
		getLockService().getObjectLock(bucket.getId(), objectName).readLock().lock();
		
		try {
			
			getLockService().getBucketLock(bucket.getId()).readLock().lock();
		
			try {
				List<ObjectMetadata> list = getObjectMetadataVersionAll(bucket, objectName);
				if (list!=null && !list.isEmpty())
					return list.get(list.size()-1);
				
				return null;
			}
			catch (Exception e) {
				throw new InternalCriticalException(e, "b:" + bucket.getName() + ", o:" +objectName);
			}
			finally {
				getLockService().getBucketLock(bucket.getId()).readLock().unlock();
			}
		}
		finally { 
			getLockService().getObjectLock(bucket.getId(), objectName).readLock().unlock();
		}
		
	}
	
	public abstract RedundancyLevel getRedundancyLevel();
	
	public ObjectMapper getObjectMapper() {
		return mapper;
	}
	
	public LockService getLockService() {
		return this.vfsLockService;
	}
	
	public JournalService getJournalService() {
		return this.VFS.getJournalService();
	}
		
	public SchedulerService getSchedulerService() {
		return this.VFS.getSchedulerService();
	}
	
	public VirtualFileSystemService getVFS() {
		return VFS;
	}

	public void setVFS(VirtualFileSystemService vfs) {
		this.VFS = vfs;
	}

	/**
	 * 
	 * Save metadata
	 * Save stream
	 * 
	 * @param folderName
	 * @param objectName
	 * @param file
	 */
	@Override
	public void putObject(ODBucket bucket, String objectName, File file) {
		
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		Check.requireNonNullArgument(file, "file is null | b:" + bucket.getName());

		Path filePath = file.toPath();

		if (!Files.isRegularFile(filePath))
			throw new IllegalArgumentException("'" + file.getName() + "' -> not a regular file");

		String contentType = null;
		
		try {
			 contentType = Files.probeContentType(filePath);
		 } catch (IOException e) {
				throw new InternalCriticalException(e, "b:" + bucket.getName() + ", o:" +objectName);
		 }
		try {
			putObject(bucket, objectName, new BufferedInputStream(new FileInputStream(file)), file.getName(), contentType);
		} catch (FileNotFoundException e) {
			throw new InternalCriticalException(e, "b:" + bucket.getName() + ", o:" +objectName);
		}
	}
	
	
	public ApplicationContext getApplicationContext()  {
		return this.applicationContext;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
	}
	

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public synchronized List<ServiceRequest> getSchedulerPendingRequests(String queueId) {
	
		List<ServiceRequest> list = new ArrayList<ServiceRequest>();
		Map<String, File> useful = new HashMap<String, File>();
		Map<String, File> useless = new HashMap<String, File>();
		
		Map<Drive, Map<String, File>> allDriveFiles = new HashMap<Drive, Map<String, File>>();
		
		for (Drive drive: getDrivesEnabled()) {
			allDriveFiles.put(drive, new HashMap<String, File>());
			for (File file: drive.getSchedulerRequests(queueId)) {
				allDriveFiles.get(drive).put(file.getName(), file);
			}
		}
		
		final Drive referenceDrive = getDrivesEnabled().get(0);
		
		allDriveFiles.get(referenceDrive).forEach((k,file) -> {
			boolean isOk = true;
			for (Drive drive: getDrivesEnabled()) {
					if (!drive.equals(referenceDrive)) {
						if (!allDriveFiles.get(drive).containsKey(k)) {
							isOk=false;
							break;
						}
					}
			}
			if (isOk)
				useful.put(k, file);
			else
				useless.put(k, file);
		});

		useful.forEach((k,file) -> {
			try {
				AbstractServiceRequest request = getObjectMapper().readValue(file, AbstractServiceRequest.class);
				list.add((ServiceRequest) request);
				
			} catch (IOException e) {
				logger.debug(e, "f:" + (Optional.ofNullable(file).isPresent() ? (file.getName()) :"null"));
				try {
					Files.delete(file.toPath());
				} catch (IOException e1) {
					logger.error(e, SharedConstant.NOT_THROWN);
				}
			}
		}
		);
		
		getDrivesEnabled().forEach( drive -> {
			allDriveFiles.get(drive).forEach( (k,file) -> {
					if (!useful.containsKey(k)) {
						try {
							Files.delete(file.toPath());
						} catch (Exception e1) {
							logger.error(e1, SharedConstant.NOT_THROWN);
						}			
					}
			});
		});
		return list;
	}

	

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public List<VFSOperation> getJournalPending(JournalService journalService) {
		
		List<VFSOperation> list = new ArrayList<VFSOperation>();
		
		for (Drive drive: getDrivesEnabled()) {
			File dir = new File(drive.getJournalDirPath());
			if (!dir.exists())
				return list;
			if (!dir.isDirectory())
				return list;
			File[] files = dir.listFiles();
			for (File file: files) {
				if (!file.isDirectory()) {
					Path pa=Paths.get(file.getAbsolutePath());
					try {
						String str=Files.readString(pa);
						ODVFSOperation op = getObjectMapper().readValue(str, ODVFSOperation.class);
						op.setJournalService(getJournalService());
						if (!list.contains(op))
							list.add(op);
						
					} catch (IOException e) {
						logger.debug(e, "f:" + (Optional.ofNullable(file).isPresent() ? (file.getName()) :"null"));
						try {
							Files.delete(file.toPath());
						} catch (IOException e1) {
							logger.error(e, SharedConstant.NOT_THROWN);
						}
					}				
				}
			}
		}
		
		std_logger.debug("Total operations that will rollback -> " + String.valueOf(list.size()));
		return list;
	}


	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public OdilonServerInfo getServerInfo() {
		File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
		if (file==null || !file.exists())
			return null;
		try {
				getLockService().getServerLock().readLock().lock();
				return getObjectMapper().readValue(file, OdilonServerInfo.class);
		} catch (IOException e) {
			throw new InternalCriticalException(e);
		} finally {
				getLockService().getServerLock().readLock().unlock();
		}
	}
	

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public void setServerInfo(OdilonServerInfo serverInfo) {
		Check.requireNonNullArgument(serverInfo, "serverInfo is null");
		if (getServerInfo()==null)
			saveNewServerInfo(serverInfo);
		else
			updateServerInfo(serverInfo);
	}
	
	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public byte[] getServerMasterKey() {

		getLockService().getServerLock().readLock().lock();
		
		try {

			File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
			
			if (file == null || !file.exists())
				return null;
			
			byte[] bDataEnc = FileUtils.readFileToByteArray(file);
			byte[] bdataDec = getVFS().getMasterKeyEncryptorService().decryptKey(bDataEnc);
			
			String encryptionKey = getVFS().getServerSettings().getEncryptionKey();
			
			if (encryptionKey==null)
				throw new InternalCriticalException(" encryptionKey is null");
			
			byte [] b_encryptionKey = ByteToString.hexStringToByte(encryptionKey);
			byte [] b_hmacOriginal;
			
			try {
				b_hmacOriginal = getVFS().HMAC(b_encryptionKey, b_encryptionKey);
				
			} catch (InvalidKeyException | NoSuchAlgorithmException e) {
				throw new InternalCriticalException(e, "can not calculate HMAC for odilon.properties encryption key");
			}
			
			byte[] b_hmacNew = new byte[32];
			System.arraycopy(bdataDec, 0, b_hmacNew, 0, b_hmacNew.length);
			
			if (!Arrays.equals(b_hmacOriginal, b_hmacNew)) {
				throw new InternalCriticalException("HMAC is not correct, HMAC of 'encryption.key' in 'odilon.properties' is not match with HMAC in key.enc  -> encryption.key=" + encryptionKey);
			}
			
			/** HMAC is correct */
			byte[] key = new byte[VirtualFileSystemService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
			System.arraycopy(bdataDec, b_hmacNew.length, key, 0,  key.length);
			return key;

		} catch (InternalCriticalException e) {
			if ((e.getCause()!=null) && (e.getCause() instanceof javax.crypto.BadPaddingException)) {
				
				logger.error("");
				logger.error("-----------------------------------");
				logger.error("possible cause -> the value of 'encryption.key' in 'odilon.properties' is incorrect");
				logger.error("-----------------------------------");
				logger.error("");
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
				}
			}
			throw e;
			
		} catch (IOException e) {
			throw new InternalCriticalException(e);

		} finally {
			getLockService().getServerLock().readLock().unlock();
		}
	}

	
	

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */

	@Override
	public void saveServerMasterKey(byte[] key, byte[] hmac, byte[] salt) {
				
		Check.requireNonNullArgument(key, "key is null");
		Check.requireNonNullArgument(salt, "salt is null");
		
		boolean done = false;
		boolean reqRestoreBackup = false;
		
		VFSOperation op = null;
		
		getLockService().getServerLock().writeLock().lock();
		
		try {
			
				/** backup */
				for (Drive drive : getDrivesAll()) {
					try {
						// drive.putSysFile(ServerConstant.ODILON_SERVER_METADATA_FILE, jsonString);
						// backup
					} catch (Exception e) {
						//isError = true;
						reqRestoreBackup = false;
						throw new InternalCriticalException(e, "Drive -> " + drive.getName());
					}
				}
				
				op = getJournalService().saveServerKey();
				
				reqRestoreBackup = true;
				
				Exception eThrow = null;

				byte[] data = new byte[hmac.length + key.length + salt.length];
				

				// HMAC(32) + Master Key (16) + Salt (64)
				
				System.arraycopy(hmac, 0, data, 0        					, hmac.length);
				System.arraycopy(key,  0, data, hmac.length 				, key.length);
				System.arraycopy(salt, 0, data, (hmac.length+key.length)	, salt.length);
				
				// logger.debug("hmac -> " + ByteToString.byteToHexString(hmac));
				// logger.debug("key -> " + ByteToString.byteToHexString(key));
				
				byte[] dataEnc = getVFS().getMasterKeyEncryptorService().encryptKey(data);

				/** save */
				for (Drive drive: getDrivesAll()) {
					try {
						File file = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
						FileUtils.writeByteArrayToFile(file, dataEnc);
						
					} catch (Exception e) {
						eThrow = new InternalCriticalException(e, "Drive -> " + drive.getName());
						break;
					}
				}
				
				if (eThrow!=null)
					throw eThrow;
				
				done = op.commit();
				
		} catch (InternalCriticalException e) {
			throw e;
			
		} catch (Exception e) {
			throw new InternalCriticalException(e);
			
		} finally {
			
			try {
				
				if (!done) {
					
					if (!reqRestoreBackup) 
						op.cancel();
					else	
						rollbackJournal(op);
				}
				
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				getLockService().getServerLock().writeLock().unlock();
			}
		}
	}

	

	/**
	 * 
	 * 
	 */
	@Override
	public synchronized List<Drive> getDrivesEnabled() {
		 
			if (this.drivesEnabled!=null)
				return this.drivesEnabled;
			
			this.drivesEnabled = new ArrayList<Drive>();
			
			getVFS().getMapDrivesEnabled().forEach( (K,V) -> this.drivesEnabled.add(V));
			
			this.drivesEnabled.sort(new Comparator<Drive>() {
					@Override
					public int compare(Drive o1, Drive o2) {
							try {
								
								if ((o1.getDriveInfo()==null))
									if (o2.getDriveInfo()!=null) return 1;
								
								if ((o2.getDriveInfo()==null))
									if (o1.getDriveInfo()!=null) return -1;

								if ((o1.getDriveInfo()==null) && o2.getDriveInfo()==null)
									return 0;
									
								if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
									return -1;
								
								if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
									return 1;
								
								return 0;
							}
								catch (Exception e) {
									return 0;		
							}
						}
				});
				
			return this.drivesEnabled;
	}

	/**
	 * 
	 */
	public synchronized List<Drive> getDrivesAll() {
		 
			if (drivesAll!=null)
				return drivesAll;	
			
			
			this.drivesAll = new ArrayList<Drive>();
			getVFS().getMapDrivesAll().forEach( (K,V) -> drivesAll.add(V));
			
			this.drivesAll.sort(new Comparator<Drive>() {
				@Override
				public int compare(Drive o1, Drive o2) {
					try {
							if ((o1.getDriveInfo()==null))
								if (o2.getDriveInfo()!=null) return 1;
							
							if ((o2.getDriveInfo()==null))
								if (o1.getDriveInfo()!=null) return -1;
							
							if ((o1.getDriveInfo()==null) && o2.getDriveInfo()==null)
								return 0;
							
							if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
								return -1;
							
							if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
								return 1;
								
							return 0;
						}
							catch (Exception e) {
								return 0;		
						}
					}
			});

			return this.drivesAll;
	}

	/**
	 * 
	 * @return
	 */
	public boolean isEncrypt() {
		return getVFS().isEncrypt();
	}

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public void saveScheduler(ServiceRequest request, String queueId) {
		for (Drive drive: getDrivesEnabled()) {
			drive.saveScheduler(request, queueId);
		}
	}
	
	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public void removeScheduler(ServiceRequest request, String queueId) {
		for (Drive drive: getDrivesEnabled())
			drive.removeScheduler(request, queueId);
	}
	
	
	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public void saveJournal(VFSOperation op) {
		for (Drive drive: getDrivesEnabled())
			drive.saveJournal(op);
	}

	/**
	 * <p>Shared by RAID 1 and RAID 6</p>
	 */
	@Override
	public void removeJournal(String id) {
		for (Drive drive: getDrivesEnabled())
			drive.removeJournal(id);
	}

													
	protected abstract Drive getObjectMetadataReadDrive(ODBucket bucket, String objectName);
	

	
	/**
	 *<p> Note that bucketName is not stored on disk, we must set the bucketName explicitly. Disks identify Buckets by id, the name is stored in the
	 *BucketMetadata file</p> 
	 */
	public ObjectMetadata getObjectMetadataInternal(ODBucket bucket, String objectName, boolean addToCacheIfmiss) {

		if ((!getVFS().getServerSettings().isUseObjectCache()) || (getVFS().getObjectMetadataCacheService().size() >= MAX_CACHE_SIZE)) 
			return  getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket.getId(), objectName);

		
		if (getVFS().getObjectMetadataCacheService().containsKey(bucket.getId(), objectName)) {
			getVFS().getSystemMonitorService().getCacheObjectHitCounter().inc();
			
			ObjectMetadata meta = getVFS().getObjectMetadataCacheService().get(bucket.getId(), objectName);
			meta.setBucketName(bucket.getName());
			return meta;
		}

		ObjectMetadata meta =  getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket.getId(), objectName);
		meta.setBucketName(bucket.getName());
		
		getVFS().getSystemMonitorService().getCacheObjectMissCounter().inc();

		if (addToCacheIfmiss) {
			getVFS().getObjectMetadataCacheService().put(bucket.getId(), objectName, meta);
		}
		return meta;
	}
	
	/**
	 * 
	 */
	protected boolean existsBucketInDrives(Long bucketId) {

		for (Drive drive : getDrivesEnabled()) {
			if (!drive.existsBucket(bucketId)) {
				logger.error(("b: " + (Optional.of(bucketId).isPresent() ? bucketId.toString() : "null")) + " -> not in d:" + drive.getName());
				return false;
			}
		}
		return true;
	}
	
	/**
	 * <p>all drives have all buckets</p>
	 */
	protected Map<String, ODBucket> getBucketsMap() {
		
		Map<String, ODBucket> map = new HashMap<String, ODBucket>();
		Map<String, Integer> control = new HashMap<String, Integer>();
		
		int totalDrives = getDrivesEnabled().size();
		
		for (Drive drive: getDrivesEnabled()) {
			for (DriveBucket bucket: drive.getBuckets()) {
				if (bucket.getStatus().isAccesible()) {
					String name = bucket.getName();
					Integer count;
					if (control.containsKey(name))
						count = control.get(name)+1; 
					else
						count = Integer.valueOf(1);
					control.put(name, count);
				}
			}
		}

		/** any drive is ok because all have all the buckets 
		 * */
		
		Drive drive=getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random()*1000)).intValue() % getDrivesEnabled().size());
		for (DriveBucket bucket: drive.getBuckets()) {
			String name = bucket.getName();
			if (control.containsKey(name)) {
				Integer count = control.get(name);
				if (count==totalDrives) {
					ODBucket vfsbucket = new ODVFSBucket(bucket);
					map.put(vfsbucket.getName(), vfsbucket);
				}
			}
		}
		return map;
	}


	/**
	 * @param serverInfo
	 */
	private void saveNewServerInfo(OdilonServerInfo serverInfo) {
		
		boolean done = false;
		VFSOperation op = null;
		
		try {
			getLockService().getServerLock().writeLock().lock();
			op = getJournalService().createServerMetadata();
			String jsonString = getObjectMapper().writeValueAsString(serverInfo);
			
			for (Drive drive: getDrivesAll()) {
				try {
					drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
				} catch (Exception e) {
					done=false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			done = op.commit();
			
		} catch (Exception e) {
			throw new InternalCriticalException(e, serverInfo.toString());
			
		} finally {						
			
			try {
				if (!done) {
					rollbackJournal(op);
				}
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
			finally {
				getLockService().getServerLock().writeLock().unlock();	
			}
		}
	}

	
	private void updateServerInfo(OdilonServerInfo serverInfo) {
		
		boolean done = false;
		boolean mayReqRestoreBackup = false;
		VFSOperation op = null;
		
		getLockService().getServerLock().writeLock().lock();
		
		try {

			op = getJournalService().updateServerMetadata();
			String jsonString = getObjectMapper().writeValueAsString(serverInfo);
			
			for (Drive drive: getDrivesAll()) {
				try {
					// drive.putSysFile(ServerConstant.ODILON_SERVER_METADATA_FILE, jsonString);
					// backup
				} catch (Exception e) {
					done=false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			
			mayReqRestoreBackup = true;
			
			for (Drive drive: getDrivesAll()) {
				try {
					drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
				} catch (Exception e) {
					done=false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			done = op.commit();
			
		} catch (Exception e) {
			throw new InternalCriticalException(e, serverInfo.toString());
			
		} finally {						
			try {
				if (!mayReqRestoreBackup) {
					op.cancel();
				}
				else if (!done) {
					rollbackJournal(op);
				}
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
			finally {
				getLockService().getServerLock().writeLock().unlock();	
			}
		}
	}
	
		
}
