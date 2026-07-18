/*
 * (c) kbee 
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
package io.odilon.virtualFileSystem;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
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
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.OdilonVersion;
import io.odilon.cache.ObjectMetadataCacheService;
import io.odilon.encryption.EncryptionService;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.error.OdilonServerAPIException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.json.OdilonObjectMapper;
import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.SharedConstant;
import io.odilon.model.VersionControl;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.query.BucketIteratorService;
import io.odilon.replication.ReplicationService;

import io.odilon.scheduler.SchedulerService;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.service.ServerSettings;
import io.odilon.service.util.ByteToString;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveBucket;
import javax.annotation.concurrent.GuardedBy;

import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;
import tools.jackson.databind.ObjectMapper;

/**
 * <p>
 * Base class for VirtualFileSystemDrivers ({@link IODriver}): <br/>
 * RAID 0: {@link RAIDZeroDriver}, <br/>
 * RAID 1: {@link RAIDOneDriver}, <br/>
 * ErasureCoding: {@link ECDriver} <br/>
 * </p>
 *
 * @see {@link RAIDZeroDriver} {@link RAIDOneDriver} {@link ECDriver}
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class BaseIODriver implements IODriver, ApplicationContextAware {

	private static Logger logger = Logger.getLogger(BaseIODriver.class.getName());
	static private Logger std_logger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	static final public int MAX_CACHE_SIZE = 4000000;

	@JsonIgnore
	static private OdilonObjectMapper mapper = new OdilonObjectMapper();

	@JsonIgnore
	private VirtualFileSystemService virtualFileSystem;

	@JsonIgnore
	private LockService lockService;

	@JsonIgnore
	private List<Drive> drivesEnabled;

	@JsonIgnore
	private List<Drive> drivesAll;

	@JsonIgnore
	private ApplicationContext applicationContext;

	public BaseIODriver(VirtualFileSystemService virtualFileSystemService, LockService lockService) {
		this.virtualFileSystem = virtualFileSystemService;
		this.lockService = lockService;
	}

	/**
	 * Rolls back a journaled {@link VirtualFileSystemOperation}.
	 *
	 * <p>
	 * <b>Locking contract:</b> this method does <em>not</em> acquire any lock
	 * internally. Callers are responsible for holding the appropriate
	 * {@link LockService} locks before invoking it — except during single-threaded
	 * server startup, where journal recovery runs before any other thread is active
	 * and no lock is required.
	 *
	 * <ul>
	 * <li>Object operations (CREATE/UPDATE/DELETE/RESTORE): caller must hold the
	 * object write-lock for the affected {@code (bucketId, objectName)} pair.</li>
	 * <li>Bucket operations (CREATE_BUCKET/DELETE_BUCKET/UPDATE_BUCKET): caller
	 * must hold the bucket write-lock for the affected bucket.</li>
	 * <li>Startup journal recovery: no lock required — single-threaded
	 * context.</li>
	 * </ul>
	 *
	 * <p>
	 * This method is intentionally excluded from the {@code @ThreadSafe} guarantee
	 * of the concrete subclasses. It is annotated {@code @GuardedBy} to make the
	 * precondition visible to static-analysis tools and code reviewers.
	 * </p>
	 */
	@GuardedBy("LockService.objectWriteLock | LockService.bucketWriteLock | startup")
	public abstract void rollback(VirtualFileSystemOperation operation, Object payload, boolean recoveryMode);

	public abstract RedundancyLevel getRedundancyLevel();

	public abstract List<VirtualFileSystemOperation> getJournalPending();

	@Override
	public boolean existsBucket(String bucketName) {
		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");

		bucketReadLock(bucketName);
		try {
			/** Bucket cache must be used after locking critical resource */
			return existsCacheBucket(bucketName);
		} finally {
			bucketReadUnLock(bucketName);
		}
	}

	@Override
	public ServerBucket createBucket(String bucketName) {

		Check.requireNonNullStringArgument(bucketName, "bucketName can not be null or empty");

		if (!bucketName.matches(SharedConstant.bucket_valid_regex))
			throw new IllegalArgumentException("bucketName contains invalid character | regular expression is -> " + SharedConstant.bucket_valid_regex + " |  b:" + bucketName);

		BucketMetadata bucketMeta = new BucketMetadata(bucketName);
		bucketMeta.setStatus(BucketStatus.ENABLED);
		bucketMeta.setAppVersion(OdilonVersion.VERSION);
		bucketMeta.setId(getVirtualFileSystemService().getNextBucketId());

		ServerBucket bucket = new OdilonBucket(bucketMeta);
		boolean commitOK = false;
		boolean isMainException = false;

		VirtualFileSystemOperation operation = null;

		bucketWriteLock(bucketName);
		try {

			/** must be executed inside the critical zone. */
			checkNotExistsBucket(bucket);

			operation = getJournalService().createBucket(bucketMeta);
			OffsetDateTime now = OffsetDateTime.now();
			bucketMeta.setCreationDate(now);
			bucketMeta.setLastModified(now);

			for (Drive drive : getDrivesAll()) {
				try {
					drive.createBucket(bucketMeta);
				} catch (Exception e) {
					commitOK = false;
					isMainException = true;
					logger.error(e, SharedConstant.THROWN_WRAPPED);
					throw new InternalCriticalException(e, objectInfo(drive));
				}
			}
			commitOK = operation.commit(bucket);
			return bucket;

		} finally {
			try {
				if (!commitOK)
					rollback(operation, bucket);
			} catch (Exception e) {
				if (!isMainException) {
					if (e instanceof InternalCriticalException)
						throw e;
					throw new InternalCriticalException(e, objectInfo(bucketMeta));
				} else
					logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				bucketWriteUnLock(bucketName);
			}
		}
	}

	@Override
	public ServerBucket getBucket(String bucketName) {
		Check.requireNonNullArgument(bucketName, "bucket is null");
		bucketReadLock(bucketName);
		try {
			checkExistsBucket(bucketName);
			return getCacheBucket(bucketName);
		} finally {
			bucketReadUnLock(bucketName);
		}
	}

	@Override
	public ServerBucket renameBucket(ServerBucket bucket, String newBucketName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		VirtualFileSystemOperation operation = null;
		boolean commitOK = false;
		BucketMetadata bucketMeta = null;
		OffsetDateTime now = OffsetDateTime.now();
		String oldName = bucket.getName();

		bucketWriteLock(oldName);
		try {
			bucketWriteLock(newBucketName);
			try {

				/** must be executed inside the critical zone. */
				checkExistsBucket(bucket);

				/** must be executed also inside the critical zone. */
				checkNotExistsBucket(newBucketName);

				operation = getJournalService().updateBucket(bucket, newBucketName);
				backupBucketMetadata(bucket);

				bucketMeta = bucket.getBucketMetadata();
				bucketMeta.setLastModified(now);
				bucketMeta.setBucketName(newBucketName);

				for (Drive drive : getDrivesAll()) {
					try {
						drive.updateBucket(bucketMeta);

					} catch (IOException e) {
						commitOK = false;
						logger.error(e, SharedConstant.THROWN_WRAPPED);
						throw new InternalCriticalException(e, objectInfo(drive));
					} catch (Exception e) {
						commitOK = false;
						logger.error(e, SharedConstant.THROWN_WRAPPED);
						throw new InternalCriticalException(e, objectInfo(drive));
					}
				}

				commitOK = operation.commit(bucket);
				return bucket;

			} finally {
				try {
					if (!commitOK)
						rollback(operation);
				} finally {
					bucketWriteUnLock(newBucketName);
				}
			}
		} finally {
			bucketWriteUnLock(oldName);
		}
	}

	@Override
	public void removeBucket(ServerBucket bucket) {
		removeBucket(bucket, false);
	}

	@Override
	public void removeBucket(ServerBucket bucket, boolean forceDelete) {
		Check.requireNonNullArgument(bucket, "bucket can not be null");
		boolean commitOK = false;
		VirtualFileSystemOperation operation = null;

		bucketWriteLock(bucket);
		try {

			try {
				/** must be executed inside the critical zone. */
				checkExistsBucket(bucket);

				if ((!forceDelete) && (!isEmpty(bucket))) {
					throw new OdilonServerAPIException("bucket must be empty to be deleted -> b:" + bucket.getName());
				}

				operation = getJournalService().deleteBucket(bucket);

				for (Drive drive : getDrivesAll()) {
					try {
						drive.markAsDeletedBucket(bucket);
					} catch (Exception e) {
						commitOK = false;
						logger.error(e, SharedConstant.THROWN_WRAPPED);
						throw new InternalCriticalException(e);
					}
				}

				/**
				 * once the bucket is marked as DELETED on all drives and the TRX commited -> it
				 * is gone, it can not be restored
				 */
				commitOK = operation.commit(bucket);

			} finally {

				if (commitOK) {
					/**
					 * once the TRX is completed buckets -> all marked as "deleted" or all marked as
					 * "enabled". TBA this step can be Async
					 */
					if (bucket != null) {
						for (Drive drive : getDrivesAll()) {
							((OdilonDrive) drive).forceDeleteBucketById(bucket.getId());
						}
					}
				} else {
					/** rollback restores all buckets */
					rollback(operation);
				}
			}

		} finally {
			bucketWriteUnLock(bucket);
		}
	}

	@Override
	public boolean isEmpty(ServerBucket bucket) {

		Check.requireNonNullArgument(bucket, ServerBucket.class.getSimpleName() + " is null");

		bucketReadLock(bucket);
		try {

			/** must be executed inside the critical zone. */
			checkExistsBucket(bucket);

			for (Drive drive : getDrivesEnabled()) {
				if (!drive.isEmpty(bucket))
					return false;
			}
			return true;
		} catch (Exception e) {
			if (e instanceof InternalCriticalException)
				throw e;

			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new InternalCriticalException(e, objectInfo(bucket));

		} finally {
			bucketReadUnLock(bucket);
		}
	}

	/**
	 * Handles bucket-level rollback operations (CREATE_BUCKET, DELETE_BUCKET,
	 * UPDATE_BUCKET, UPDATE_SERVER_METADATA) that are common to all RAID drivers.
	 *
	 * <p>
	 * <b>Locking contract:</b> during normal operation the caller must hold the
	 * appropriate {@link LockService} bucket write-lock. During single-threaded
	 * server-startup journal recovery no lock is required.
	 * </p>
	 *
	 * @see #rollback(VirtualFileSystemOperation, Object, boolean)
	 */
	@GuardedBy("LockService.bucketWriteLock | startup")
	protected boolean generalRollbackJournal(VirtualFileSystemOperation operation) {

		Long bucketId = operation.getBucketId();

		try {

			if (operation.getOperationCode() == OperationCode.CREATE_BUCKET) {
				removeCacheBucket(bucketId);

				for (Drive drive : getDrivesAll()) {
					((OdilonDrive) drive).forceDeleteBucketById(bucketId);
				}
				return true;

			} else if (operation.getOperationCode() == OperationCode.DELETE_BUCKET) {
				BucketMetadata meta = null;
				for (Drive drive : getDrivesAll()) {
					drive.markAsEnabledBucket(getCacheBucket(bucketId));
					if (meta == null) {
						meta = drive.getBucketMetadataById(bucketId);
						break;
					}
				}
				if (meta != null) {
					ServerBucket bucket = new OdilonBucket(meta);
					addCacheBucket(bucket);
				}
				return true;

			} else if (operation.getOperationCode() == OperationCode.UPDATE_BUCKET) {
				restoreBucketMetadata(getCacheBucket(bucketId));
				BucketMetadata meta = null;
				for (Drive drive : getDrivesAll()) {
					if (meta == null) {
						meta = drive.getBucketMetadataById(bucketId);
						break;
					}
				}
				if (meta != null) {
					// ── Remove the stale new-name entry from the cache ──────────────────────────
					// operation.getObjectName() holds the new bucket name recorded at journal
					// creation time (see OdilonJournalService.updateBucket).
					// After a crash mid-rename, loadBuckets() may have loaded the bucket under
					// the new name (from drives already updated before the crash). If we only
					// call addCacheBucket(old), the new-name entry persists in nameMap, making
					// checkNotExistsBucket(newName) report a false positive forever.
					String newBucketName = operation.getObjectName();
					if (newBucketName != null)
						getVirtualFileSystemService().getBucketCache().remove(newBucketName);
					ServerBucket bucket = new OdilonBucket(meta);
					addCacheBucket(bucket);
				}
				return true;
			}
			if (operation.getOperationCode() == OperationCode.CREATE_SERVER_MASTERKEY) {
				for (Drive drive : getDrivesAll()) {
					File bak = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE + ".bak");
					File current = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
					if (bak != null && bak.exists()) {
						/** rekey rollback: restore the previous working key */
						if (current != null)
							FileUtils.copyFile(bak, current);
						FileUtils.deleteQuietly(bak);
					} else {
						/** initial encryption rollback: no previous key — delete the partial write */
						if (current != null && current.exists())
							FileUtils.forceDelete(current);
					}
				}
				return true;
			} else if (operation.getOperationCode() == OperationCode.CREATE_SERVER_METADATA) {
				if (operation.getObjectName() != null) {
					for (Drive drive : getDrivesAll()) {
						drive.removeSysFile(operation.getObjectName());
					}
				}
				return true;
			} else if (operation.getOperationCode() == OperationCode.UPDATE_SERVER_METADATA) {
				// Restore odilon.json from the .backup copy that setServerInfo() writes before
				// overwriting the file. If no backup exists the original is intact (crash
				// between journal write and backup write) so no action is needed.
				for (Drive drive : getDrivesAll()) {
					File backup = drive.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE + ".backup");
					if (backup != null && backup.exists()) {
						File current = drive.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
						FileUtils.copyFile(backup, current);
						FileUtils.deleteQuietly(backup);
					} else {
						logger.warn("UPDATE_SERVER_METADATA rollback: no backup found on drive -> " + drive.getName() + " | original file left unchanged");
					}
				}
				return true;
			}

		} catch (InternalCriticalException e) {
			throw (e);

		} catch (IOException e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new InternalCriticalException(e);
		}

		return false;

	}

	/**
	 * <p>
	 * ObjectMetadata is copied to all drives as regular files. Shared by RAID 1 and
	 * RAID 6
	 * </p>
	 */
	@Override
	public ObjectMetadata getObjectMetadataPreviousVersion(ServerBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());

		objectReadLock(bucket, objectName);
		try {

			bucketReadLock(bucket);
			try {
				/** must be executed inside the critical zone. */
				checkExistsBucket(bucket);

				List<ObjectMetadata> list = getObjectMetadataVersionAll(bucket, objectName);

				if (list != null && !list.isEmpty())
					return list.get(list.size() - 1);

				return null;

			} catch (IllegalArgumentException e1) {
				throw e1;
			} catch (Exception e) {
				logger.error(e, SharedConstant.THROWN_WRAPPED);
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));
			} finally {
				bucketReadUnLock(bucket);
			}
		} finally {
			objectReadUnLock(bucket, objectName);
		}

	}

	/**
	 * Save metadata Save stream
	 * 
	 * @param folderName
	 * @param objectName
	 * @param file
	 */
	@Override
	public void putObject(ServerBucket bucket, String objectName, File file) {

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
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new InternalCriticalException(e, objectInfo(bucket, objectName));
		}
		try {

			putObject(bucket, objectName, new BufferedInputStream(new FileInputStream(file)), file.getName(), contentType, Optional.empty());

		} catch (FileNotFoundException e) {
			throw new InternalCriticalException(e, objectInfo(bucket, objectName));
		}
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public abstract List<ServiceRequest> getSchedulerPendingRequests(String queueId);

	@Override
	public OdilonServerInfo getServerInfo() {
		File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
		if (file == null || !file.exists())
			return null;
		try {
			getLockService().getServerLock().readLock().lock();
			return getObjectMapper().readValue(file, OdilonServerInfo.class);
		} catch (Exception e) {
			throw new InternalCriticalException(e);
		} finally {
			getLockService().getServerLock().readLock().unlock();
		}
	}

	@Override
	public void setServerInfo(OdilonServerInfo serverInfo) {
		Check.requireNonNullArgument(serverInfo, "serverInfo is null");
		if (getServerInfo() == null)
			saveNewServerInfo(serverInfo);
		else
			updateServerInfo(serverInfo);
	}

	@Override
	public byte[] getServerMasterKey() {

		getLockService().getServerLock().readLock().lock();
		byte[] bdataDec = null;
		try {

			File file = getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);

			if (file == null || !file.exists())
				return null;

			byte[] bDataEnc = FileUtils.readFileToByteArray(file);

			String encryptionKey = getVirtualFileSystemService().getServerSettings().getEncryptionKey();
			String encryptionIV = getVirtualFileSystemService().getServerSettings().getEncryptionIV();

			if (encryptionKey == null || encryptionIV == null)
				throw new InternalCriticalException(" encryption Key or IV is null");

			byte[] b_encryptionKey = ByteToString.hexStringToByte(encryptionKey);
			byte[] b_encryptionKeyIV = ByteToString.hexStringToByte(encryptionKey + encryptionIV);

			byte[] b_hmacOriginal;

			try {
				b_hmacOriginal = getVirtualFileSystemService().HMAC(b_encryptionKeyIV, b_encryptionKey);
			} catch (InvalidKeyException | NoSuchAlgorithmException e) {
				throw new InternalCriticalException(e, "can not calculate HMAC for 'odilon.properties' encryption key");
			}

			/** HMAC(32) + Master Key (16) + IV(12) + Salt (64) */
			bdataDec = getVirtualFileSystemService().getMasterKeyEncryptorService().decryptKey(bDataEnc);

			byte[] b_hmacNew = new byte[EncryptionService.HMAC_SIZE];
			System.arraycopy(bdataDec, 0, b_hmacNew, 0, b_hmacNew.length);

			if (!Arrays.equals(b_hmacOriginal, b_hmacNew)) {
				logger.error("HMAC of 'encryption.key' in 'odilon.properties' does not match with HMAC in 'key.enc'  -> encryption.key=" + encryptionKey + encryptionIV);

				std_logger.error("HMAC of 'encryption.key' in 'odilon.properties' does not match with HMAC in 'key.enc'  -> encryption.key=" + encryptionKey + encryptionIV);

				throw new InternalCriticalException("HMAC of 'encryption.key' in 'odilon.properties' does not match with HMAC in 'key.enc'  -> encryption.key=" + encryptionKey + encryptionIV);
			}

			/** HMAC is correct */
			// Derive the master key size from the stored buffer rather than the constant so
			// that existing 128-bit (16-byte) and new 256-bit (32-byte) master keys are
			// both handled transparently.
			// Buffer layout: HMAC(32) + MasterKey(N) + IV(12) + Salt(64)
			int ivSizeBytes = EncryptionService.AES_IV_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE;
			int saltSizeBytes = EncryptionService.AES_KEY_SALT_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE;
			int masterKeySizeBytes = bdataDec.length - EncryptionService.HMAC_SIZE - ivSizeBytes - saltSizeBytes;
			byte[] key = new byte[masterKeySizeBytes];
			System.arraycopy(bdataDec, b_hmacNew.length, key, 0, key.length);

			return key;

		} catch (InternalCriticalException e) {
			if ((e.getCause() != null) && (e.getCause() instanceof javax.crypto.BadPaddingException)) {
				logger.error("possible cause -> the value of 'encryption.key' in 'odilon.properties' is incorrect");
				std_logger.error("possible cause -> the value of 'encryption.key' in 'odilon.properties' is incorrect");
			}
			throw e;

		} catch (IOException e) {
			std_logger.error(e, "getServerMasterKey");
			throw new InternalCriticalException(e, "getServerMasterKey");

		} finally {
			/**
			 * Zero the decrypted buffer so HMAC+masterKey+IV+salt do not linger in the heap
			 */
			if (bdataDec != null)
				Arrays.fill(bdataDec, (byte) 0);
			getLockService().getServerLock().readLock().unlock();
		}
	}

	/**
	 * <p>
	 * Shared by RAID 1 and ErasureCoding
	 * </p>
	 */

	@Override
	public void saveServerMasterKey(byte[] key, byte[] hmac, byte[] iv, byte[] salt) {

		Check.requireNonNullArgument(key, "key is null");
		Check.requireNonNullArgument(salt, "salt is null");

		boolean done = false;
		boolean reqRestoreBackup = false;

		VirtualFileSystemOperation op = null;

		getLockService().getServerLock().writeLock().lock();
		try {
			/**
			 * Backup existing key.enc → key.enc.bak on every drive BEFORE writing. Required
			 * for rekey rollback: without this, a failed rekey would delete key.enc (the
			 * only copy of the working key) making all stored data permanently
			 * inaccessible. For initial encryption key.enc does not exist yet, so nothing
			 * is backed up and rollback will simply delete the partially-written file.
			 */
			for (Drive drive : getDrivesAll()) {
				try {
					File current = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
					if (current != null && current.exists()) {
						File bak = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE + ".bak");
						FileUtils.copyFile(current, bak);
					}
				} catch (Exception e) {
					reqRestoreBackup = false;
					throw new InternalCriticalException(e, "backup key.enc on drive -> " + drive.getName());
				}
			}

			op = getJournalService().saveServerKey();

			reqRestoreBackup = true;

			Exception eThrow = null;

			byte[] data = new byte[hmac.length + iv.length + key.length + salt.length];

			/** HMAC(32) + Master Key (16) + IV(12) + Salt (64) */
			System.arraycopy(hmac, 0, data, 0, hmac.length);
			System.arraycopy(key, 0, data, hmac.length, key.length);
			System.arraycopy(iv, 0, data, (hmac.length + key.length), iv.length);
			System.arraycopy(salt, 0, data, (hmac.length + iv.length + key.length), salt.length);

			byte[] dataEnc = getVirtualFileSystemService().getMasterKeyEncryptorService().encryptKey(data, iv);

			/** save */
			for (Drive drive : getDrivesAll()) {
				try {
					File file = drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
					FileUtils.writeByteArrayToFile(file, dataEnc);
					setOwnerReadWriteOnly(file.toPath());

				} catch (Exception e) {
					eThrow = new InternalCriticalException(e, objectInfo(drive));
					break;
				}
			}

			if (eThrow != null)
				throw eThrow;

			done = op.commit();

		} catch (InternalCriticalException e) {
			throw e;

		} catch (Exception e) {
			if (logger.isDebugEnabled())
				logger.error(e, SharedConstant.NOT_THROWN);
			throw new InternalCriticalException(e, "saveServerMasterKey");

		} finally {
			try {
				if (done) {
					/** clean up .bak files after successful commit */
					for (Drive drive : getDrivesAll()) {
						FileUtils.deleteQuietly(drive.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE + ".bak"));
					}
				} else {
					if (!reqRestoreBackup)
						op.cancel();
					else
						rollback(op);
				}
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				getLockService().getServerLock().writeLock().unlock();
			}
		}
	}

	@Override
	public synchronized List<Drive> getDrivesEnabled() {

		if (this.drivesEnabled != null)
			return this.drivesEnabled;

		this.drivesEnabled = new ArrayList<Drive>();

		getVirtualFileSystemService().getMapDrivesEnabled().forEach((K, V) -> this.drivesEnabled.add(V));

		this.drivesEnabled.sort(new Comparator<Drive>() {
			@Override
			public int compare(Drive o1, Drive o2) {
				try {

					if ((o1.getDriveInfo() == null))
						if (o2.getDriveInfo() != null)
							return 1;

					if ((o2.getDriveInfo() == null))
						if (o1.getDriveInfo() != null)
							return -1;

					if ((o1.getDriveInfo() == null) && o2.getDriveInfo() == null)
						return 0;

					if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
						return -1;

					if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
						return 1;

					return 0;
				} catch (Exception e) {
					return 0;
				}
			}
		});
		return this.drivesEnabled;
	}

	public synchronized List<Drive> getDrivesAll() {

		if (drivesAll != null)
			return drivesAll;

		this.drivesAll = new ArrayList<Drive>();

		getVirtualFileSystemService().getMapDrivesAll().forEach((K, V) -> drivesAll.add(V));

		this.drivesAll.sort(new Comparator<Drive>() {
			@Override
			public int compare(Drive o1, Drive o2) {
				try {
					if ((o1.getDriveInfo() == null))
						if (o2.getDriveInfo() != null)
							return 1;

					if ((o2.getDriveInfo() == null))
						if (o1.getDriveInfo() != null)
							return -1;

					if ((o1.getDriveInfo() == null) && o2.getDriveInfo() == null)
						return 0;

					if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
						return -1;

					if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
						return 1;

					return 0;
				} catch (Exception e) {
					return 0;
				}
			}
		});

		return this.drivesAll;
	}

	public abstract void saveScheduler(ServiceRequest request, String queueId);

	public abstract void removeScheduler(ServiceRequest request, String queueId);

	public abstract void saveJournal(VirtualFileSystemOperation op);

	public abstract void removeJournal(String id);

	public boolean isEncrypt() {
		return getVirtualFileSystemService().isEncrypt();
	}

	public boolean isStandByEnabled() {
		return getVirtualFileSystemService().getServerSettings().isStandByEnabled();
	}

	public void rollback(VirtualFileSystemOperation operation) {

		if (operation == null)
			return;

		if (isStandByEnabled())
			getReplicationService().cancel(operation);

		rollback(operation, null, false);
	}

	public void rollback(VirtualFileSystemOperation operation, boolean recoveryMode) {

		if (operation == null)
			return;

		if (isStandByEnabled())
			getReplicationService().cancel(operation);

		rollback(operation, null, recoveryMode);
	}

	public void rollback(VirtualFileSystemOperation operation, Object payload) {

		if (operation == null)
			return;

		if (isStandByEnabled())
			getReplicationService().cancel(operation);

		rollback(operation, payload, false);
	}

	public ObjectMapper getObjectMapper() {
		return mapper;
	}

	public LockService getLockService() {
		return this.lockService;
	}

	public JournalService getJournalService() {
		return getVirtualFileSystemService().getJournalService();
	}

	public SchedulerService getSchedulerService() {
		return getVirtualFileSystemService().getSchedulerService();
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		return virtualFileSystem;
	}

	public void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
		this.virtualFileSystem = virtualFileSystemService;
	}

	public String opInfo(VirtualFileSystemOperation op) {
		return "op:" + (op != null ? op.toString() : "null");
	}

	public String objectInfo(BucketMetadata bucket) {
		if (bucket == null)
			return "b: null";
		return "b_id:" + (bucket.getId() != null ? bucket.getId().toString() : "null") + " bn:" + (bucket.getBucketName() != null ? bucket.getBucketName() : "null");
	}

	public String objectInfo(Drive drive) {
		if (drive == null)
			return "d: null";
		return "d:" + drive.getName();
	}

	public String objectInfo(ServerBucket bucket) {
		if (bucket == null)
			return "b: null";
		return "b_id:" + (bucket.getId() != null ? bucket.getId().toString() : "null") + " bn:" + (bucket.getName() != null ? bucket.getName() : "null");
	}

	public String objectInfo(String bucketName) {
		return "bn:" + (bucketName != null ? bucketName : "null");
	}

	public String objectInfo(String bucketName, String objectName) {
		return "bn:" + (bucketName != null ? bucketName : "null") + " o:" + (objectName != null ? objectName : "null");
	}

	public String objectInfo(ServerBucket bucket, String objectName, int version) {
		if (bucket == null)
			return objectInfo("null", objectName);
		return objectInfo(bucket.getName(), objectName, version);
	}

	public String objectInfo(String bucketName, String objectName, int version) {
		return "bn:" + (bucketName != null ? bucketName : "null") + " o:" + (objectName != null ? objectName : "null") + "v:" + String.valueOf(version);
	}

	public String objectInfo(ServerBucket bucket, String objectName, Drive drive) {
		return "bn:" + (bucket != null ? bucket.getName() : "null") + " o:" + (objectName != null ? objectName : "null") + " d:" + (drive != null ? drive.getName() : "null");
	}

	public String objectInfo(ServerBucket bucket, Drive drive) {
		return "bn:" + (bucket != null ? bucket.getName() : "null") + " d:" + (drive != null ? drive.getName() : "null");
	}

	public String objectInfo(ServerBucket bucket, String objectName) {
		if (bucket == null)
			return objectInfo("null", objectName);
		return objectInfo(bucket.getName(), objectName);
	}

	public String objectInfo(ServerBucket bucket, String objectName, String fileName) {
		if (bucket == null)
			return objectInfo("null", objectName, fileName);
		return objectInfo(bucket.getName(), objectName, fileName);
	}

	public String objectInfo(ObjectMetadata meta) {
		if (meta == null)
			return "om: null";

		if (meta.getBucketName() != null)
			return objectInfo(meta.getBucketName(), meta.getObjectName(), meta.getFileName(), meta.length());
		else
			return objectInfo(meta.getBucketId(), meta.getObjectName(), meta.getFileName(), meta.length());
	}

	public String objectInfo(Long bucket_id, String objectName, String fileName, long size) {
		return "b_id:" + (bucket_id != null ? bucket_id.toString() : "null") + " o:" + (objectName != null ? objectName : "null") + (fileName != null ? (" f:" + fileName) : "") + " s:" + String.valueOf(size) + " bytes";
	}

	public String objectInfo(String bucketName, String objectName, String fileName) {
		return "bn:" + (bucketName != null ? bucketName.toString() : "null") + " o:" + (objectName != null ? objectName : "null") + (fileName != null ? (" f:" + fileName) : "");
	}

	public String objectInfo(String bucketName, String objectName, String fileName, long size) {
		return "bn:" + (bucketName != null ? bucketName.toString() : "null") + " o:" + (objectName != null ? objectName : "null") + (fileName != null ? (" f:" + fileName) : "") + " s:" + String.valueOf(size) + " bytes";
	}

	public String fileInfo(File file) {
		if (file == null)
			return "f:null";
		return "f:" + file.getName();
	}

	public int getTotalDisks() {
		return getVirtualFileSystemService().getServerSettings().getTotalDisks();
	}

	public VersionControl getVersionControl() {
		return getVirtualFileSystemService().getVersionControl();
	}

	protected abstract Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName);

	/**
	 * <p>
	 * Note that bucketName is not stored on disk, we must set the bucketName
	 * explicitly. Disks identify Buckets by id, the name is stored in the
	 * BucketMetadata file
	 * </p>
	 * 
	 * MUST BE CALLED INSIDE THE CRITICAL ZONE
	 */
	protected ObjectMetadata getMetadata(ServerBucket bucket, String objectName) {
		return getDriverObjectMetadataInternal(bucket, objectName, true);
	}

	protected ObjectMetadata getDriverObjectMetadataInternal(ServerBucket bucket, String objectName, boolean addToCacheIfmiss) {

		if ((!getServerSettings().isUseObjectCache())) {
			ObjectMetadata meta = getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket, objectName);
			meta.setBucketName(bucket.getName());
			return meta;
		}

		if (getObjectMetadataCacheService().containsKey(bucket, objectName)) {
			ObjectMetadata meta = getObjectMetadataCacheService().get(bucket, objectName);
			meta.setBucketName(bucket.getName());
			return meta;
		}

		ObjectMetadata meta = getObjectMetadataReadDrive(bucket, objectName).getObjectMetadata(bucket, objectName);

		if (meta == null)
			return meta;

		meta.setBucketName(bucket.getName());

		if (addToCacheIfmiss)
			getObjectMetadataCacheService().put(bucket, objectName, meta);

		return meta;
	}

	/**
	 * <p>
	 * all drives have all buckets
	 * </p>
	 */
	protected Map<String, ServerBucket> getBucketsMap() {

		Map<String, ServerBucket> map = new HashMap<String, ServerBucket>();

		// ── Count bucket presence by ID, not by name ───────────────────────────────
		// A crash during renameBucket leaves some drives with the old bucket name and
		// some with the new name. Counting by name would make neither name reach
		// totalDrives, causing the bucket to disappear from the cache and triggering a
		// NullPointerException when the journal rollback tries to restore it.
		// Counting by the stable bucket ID correctly aggregates both name variants.
		Map<Long, Integer> control = new HashMap<Long, Integer>();

		int totalDrives = getDrivesEnabled().size();

		for (Drive drive : getDrivesEnabled()) {
			for (DriveBucket bucket : drive.getBuckets()) {
				if (bucket.getStatus().isAccesible()) {
					Long id = bucket.getId();
					control.put(id, control.getOrDefault(id, 0) + 1);
				}
			}
		}

		/**
		 * any drive is ok because all have all the buckets
		 */
		Drive drive = getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % getDrivesEnabled().size());
		for (DriveBucket bucket : drive.getBuckets()) {
			Long id = bucket.getId();
			if (control.containsKey(id)) {
				Integer count = control.get(id);
				if (count == totalDrives) {
					ServerBucket vfsbucket = new OdilonBucket(bucket);
					map.put(vfsbucket.getName(), vfsbucket);
				}
			}
		}
		return map;
	}

	/**
	 * @param bucket
	 */
	protected void restoreBucketMetadata(ServerBucket bucket) {
		try {
			for (Drive drive : getDrivesAll()) {
				BucketPath b_path = new BucketPath(drive, bucket);
				Path backup = b_path.bucketMetadata(Context.BACKUP);
				// ── Guard: backup file may not exist if the crash occurred during
				// backupBucketMetadata itself (before this drive's backup was written).
				// In that case the drive's metadata file was never overwritten, so it
				// still holds the correct pre-rename state — nothing to restore here.
				if (!backup.toFile().exists()) {
					logger.warn("restoreBucketMetadata: backup file missing on drive -> " + drive.getName() + " b:" + (bucket != null ? bucket.getName() : "null") + " | drive metadata left unchanged (pre-rename state intact)");
					continue;
				}
				drive.updateBucket(getObjectMapper().readValue(backup.toFile(), BucketMetadata.class));
			}
		} catch (Exception e) {
			throw new InternalCriticalException(e, objectInfo(bucket));
		}
	}

	/**
	 * @param bucket
	 */
	protected void backupBucketMetadata(ServerBucket bucket) {
		try {
			for (Drive drive : getDrivesAll()) {
				BucketMetadata meta = drive.getBucketMetadata(bucket);
				BucketPath b_path = new BucketPath(drive, bucket);
				Path backup = b_path.bucketMetadata(Context.BACKUP);
				Files.writeString(backup, getObjectMapper().writeValueAsString(meta));
			}
		} catch (Exception e) {
			throw new InternalCriticalException(e, objectInfo(bucket));
		}
	}

	protected EncryptionService getEncryptionService() {
		return getVirtualFileSystemService().getEncryptionService();
	}

	protected BucketIteratorService getBucketIteratorService() {
		return getVirtualFileSystemService().getBucketIteratorService();
	}

	protected ServerSettings getServerSettings() {
		return getVirtualFileSystemService().getServerSettings();
	}

	protected ReplicationService getReplicationService() {
		return getVirtualFileSystemService().getReplicationService();
	}

	protected SystemMonitorService getSystemMonitorService() {
		return getVirtualFileSystemService().getSystemMonitorService();
	}

	protected void objectReadLock(ServerBucket bucket, String objectName) {
		getLockService().getObjectLock(bucket, objectName).readLock().lock();
	}

	protected void objectReadUnLock(ServerBucket bucket, String objectName) {
		getLockService().getObjectLock(bucket, objectName).readLock().unlock();
	}

	protected void objectWriteLock(ServerBucket bucket, String objectName) {
		getLockService().getObjectLock(bucket, objectName).writeLock().lock();
	}

	protected void objectWriteUnLock(ServerBucket bucket, String objectName) {
		getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
	}

	protected void bucketReadLock(ServerBucket bucket) {
		getLockService().getBucketLock(bucket).readLock().lock();
	}

	protected void bucketReadLock(String bucketName) {
		getLockService().getBucketLock(bucketName).readLock().lock();
	}

	protected void bucketReadUnLock(String bucketName) {
		getLockService().getBucketLock(bucketName).readLock().unlock();
	}

	protected void bucketWriteLock(String bucketName) {
		getLockService().getBucketLock(bucketName).writeLock().lock();
	}

	protected void bucketWriteUnLock(String bucketName) {
		getLockService().getBucketLock(bucketName).writeLock().unlock();
	}

	protected void bucketReadUnLock(ServerBucket bucket) {
		getLockService().getBucketLock(bucket).readLock().unlock();
	}

	protected void bucketWriteLock(ServerBucket bucket) {
		getLockService().getBucketLock(bucket).writeLock().lock();
	}

	protected void bucketWriteUnLock(ServerBucket bucket) {
		getLockService().getBucketLock(bucket).writeLock().unlock();
	}

	protected void bucketWriteLock(BucketMetadata meta) {
		getLockService().getBucketLock(meta.getId()).writeLock().lock();
	}

	protected void bucketWriteUnLock(BucketMetadata meta) {
		getLockService().getBucketLock(meta.getId()).writeLock().unlock();
	}

	/**
	 * must be executed inside the critical zone.
	 */
	protected void checkExistBucket(ServerBucket bucket) {
		if (!existsCacheBucket(bucket))
			throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected void addCacheBucket(ServerBucket bucket) {
		getVirtualFileSystemService().getBucketCache().add(bucket);
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected void updateCacheBucket(String oldName, OdilonBucket odilonBucket) {
		getVirtualFileSystemService().getBucketCache().update(oldName, odilonBucket);
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected void removeCacheBucket(ServerBucket bucket) {
		getVirtualFileSystemService().getBucketCache().remove(bucket.getId());
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected void removeCacheBucket(Long bucketId) {
		getVirtualFileSystemService().getBucketCache().remove(bucketId);
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected ServerBucket getCacheBucket(Long id) {
		return getVirtualFileSystemService().getBucketCache().get(id);
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected ServerBucket getCacheBucket(String bucketName) {
		return getVirtualFileSystemService().getBucketCache().get(bucketName);
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected boolean existsCacheBucket(String bucketName) {
		return getVirtualFileSystemService().getBucketCache().contains(bucketName);
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected boolean existsCacheBucket(Long id) {
		return getVirtualFileSystemService().getBucketCache().contains(id);
	}

	/**
	 * This check must be executed inside the critical section
	 */
	protected boolean existsCacheBucket(ServerBucket bucket) {
		return getVirtualFileSystemService().getBucketCache().contains(bucket);
	}

	protected ObjectMetadataCacheService getObjectMetadataCacheService() {
		return getVirtualFileSystemService().getObjectMetadataCacheService();
	}

	protected void checkIsAccesible(ServerBucket bucket) {
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible (ie. " + BucketStatus.ENABLED.getName() + " or " + BucketStatus.ENABLED.getName() + "  | b:" + bucket.getName());
	}

	/**
	 * must be executed inside the critical zone.
	 */
	protected void checkNotExistsBucket(ServerBucket bucket) {
		if (existsCacheBucket(bucket))
			throw new IllegalArgumentException("bucket already exists -> " + objectInfo(bucket));
	}

	/**
	 * must be executed inside the critical zone.
	 */
	protected void checkNotExistsBucket(String bucketName) {
		if (getVirtualFileSystemService().getBucketCache().contains(bucketName))
			throw new IllegalArgumentException("bucket already exists -> " + bucketName);
	}

	/**
	 * must be executed inside the critical zone.
	 */
	protected void checkExistsBucket(ServerBucket bucket) {
		if (!existsCacheBucket(bucket))
			throw new OdilonObjectNotFoundException("bucket does not exist -> " + objectInfo(bucket));
	}

	/**
	 * must be executed inside the critical zone.
	 */
	protected void checkExistsBucket(String bucketName) {
		if (!getVirtualFileSystemService().getBucketCache().contains(bucketName))
			throw new IllegalArgumentException("bucket does not exist -> " + bucketName);
	}

	/**
	 * @param serverInfo
	 */
	private void saveNewServerInfo(OdilonServerInfo serverInfo) {

		boolean done = false;
		VirtualFileSystemOperation op = null;

		getLockService().getServerLock().writeLock().lock();
		try {
			op = getJournalService().createServerMetadata();
			String jsonString = getObjectMapper().writeValueAsString(serverInfo);

			for (Drive drive : getDrivesAll()) {
				try {
					drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
				} catch (Exception e) {
					done = false;
					throw new InternalCriticalException(e, "Drive -> " + drive.getName());
				}
			}
			done = op.commit();

		} catch (Exception e) {
			throw new InternalCriticalException(e, serverInfo.toString());

		} finally {

			try {
				if (!done) {
					rollback(op);
				}
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				getLockService().getServerLock().writeLock().unlock();
			}
		}
	}

	private void updateServerInfo(OdilonServerInfo serverInfo) {

		boolean done = false;
		boolean mayReqRestoreBackup = false;
		VirtualFileSystemOperation op = null;

		getLockService().getServerLock().writeLock().lock();

		try {
			op = getJournalService().updateServerMetadata();
			String jsonString = getObjectMapper().writeValueAsString(serverInfo);

			/** backup current odilon.json on every drive before overwriting */
			for (Drive drive : getDrivesAll()) {
				try {
					File current = drive.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
					if (current != null && current.exists()) {
						File backup = drive.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE + ".backup");
						FileUtils.copyFile(current, backup);
					}
				} catch (Exception e) {
					done = false;
					throw new InternalCriticalException(e, "backup server metadata | Drive -> " + drive.getName());
				}
			}

			mayReqRestoreBackup = true;

			for (Drive drive : getDrivesAll()) {
				try {
					drive.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
				} catch (Exception e) {
					done = false;
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
				} else if (!done) {
					rollback(op);
				} else {
					/** commit succeeded — clean up the backup files */
					for (Drive drive : getDrivesAll()) {
						FileUtils.deleteQuietly(drive.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE + ".backup"));
					}
				}
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				getLockService().getServerLock().writeLock().unlock();
			}
		}
	}

	/**
	 * <p>
	 * Sets the file permissions to {@code rw-------} (owner read+write only, mode
	 * 600). Only has effect on POSIX file systems (Linux / macOS). Silently skipped
	 * on Windows where POSIX views are unavailable.
	 * </p>
	 */
	protected void setOwnerReadWriteOnly(Path path) {
		try {
			PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
			if (view == null)
				return; // Windows NTFS — skip
			Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
			view.setPermissions(perms);
		} catch (IOException e) {
			// Log but do not abort — a permission warning is better than crashing init
			logger.error(e, "setOwnerReadWriteOnly: could not set permissions on -> " + path.toAbsolutePath());
		}
	}

}
