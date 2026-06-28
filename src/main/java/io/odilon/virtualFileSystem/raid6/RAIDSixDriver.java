/*
 * Odilon Object Storage
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
package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
 
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.query.BucketIteratorService;
import io.odilon.scheduler.AbstractServiceRequest;
import io.odilon.scheduler.DeleteBucketObjectPreviousVersionServiceRequest;
import io.odilon.scheduler.ServiceRequest;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.BaseIODriver;
import io.odilon.virtualFileSystem.OdilonObject;
import io.odilon.virtualFileSystem.model.BucketIterator;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

// Volume-expansion imports
// (same package – no import needed for RAIDSixVolume / OdilonRAIDSixVolumeManager)

/**
 * <p>
 * <b>RAID 6 / Erasure Coding </b> <br/>
 * It is a method of encoding data into blocks that can be distributed across
 * multiple disks or nodes and then reconstructed from a subset of those blocks.
 * </p>
 * <p>
 * It has great flexibility since you can adjust the number and size of the
 * blocks and the minimum required for recovery. It uses less disk space than
 * RAID 1 and can withstand multiple full disk failures.
 * </p>
 * <p>
 * Odilon implements this architecture using Reed Solomon error-correction
 * codes.
 * </p>
 * <p>
 * The configurations are:
 * <ul>
 * <li>3 disks (2 data and 1 parity, supports 1 full disk failure)</li>
 * <li>6 disks (4 data and 2 parity, supports up to 2 full disks failure)</li>
 * <li>12 disks (8 data and 4 parity, supports up to 4 full disk failure)</li>
 * <li>24 disks (16 data and 8 parity, supports up to 8 full disk failure)</li>
 * <li>48 disks (32 data and 16 parity, supports up to 16 full disk
 * failure)</li>
 * </ul>
 * </p>
 * <p>
 * The coding convention for RS blocks is:
 * <ul>
 * <li><b>objectName.[chunk#].[block#]</b></li>
 * <li><b>objectName.[chunk#].[block#].v[version#]</b></li>
 * </ul>
 * where: <br/>
 * <ul>
 * <li><b>chunk#</b><br/>
 * 0..total_chunks, depending of the size of the file to encode
 * ({@link ServerConstant.MAX_CHUNK_SIZE} is 32 MB) this means that for files
 * smaller or equal to 32 MB there will be only one chunk (chunk=0), for files
 * up to 64 MB there will be 2 chunks and so on. <br/>
 * <br/>
 * </li>
 * <li><b>block#</b><br/>
 * is the disk order [0..(data+parity-1)] <br/>
 * <br/>
 * </li>
 * <li><b>version#</b><br/>
 * is omitted for head version. <br/>
 * <br/>
 * </li>
 * </ul>
 * <p>
 * The total number of files once the src file is encoded are: <br/>
 * <br/>
 * (data+parity) * (file_size / MAX_CHUNK_SIZE ) rounded to the following
 * integer. Examples:
 * </p>
 * <p>
 * objectname.block#.disk# <br/>
 * <br/>
 * D:\odilon-data-raid6\drive0\bucket1\TOLOMEI.0.0 <br/>
 * _______________________________________________________________ <br/>
 * <br/>
 * D:\odilon-data-raid6\drive0\bucket1\TOLOMEI.0.0 <br/>
 * D:\odilon-data-raid6\drive1\bucket1\TOLOMEI.1.0 <br/>
 * D:\odilon-data-raid6\drive1\bucket1\TOLOMEI.2.0 <br/>
 * </p>
 * <p>
 * RAID 6. The only configurations supported in v1.x is -><br/>
 * <br/>
 * data shards = 2 + parity shards = 1 -> 3 disks <br/>
 * data shards = 4 + parity shards = 2 -> 6 disks <br/>
 * data shards = 8 + parity shards = 4 -> 12 disks <br/>
 * data shards = 16 + parity shards = 8 -> 24 disks <br/>
 * data shards = 32 + parity shards = 16 -> 48 disks <br/>
 * </p>
 * <p>
 * All buckets <b>must</b> exist on all drives. If a bucket is not present on a
 * drive -> the bucket is considered "non existent".<br/>
 * Each file is stored only on 6 Drives. If a file does not have the file's
 * Metadata Directory -> the file is considered "non existent"
 * </p>
 * <p>
 * This Class is works as a
 * <a href="https://en.wikipedia.org/wiki/Facade_pattern">Facade pattern</a>
 * that uses {@link RAIDSixCreateObjectHandler},
 * {@link RAIDSixDeleteObjectHandler}, {@link RAIDSixUpdateObjectHandler},
 * {@link RAIDSixSyncObjectHandler} and other
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
@Component
@Scope("prototype")
public class RAIDSixDriver extends BaseIODriver implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(RAIDSixDriver.class.getName());

	@JsonIgnore
	private ApplicationContext applicationContext;

	public RAIDSixDriver(VirtualFileSystemService vfs, LockService vfsLockService) {
		super(vfs, vfsLockService);
	}

	@Override
	public void syncObject(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDSixSyncObjectHandler handler = new RAIDSixSyncObjectHandler(this);
		handler.sync(meta);
	}

	@Override
	public InputStream getInputStream(ServerBucket bucket, String objectName) throws IOException {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		objectReadLock(bucket, objectName);
		try {

			bucketReadLock(bucket);
			try {

				checkExistBucket(bucket);
				checkIsAccesible(bucket);

				ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);

				if ((meta != null) && meta.isAccesible()) {
					RAIDSixDecoder decoder = new RAIDSixDecoder(this);
					return (meta.isEncrypt()) ? getVirtualFileSystemService().getEncryptionService().decryptStream(Files.newInputStream(decoder.decodeHead(meta, bucket).toPath()))
							: Files.newInputStream(decoder.decodeHead(meta, bucket).toPath());
				}
				throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));
			} catch (OdilonObjectNotFoundException e) {
				throw e;
			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));
			} finally {
				bucketReadUnLock(bucket);
			}
		} finally {
			objectReadUnLock(bucket, objectName);
		}
	}

	@Override
	public InputStream getObjectVersionInputStream(ServerBucket bucket, String objectName, int version) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		objectReadLock(bucket, objectName);
		try {
			bucketReadLock(bucket);
			try {
				checkExistBucket(bucket);
				checkIsAccesible(bucket);

				// ── Resolve owning volume before reading version metadata ─────────────────────
				// getObjectMetadataReadDrive() for a cache-miss returns a drive from the active
				// volume. If the object lives on an older (READONLY) volume that drive has no
				// metadata → NPE / OdilonObjectNotFoundException. Load head meta first (cross-
				// volume aware) to obtain volumeId, then read the version from the right drive.
				ObjectMetadata headMeta = getDriverObjectMetadataInternal(bucket, objectName, true);
				if (headMeta == null || !headMeta.isAccesible())
					throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

				List<Drive> vDrives = getVolumeForObject(headMeta).getDrives();
				List<Drive> enabledVer = vDrives.stream()
						.filter(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
						.collect(Collectors.toList());
				List<Drive> verPool = enabledVer.isEmpty() ? vDrives : enabledVer;
				Drive readDrive = verPool.get(
						Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % verPool.size());

				ObjectMetadata meta = readDrive.getObjectMetadataVersion(bucket, objectName, version);
				if ((meta == null) || (!meta.isAccesible()))
					throw new OdilonObjectNotFoundException("object version does not exist -> b:" + objectInfo(bucket, objectName) + " | v:" + version);

				RAIDSixDecoder decoder = new RAIDSixDecoder(this);
				File file = decoder.decodeVersion(meta, bucket);

				if (meta.isEncrypt())
					return getVirtualFileSystemService().getEncryptionService().decryptStream(Files.newInputStream(file.toPath()));
				else
					return Files.newInputStream(file.toPath());
			} catch (OdilonObjectNotFoundException e) {
				throw e;
			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucket, objectName) + " | v:" + String.valueOf(version));
			} finally {
				bucketReadUnLock(bucket);
			}
		} finally {
			objectReadUnLock(bucket, objectName);
		}
	}

	/**
	 * <p>
	 * falta completar
	 * </p>
	 */
	@Override
	public boolean checkIntegrity(ServerBucket bucket, String objectName, boolean forceCheck) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null");

		// TODO
		logger.warn("integrity check not completed for RAID 6");

		OffsetDateTime thresholdDate = OffsetDateTime.now().minusDays(getVirtualFileSystemService().getServerSettings().getIntegrityCheckDays());

		Drive readDrive = null;
		ObjectMetadata metadata = null;

		boolean objectLock = false;
		boolean bucketLock = false;

		try {

			try {
				objectLock = getLockService().getObjectLock(bucket, objectName).readLock().tryLock(20, TimeUnit.SECONDS);
				if (!objectLock) {
					logger.error("Can not acquire read Lock for o: " + objectName + ". Assumes -> check is ok");
					return true;
				}
			} catch (InterruptedException e) {
				return true;
			}

			try {
				bucketLock = getLockService().getBucketLock(bucket).readLock().tryLock(20, TimeUnit.SECONDS);
				if (!bucketLock) {
					logger.error("Can not acquire read Lock for b: " + bucket.getName() + ". Assumes -> check is ok");
					return true;
				}
			} catch (InterruptedException e) {
				return true;
			}

			readDrive = getObjectMetadataReadDrive(bucket, objectName);
			// ── Use cross-volume metadata lookup ─────────────────────────────────────────
			// readDrive for a cache-miss may be from the active volume; if the object lives
			// on an older volume that drive has no metadata → null. Use
			// getDriverObjectMetadataInternal which searches all volumes.
			metadata = getDriverObjectMetadataInternal(bucket, objectName, false);

			// --- start of inserted detection-only logic ---

			// If a recent integrityCheck exists (and forceCheck is false) skip
			if (!forceCheck && metadata.integrityCheck != null && metadata.integrityCheck.isAfter(thresholdDate)) {
				logger.debug("Integrity check skipped (recent) -> b:" + bucket.getName() + " o:" + objectName);
				return true;
			}

			// Reconstruct current head into file cache using RAIDSixDecoder
			RAIDSixDecoder decoder = new RAIDSixDecoder(this);
			File decodedFile;
			try {
				decodedFile = decoder.decodeHead(metadata, bucket);
			} catch (Exception e) {
				// Not enough shards or other decode error: log and return false
				// (detection-only)
				logger.error("Integrity check: unable to decode object for integrity verification -> b:" + bucket.getName() + " o:" + objectName + " d:" + (readDrive != null ? readDrive.getName() : "null") + " | " + e.getMessage(),
						SharedConstant.NOT_THROWN);
				return false;
			}

			// Compute SHA-256 of the payload. If the object is encrypted, decodeHead
			// returns the encrypted cache file,
			// so we must decrypt the stream before hashing to compare with a SHA computed
			// from the original payload.
			String computedSha = null;
			try (InputStream rawIn = Files.newInputStream(decodedFile.toPath()); InputStream in = (metadata.isEncrypt() ? getVirtualFileSystemService().getEncryptionService().decryptStream(rawIn) : rawIn)) {

				java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
				byte[] buffer = new byte[8192];
				int read;
				while ((read = in.read(buffer)) != -1) {
					md.update(buffer, 0, read);
				}
				byte[] digest = md.digest();
				// Convert to hex using project's helper
				computedSha = io.odilon.service.util.ByteToString.byteToHexString(digest);

			} catch (Exception e) {
				logger.error(e, "Integrity check: error computing SHA-256 for -> b:" + bucket.getName() + " o:" + objectName);
				return false;
			}

			// Get metadata SHA (may be null)
			String metaSha = null;
			try {
				// metadata.sha256 is used in other code comments; if accessor exists, this will
				// still work.
				// Use field access if available; otherwise try getter as fallback.
				metaSha = (metadata.sha256 != null) ? metadata.sha256 : (metadata.getSha256() != null ? metadata.getSha256() : null);
			} catch (NoSuchMethodError ignored) {
				// Fallback to field only; if metadata.getSha256() doesn't exist, above will use
				// metadata.sha256
				metaSha = metadata.sha256;
			}

			// If metadata has no SHA, log info (no write performed — detection-only)
			if (metaSha == null || metaSha.trim().isEmpty()) {
				logger.info("Integrity check (detection-only): no metadata SHA stored -> b:" + bucket.getName() + " o:" + objectName + " computedSha:" + computedSha);
				return true;
			}

			// Compare (case-insensitive hex)
			if (metaSha.equalsIgnoreCase(computedSha)) {
				logger.debug("Integrity OK -> b:" + bucket.getName() + " o:" + objectName + " d:" + readDrive.getName() + " sha:" + computedSha);
				return true;
			} else {
				// Structured, grep-friendly warning
				logger.warn("RE-ENCODE REQUIRED | bucket=" + bucket.getName() + " | object=" + objectName + " | readDrive=" + (readDrive != null ? readDrive.getName() : "null") + " | metaSha=" + metaSha + " | computedSha=" + computedSha
						+ " | time=" + OffsetDateTime.now().toString());
				return false;
			}

		} finally {

			try {
				if (bucketLock)
					getLockService().getBucketLock(bucket).readLock().unlock();
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}

			try {
				if (objectLock)
					getLockService().getObjectLock(bucket, objectName).readLock().unlock();
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
		}

	}

	/**
	 * <p>
	 * This method is not ThreadSafe. The calling object must ensure concurrency
	 * control.
	 * 
	 * from VFS -> there is only one Thread active from Handler -> objects are
	 * locked before calling this
	 * 
	 */
	@Override
	public void rollback(VirtualFileSystemOperation operation, Object payload, boolean recoveryMode) {

		switch (operation.getOperationCode()) {
		case CREATE_OBJECT: {
			RAIDSixRollbackCreateHandler handler = new RAIDSixRollbackCreateHandler(this, operation, recoveryMode);
			handler.rollback();
			return;
		}
		case UPDATE_OBJECT: {
			RAIDSixRollbackUpdateHandler handler = new RAIDSixRollbackUpdateHandler(this, operation, recoveryMode);
			handler.rollback();
			return;
		}
		case DELETE_OBJECT: {
			RAIDSixRollbackDeleteHandler handler = new RAIDSixRollbackDeleteHandler(this, operation, recoveryMode);
			handler.rollback();
			return;
		}
		case DELETE_OBJECT_PREVIOUS_VERSIONS: {
			RAIDSixRollbackDeleteHandler handler = new RAIDSixRollbackDeleteHandler(this, operation, recoveryMode);
			handler.rollback();
			return;
		}
		case UPDATE_OBJECT_METADATA: {
			RAIDSixRollbackUpdateHandler handler = new RAIDSixRollbackUpdateHandler(this, operation, recoveryMode);
			handler.rollback();
			return;
		}
		case SYNC_OBJECT_NEW_DRIVE: {
			RAIDSixRollbackSyncHandler handler = new RAIDSixRollbackSyncHandler(this, operation, recoveryMode);
			handler.rollback();
			return;
		}
		default:
			break;
		}

		boolean done = false;

		try {

			if (operation.getOperationCode() == OperationCode.CREATE_BUCKET) {
				done = generalRollbackJournal(operation);

			} else if (operation.getOperationCode() == OperationCode.DELETE_BUCKET) {
				done = generalRollbackJournal(operation);

			} else if (operation.getOperationCode() == OperationCode.UPDATE_BUCKET) {
				done = generalRollbackJournal(operation);

			} else if (operation.getOperationCode() == OperationCode.CREATE_SERVER_MASTERKEY) {
				done = generalRollbackJournal(operation);

			} else if (operation.getOperationCode() == OperationCode.CREATE_SERVER_METADATA) {
				done = generalRollbackJournal(operation);

			} else if (operation.getOperationCode() == OperationCode.UPDATE_SERVER_METADATA) {
				done = generalRollbackJournal(operation);
			}

		} catch (InternalCriticalException e) {
			if (!recoveryMode)
				logger.error(opInfo(operation));
			throw (e);

		} catch (Exception e) {
			if (!recoveryMode)
				throw new InternalCriticalException(e, opInfo(operation));
		} finally {
			if (done || recoveryMode) {
				operation.cancel();
			} else {
				if (getVirtualFileSystemService().getServerSettings().isRecovery()) {
					logger.error("---------------------------------------------------------------");
					logger.error("Cancelling failed operation -> " + operation.toString());
					logger.error("---------------------------------------------------------------");
					operation.cancel();
				}
			}
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public ObjectMetadata getObjectMetadata(ServerBucket bucket, String objectName) {
		return getOM(bucket, objectName, Optional.empty(), true);
	}

	/**
	 * <p>
	 * Invariant: all drives contain the same bucket structure
	 * </p>
	 */
	/**
	 * <p>
	 * Volume-aware existence check: searches across all volumes so that an object
	 * stored on any volume is correctly detected.
	 * </p>
	 */
	@Override
	public boolean exists(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		getLockService().getObjectLock(bucket, objectName).readLock().lock();
		try {
			getLockService().getBucketLock(bucket).readLock().lock();
			try {
				checkIsAccesible(bucket);
				// Cross-volume metadata search (active volume first)
				return getDriverObjectMetadataInternal(bucket, objectName, false) != null;
			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));
			} finally {
				getLockService().getBucketLock(bucket).readLock().unlock();
			}
		} finally {
			getLockService().getObjectLock(bucket, objectName).readLock().unlock();
		}
	}

	@Override
	public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType, Optional<List<String>> customTags) {
		putObject(bucket, objectName, stream, fileName, contentType, customTags, Optional.of(Boolean.FALSE));
	}

	/**
	 * 
	 * 
	 */
	@Override
	public void putObject(ServerBucket bucket, String objectName, InputStream stream, String fileName, String contentType, Optional<List<String>> customTags, Optional<Boolean> o_public) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucket.getName());
		Check.requireNonNullStringArgument(fileName, "fileName is null | b: " + bucket.getName() + " o:" + objectName);
		Check.requireNonNullArgument(stream, "InpuStream can not null -> b:" + bucket.getName() + " | o:" + objectName);
		if (exists(bucket, objectName)) {
			RAIDSixUpdateObjectHandler updateAgent = new RAIDSixUpdateObjectHandler(this, bucket, objectName);
			updateAgent.update(bucket, objectName, stream, fileName, contentType, customTags, o_public);
			getVirtualFileSystemService().getSystemMonitorService().getUpdateObjectCounter().inc();
		} else {
			RAIDSixCreateObjectHandler createAgent = new RAIDSixCreateObjectHandler(this, bucket, objectName);
			createAgent.create(stream, fileName, contentType, customTags, o_public);
			getVirtualFileSystemService().getSystemMonitorService().getCreateObjectCounter().inc();
		}
	}

	@Override
	public ObjectMetadata updateObjectMetadata(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		meta.setLastModified(OffsetDateTime.now());
		putObjectMetadata(meta);
		return meta;
	}

	@Override
	public void putObjectMetadata(ObjectMetadata meta) {
		Check.requireNonNullArgument(meta, "meta is null");
		RAIDSixUpdateObjectHandler updateAgent = new RAIDSixUpdateObjectHandler(this, getBucket(meta.getBucketName()), meta.getObjectName());
		updateAgent.updateObjectMetadataHeadVersion(meta);
		getVirtualFileSystemService().getSystemMonitorService().getUpdateObjectCounter().inc();
	}

	@Override
	public VirtualFileSystemObject getObject(ServerBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName can not be null | b:" + bucket.getName());

		String bucketName = bucket.getName();
		getLockService().getObjectLock(bucket, objectName).readLock().lock();

		try {
			bucketReadLock(bucket);
			try {

				checkIsAccesible(bucket);

				/** must be executed also inside the critical zone. */
				if (!existsCacheBucket(bucket.getName()))
					throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

				ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);
				if ((meta == null) || (!meta.isAccesible()))
					throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

				return new OdilonObject(bucket, objectName, getVirtualFileSystemService());

			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucketName, objectName));
			} finally {
				bucketReadUnLock(bucket);

			}
		} finally {
			getLockService().getObjectLock(bucket, objectName).readLock().unlock();
		}
	}

	/**
	 * 
	 */
	@Override
	public void postObjectDeleteTransaction(ObjectMetadata meta, int headVersion) {
		/**
		 * Check.requireNonNullArgument(meta, "meta is null"); String bucketName =
		 * meta.getBucketName(); String objectName = meta.getObjectName();
		 * Check.requireNonNullArgument(bucketName, "bucketName is null");
		 * Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" +
		 * bucketName); RAIDSixDeleteObjectHandler deleteAgent = new
		 * RAIDSixDeleteObjectHandler(this); deleteAgent.postObjectDelete(meta,
		 * headVersion);
		 **/
	}

	/**
	 * 
	 */
	@Override
	public void postObjectPreviousVersionDeleteAllTransaction(ObjectMetadata meta, int headVersion) {
		/**
		 * Check.requireNonNullArgument(meta, "meta is null"); String bucketName =
		 * meta.getBucketName(); String objectName = meta.getObjectName();
		 * Check.requireNonNullArgument(bucketName, "bucket is null");
		 * Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" +
		 * bucketName); RAIDSixDeleteObjectHandler deleteAgent = new
		 * RAIDSixDeleteObjectHandler(this);
		 * deleteAgent.postObjectPreviousVersionDeleteAll(meta, headVersion);
		 **/
	}

	@Override
	public boolean hasVersions(ServerBucket bucket, String objectName) {
		return !getObjectMetadataVersionAll(bucket, objectName).isEmpty();
	}

	@Override
	public List<ObjectMetadata> getObjectMetadataVersionAll(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

		getLockService().getObjectLock(bucket, objectName).readLock().lock();
		try {
			bucketReadLock(bucket);
			try {
				checkIsAccesible(bucket);

				if (!existsCacheBucket(bucket.getName()))
					throw new IllegalArgumentException("bucket does not exist -> " + objectInfo(bucket));

				// ── Cross-volume head metadata lookup ─────────────────────────────────────────
				// getDriverObjectMetadataInternal() searches active volume first, then archives.
				// This gives us the correct volumeId so that version reads go to the right drive.
				ObjectMetadata meta = getDriverObjectMetadataInternal(bucket, objectName, true);

				if ((meta == null) || (!meta.isAccesible()))
					throw new OdilonObjectNotFoundException(ObjectMetadata.class.getName() + " does not exist");

				meta.setBucketName(bucket.getName());

				if (meta.getVersion() == 0)
					return list;

				// Use an ENABLED drive from the owning volume for version reads.
				List<Drive> vDrives = getVolumeForObject(meta).getDrives();
				List<Drive> enabledVAll = vDrives.stream()
						.filter(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
						.collect(Collectors.toList());
				List<Drive> vAllPool = enabledVAll.isEmpty() ? vDrives : enabledVAll;
				Drive readDrive = vAllPool.get(
						Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % vAllPool.size());

				for (int version = 0; version < meta.getVersion(); version++) {
					ObjectMetadata meta_version = readDrive.getObjectMetadataVersion(bucket, objectName, version);
					if (meta_version != null) {
						meta_version.setBucketName(bucket.getName());
						list.add(meta_version);
					}
				}
				return list;

			} catch (OdilonObjectNotFoundException e) {
				throw e;
			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));
			} finally {
				bucketReadUnLock(bucket);
			}
		} finally {
			getLockService().getObjectLock(bucket, objectName).readLock().unlock();
		}
	}

	@Override
	public void wipeAllPreviousVersions() {
		getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class));
	}

	@Override
	public void delete(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());
		RAIDSixDeleteObjectHandler agent = new RAIDSixDeleteObjectHandler(this, bucket, objectName);
		agent.delete();
	}

	@Override
	public ObjectMetadata getObjectMetadataVersion(ServerBucket bucket, String objectName, int version) {
		return getOM(bucket, objectName, Optional.of(Integer.valueOf(version)), true);
	}

	@Override
	public ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		RAIDSixUpdateObjectHandler agent = new RAIDSixUpdateObjectHandler(this, bucket, objectName);
		return agent.restorePreviousVersion(bucket, objectName);
	}

	@Override
	public void deleteObjectAllPreviousVersions(ServerBucket bucket, String objectName) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(bucket, "bucket does not exist ->" + objectInfo(bucket));
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + objectInfo(bucket));
		RAIDSixDeleteObjectAllPreviousVersionsHandler agent = new RAIDSixDeleteObjectAllPreviousVersionsHandler(this, bucket, objectName);
		agent.delete();

	}

	@Override
	public void deleteBucketAllPreviousVersions(ServerBucket bucket) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(bucket, "bucket does not exist ->" + objectInfo(bucket));
		Check.requireTrue(bucket.isAccesible(), "bucket is not Accesible " + objectInfo(bucket));
		getSchedulerService().enqueue(getVirtualFileSystemService().getApplicationContext().getBean(DeleteBucketObjectPreviousVersionServiceRequest.class, bucket.getName(), bucket.getId()));

	}

	@Override
	public RedundancyLevel getRedundancyLevel() {
		return RedundancyLevel.RAID_6;
	}

	@Override
	public boolean setUpDrives() {
		logger.debug("Starting async process to set up drives");
		return getApplicationContext().getBean(RAIDSixDriveSetup.class, this).setup();
	}

	/**
	 * <p>
	 * Weak Consistency.<br/>
	 * If a file gives error while bulding the {@link DataList}, the Item will
	 * contain an String with the error {code isOK()} should be used before
	 * getObject()
	 * </p>
	 */
	@Override
	public DataList<Item<ObjectMetadata>> listObjects(ServerBucket bucket, Optional<Long> offset, Optional<Long> pageSize, Optional<String> prefix, Optional<String> serverAgentId) {

		Check.requireNonNullArgument(bucket, "bucket is null");

		BucketIterator walker = null;
		BucketIteratorService walkerService = getVirtualFileSystemService().getBucketIteratorService();

		try {
			if (serverAgentId.isPresent())
				walker = walkerService.get(serverAgentId.get());

			if (walker == null) {
				walker = new RAIDSixBucketIterator(this, bucket, offset, prefix);
				walkerService.register(walker);
			}

			List<Item<ObjectMetadata>> list = new ArrayList<Item<ObjectMetadata>>();

			long size = pageSize.orElseGet(() -> ServerConstant.DEFAULT_PAGE_SIZE);

			int counter = 0;

			while (walker.hasNext() && counter++ < size) {
				Item<ObjectMetadata> item;
				try {
					Path path = walker.next();
					String objectName = path.toFile().getName();
					item = new Item<ObjectMetadata>(getObjectMetadata(bucket, objectName));

				} catch (IllegalMonitorStateException e) {
					logger.error(e, SharedConstant.NOT_THROWN);
					item = new Item<ObjectMetadata>(e);
				} catch (Exception e) {
					logger.error(e, SharedConstant.NOT_THROWN);
					item = new Item<ObjectMetadata>(e);
				}
				list.add(item);
			}

			DataList<Item<ObjectMetadata>> result = new DataList<Item<ObjectMetadata>>(list);

			if (!walker.hasNext())
				result.setEOD(true);

			result.setOffset(walker.getOffset());
			result.setPageSize(size);
			result.setAgentId(walker.getAgentId());

			return result;

		} finally {
			if (walker != null && (!walker.hasNext()))
				getVirtualFileSystemService().getBucketIteratorService().remove(walker.getAgentId()); /** closes the stream upon removal */
		}
	}

	/**
	 * <p>
	 * RAID 6 volume-aware metadata read drive selection.
	 * </p>
	 * <p>
	 * When the object metadata is in cache its {@code volumeId} is already known —
	 * pick a random drive from that volume's drive list.
	 * When it is a cache-miss we do not yet know which volume owns the object, so
	 * we fall back to a random drive from the <em>active</em> volume (the most
	 * likely owner for recently-written objects). The cross-volume search is done
	 * in {@link #getDriverObjectMetadataInternal} which is always called before any
	 * actual read.
	 * </p>
	 */
	@Override
	protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
		// Fast path: object is cached and volumeId is known
		if (getObjectMetadataCacheService().containsKey(bucket, objectName)) {
			ObjectMetadata cached = getObjectMetadataCacheService().get(bucket, objectName);
			List<Drive> vDrives = getVolumeManager().getVolumeById(cached.getVolumeId()).getDrives();
			// NOTSYNC drives have no metadata yet — restrict pool to ENABLED drives only.
			List<Drive> pool = vDrives.stream()
					.filter(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
					.collect(Collectors.toList());
			if (pool.isEmpty()) pool = vDrives; // safety fallback (should never happen)
			return pool.get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % pool.size());
		}
		// Cache-miss: return a random ENABLED drive from the active volume.
		// getDriverObjectMetadataInternal() will do the real cross-volume search.
		List<Drive> activeDrives = getVolumeManager().getActiveVolume().getDrives();
		List<Drive> pool = activeDrives.stream()
				.filter(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
				.collect(Collectors.toList());
		if (pool.isEmpty()) pool = activeDrives; // safety fallback
		return pool.get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % pool.size());
	}

	/**
	 * <p>
	 * Volume-aware ObjectMetadata lookup for RAID 6.
	 * </p>
	 * <ol>
	 *   <li>Check the object-metadata cache — if found, return immediately
	 *       (volumeId already embedded).</li>
	 *   <li>Search the <b>active volume</b> first (most objects are recent).</li>
	 *   <li>Search remaining volumes newest-to-oldest.</li>
	 *   <li>Cache the result and return it; return {@code null} if not found
	 *       anywhere (caller turns this into {@link OdilonObjectNotFoundException}).</li>
	 * </ol>
	 */
	@Override
	protected ObjectMetadata getDriverObjectMetadataInternal(ServerBucket bucket, String objectName, boolean addToCacheIfMiss) {

		// 1. Cache hit
		if (getServerSettings().isUseObjectCache() && getObjectMetadataCacheService().containsKey(bucket, objectName)) {
			ObjectMetadata cached = getObjectMetadataCacheService().get(bucket, objectName);
			cached.setBucketName(bucket.getName());
			getSystemMonitorService().getCacheObjectHitCounter().inc();
			return cached;
		}

		// 2. Cross-volume search: active volume first, then archives newest-to-oldest
		for (RAIDSixVolume volume : getVolumeManager().getVolumesInSearchOrder()) {
			List<Drive> vDrives = volume.getDrives();
			// NOTSYNC drives have no metadata yet — only read from ENABLED drives.
			// If a candidate NOTSYNC drive is picked, it returns null and the object
			// looks "not found" even though the metadata exists on the other drives,
			// causing spurious OdilonObjectNotFoundException during drive sync.
			List<Drive> pool = vDrives.stream()
					.filter(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
					.collect(Collectors.toList());
			if (pool.isEmpty())
				continue; // entire volume is still mid-sync — skip it
			Drive candidate = pool.get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % pool.size());
			ObjectMetadata meta = candidate.getObjectMetadata(bucket, objectName);
			if (meta != null) {
				meta.setBucketName(bucket.getName());
				// Ensure volumeId is correctly set (guards against legacy objects with volumeId==0)
				meta.setVolumeId(volume.getVolumeId());
				getSystemMonitorService().getCacheObjectMissCounter().inc();
				if (addToCacheIfMiss && getServerSettings().isUseObjectCache())
					getObjectMetadataCacheService().put(bucket, objectName, meta);
				return meta;
			}
		}

		// Not found on any volume
		return null;
	}
	private ObjectMetadata getOM(ServerBucket bucket, String objectName, Optional<Integer> o_version, boolean addToCacheifMiss) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		objectReadLock(bucket, objectName);
		try {
			bucketReadLock(bucket);
			try {
				checkIsAccesible(bucket);

				ObjectMetadata meta;

				if (o_version.isPresent()) {
					// ── Version read: first resolve the owning volume via cross-volume search ──
					ObjectMetadata headMeta = getDriverObjectMetadataInternal(bucket, objectName, true);
					if (headMeta == null || !headMeta.isAccesible())
						throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

					// Pick an ENABLED drive from the owning volume.
					// NOTSYNC drives have no metadata — picking one would return null.
					List<Drive> vDrives = getVolumeForObject(headMeta).getDrives();
					List<Drive> enabledV = vDrives.stream()
							.filter(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
							.collect(Collectors.toList());
					List<Drive> vPool = enabledV.isEmpty() ? vDrives : enabledV;
					Drive vDrive = vPool.get(
							Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % vPool.size());

					meta = vDrive.getObjectMetadataVersion(bucket, objectName, o_version.get());
				} else {
					meta = getDriverObjectMetadataInternal(bucket, objectName, addToCacheifMiss);
				}

				if ((meta == null) || (!meta.isAccesible()))
					throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

				meta.setBucketName(bucket.getName());
				return meta;

			} catch (InternalCriticalException e) {
				throw e;
			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucket, objectName) + (o_version.isPresent() ? (", v:" + String.valueOf(o_version.get())) : ""));
			} finally {
				bucketReadUnLock(bucket);
			}
		} finally {
			objectReadUnLock(bucket, objectName);
		}
	}

	/**
	 * @param meta
	 * @param version
	 * @return
	 */
	protected boolean isConfigurationValid(int dataShards, int parityShards) {
		return getVirtualFileSystemService().getServerSettings().isRAID6ConfigurationValid(dataShards, parityShards);
	}

	// ─── Volume helpers ────────────────────────────────────────────────────────

	/**
	 * Returns the {@link OdilonRAIDSixVolumeManager} from the VFS service.
	 */
	public OdilonRAIDSixVolumeManager getVolumeManager() {
		return getVirtualFileSystemService().getVolumeManager();
	}

	/**
	 * Returns the currently active {@link RAIDSixVolume} – the one that receives
	 * new-object writes.
	 */
	public RAIDSixVolume getActiveVolume() {
		return getVolumeManager().getActiveVolume();
	}

	/**
	 * Resolves the {@link RAIDSixVolume} that holds the shards for {@code meta}.
	 * Defaults to volume 0 when {@code meta.volumeId == 0} (backward compatible
	 * with objects created before multi-volume support).
	 */
	public RAIDSixVolume getVolumeForObject(ObjectMetadata meta) {
		return getVolumeManager().getVolumeById(meta.getVolumeId());
	}

	// ─── Shard-file helpers (volume-aware) ────────────────────────────────────

	/**
	 * <p>
	 * Returns a map {@code Drive → [shard file names]} for the given object,
	 * using the <em>volume-local</em> disk indices stored in the shard file names.
	 * </p>
	 * <p>
	 * Uses {@code meta.getVolumeId()} to select the correct volume. Backward
	 * compatible: objects with {@code volumeId == 0} behave exactly as before
	 * (volume-local index == global {@link Drive#getConfigOrder()}).
	 * </p>
	 */
	protected Map<Drive, List<String>> getObjectDataFilesNames(ObjectMetadata meta, Optional<Integer> version) {

		Check.requireNonNullArgument(meta, "meta is null");

		RAIDSixVolume volume = getVolumeForObject(meta);
		List<Drive> volumeDrives = volume.getDrives();

		Map<Drive, List<String>> map = new HashMap<Drive, List<String>>();
		for (Drive drive : volumeDrives)
			map.put(drive, new ArrayList<String>());

		int totalBlocks = meta.getSha256Blocks().size();
		int totalDisks  = volume.getTotalShards();

		Check.checkTrue(totalDisks > 0, "total disks must be greater than zero");

		int chunks = totalBlocks / totalDisks;
		Check.checkTrue(chunks > 0, "chunks must be greater than zero");

		for (int chunk = 0; chunk < chunks; chunk++) {
			for (int disk = 0; disk < volumeDrives.size(); disk++) {
				// disk == volume-local index (matches encoder / decoder convention)
				String suffix = "." + String.valueOf(chunk) + "." + String.valueOf(disk)
						+ (version.isEmpty() ? "" : (".v" + String.valueOf(version.get())));
				Drive drive = volumeDrives.get(disk);
				map.get(drive).add(meta.getObjectName() + suffix);
			}
		}
		return map;
	}

	/**
	 * <p>
	 * Returns the list of shard {@link File}s for the given object, using
	 * volume-local disk indices so that files on any volume are located correctly.
	 * </p>
	 */
	protected List<File> getObjectDataFiles(ObjectMetadata meta, ServerBucket bucket, Optional<Integer> version) {

		List<File> files = new ArrayList<File>();

		if (meta == null)
			return files;

		RAIDSixVolume volume = getVolumeForObject(meta);
		List<Drive> volumeDrives = volume.getDrives();

		int totalBlocks = meta.getSha256Blocks().size();
		int totalDisks  = volume.getTotalShards();

		Check.checkTrue(totalDisks > 0, "total disks must be greater than zero");

		int chunks = totalBlocks / totalDisks;
		Check.checkTrue(chunks > 0, "chunks must be greater than zero");

		for (int chunk = 0; chunk < chunks; chunk++) {
			for (int disk = 0; disk < volumeDrives.size(); disk++) {
				String suffix = "." + String.valueOf(chunk) + "." + String.valueOf(disk)
						+ (version.isEmpty() ? "" : (".v" + String.valueOf(version.get())));
				Drive drive = volumeDrives.get(disk);
				if (version.isEmpty())
					files.add(new File(drive.getBucketObjectDataDirPath(bucket), meta.getObjectName() + suffix));
				else
					files.add(new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR,
							meta.getObjectName() + suffix));
			}
		}
		return files;
	}

	// ─── Journal overrides ────────────────────────────────────────────────────────

	/**
	 * <p>
	 * RAID 6 multi-volume override: write journal entries only to the
	 * <em>active</em> volume's drives.
	 * </p>
	 * <p>
	 * Rationale: if volume 0 is at OS-level capacity, writing to its drives would
	 * throw an {@link IOException}. Journal files for new operations belong on the
	 * drives that are currently receiving writes. Recovery ({@link #getJournalPending})
	 * already scans ALL enabled drives, so entries written here will always be
	 * found on restart.
	 * </p>
	 */
	@Override
	public void saveJournal(VirtualFileSystemOperation op) {
		getLockService().getJournalLock().writeLock().lock();
		try {
			for (Drive drive : getActiveVolume().getDrives())
				drive.saveJournal(op);
		} finally {
			getLockService().getJournalLock().writeLock().unlock();
		}
	}

	/**
	 * <p>
	 * RAID 6 multi-volume override: remove journal entries from <em>all</em>
	 * enabled drives across all volumes.
	 * </p>
	 * <p>
	 * On a server restart the active volume may differ from the one that was active
	 * when a pending operation was saved. Scanning all drives ensures stale journal
	 * files left on an old volume's drives are cleaned up.
	 * {@link Drive#removeJournal} uses {@code deleteIfExists} so missing files are
	 * silently ignored — this is correct because {@link #saveJournal} writes only
	 * to the <em>active</em> volume's drives, so drives on other volumes will never
	 * have the file.
	 * </p>
	 * <p>
	 * Each drive is attempted independently: a genuine I/O error on one drive is
	 * logged but must not abort the loop, otherwise the journal file would remain
	 * on all subsequent drives and be replayed again on the next restart.
	 * </p>
	 */
	@Override
	public void removeJournal(String id) {
		getLockService().getJournalLock().writeLock().lock();
		try {
			for (Drive drive : getDrivesEnabled()) {
				try {
					drive.removeJournal(id);
				} catch (Exception e) {
					// Log and continue: a failure on one drive must not prevent deletion
					// from all remaining drives (including the volume that actually holds
					// the journal file). The caller (OdilonJournalService) already treats
					// a partial-delete gracefully.
					logger.error("removeJournal: failed on drive -> " + drive.getName()
							+ " | id=" + id + " | " + e.getMessage(), SharedConstant.NOT_THROWN);
				}
			}
		} finally {
			getLockService().getJournalLock().writeLock().unlock();
		}
	}

	// ─── Scheduler overrides ──────────────────────────────────────────────────────

	/**
	 * <p>
	 * RAID 6 multi-volume override: persist scheduler requests only on the
	 * <em>active</em> volume's drives.
	 * </p>
	 * <p>
	 * The single-volume base implementation writes to every enabled drive and
	 * validates completeness by requiring the file to appear on <em>every</em>
	 * enabled drive ({@link #getSchedulerPendingRequests}). With multiple volumes
	 * this cross-volume check would wrongly discard a request saved before a volume
	 * switch. Scoping writes to the active volume makes the consistency boundary
	 * per-volume instead of server-global.
	 * </p>
	 */
	@Override
	public void saveScheduler(ServiceRequest request, String queueId) {
		getLockService().getSchedulerLock().writeLock().lock();
		try {
			for (Drive drive : getActiveVolume().getDrives())
				drive.saveScheduler(request, queueId);
		} finally {
			getLockService().getSchedulerLock().writeLock().unlock();
		}
	}

	/**
	 * <p>
	 * RAID 6 multi-volume override: remove scheduler requests from <em>all</em>
	 * enabled drives across all volumes.
	 * </p>
	 * <p>
	 * A request may have been saved on an older volume before the active volume
	 * switched. Removing from all drives ensures no orphan scheduler files remain.
	 * </p>
	 */
	@Override
	public void removeScheduler(ServiceRequest request, String queueId) {
		getLockService().getSchedulerLock().writeLock().lock();
		try {
			for (Drive drive : getDrivesEnabled())
				drive.removeScheduler(request, queueId);
		} finally {
			getLockService().getSchedulerLock().writeLock().unlock();
		}
	}

	/**
	 * <p>
	 * RAID 6 multi-volume override: recover scheduler requests by checking
	 * consistency <em>per-volume</em> rather than across all drives globally.
	 * </p>
	 * <p>
	 * The base implementation ({@link BaseIODriver#getSchedulerPendingRequests})
	 * requires every request to appear on <em>all</em> enabled drives. In a
	 * multi-volume setup a request saved on volume 0 would be absent from volume 1's
	 * drives and wrongly discarded. This override checks completeness within each
	 * volume independently and aggregates valid requests from all volumes.
	 * </p>
	 */
	@Override
	public synchronized List<ServiceRequest> getSchedulerPendingRequests(String queueId) {

		List<ServiceRequest> result = new ArrayList<>();

		getLockService().getSchedulerLock().writeLock().lock();
		try {
			for (RAIDSixVolume volume : getVolumeManager().getAllVolumes()) {

				List<Drive> vDrives = volume.getDrives();
				if (vDrives.isEmpty())
					continue;

				// Collect scheduler files per drive within this volume
				Map<Drive, Map<String, File>> driveFiles = new HashMap<>();
				for (Drive drive : vDrives) {
					Map<String, File> fileMap = new HashMap<>();
					for (File file : drive.getSchedulerRequests(queueId))
						fileMap.put(file.getName(), file);
					driveFiles.put(drive, fileMap);
				}

				Drive referenceDrive = vDrives.get(0);
				Map<String, File> referenceMap = driveFiles.get(referenceDrive);
				Map<String, File> useful  = new HashMap<>();
				Map<String, File> useless = new HashMap<>();

				// A request is valid iff it exists on ALL drives within this volume
				referenceMap.forEach((name, file) -> {
					boolean complete = vDrives.stream()
							.filter(d -> !d.equals(referenceDrive))
							.allMatch(d -> driveFiles.get(d).containsKey(name));
					if (complete)
						useful.put(name, file);
					else
						useless.put(name, file);
				});

				// Deserialize valid requests (skip if already collected from another volume)
				for (Map.Entry<String, File> entry : useful.entrySet()) {
					try {
						AbstractServiceRequest req = getObjectMapper().readValue(entry.getValue(), AbstractServiceRequest.class);
						boolean alreadyPresent = result.stream()
								.anyMatch(r -> r.getId() != null && r.getId().equals(req.getId()));
						if (!alreadyPresent)
							result.add(req);
					} catch (Exception e) {
						logger.error("Failed to deserialize ServiceRequest: " + entry.getValue().getAbsolutePath()
								+ " | " + e.getMessage(), SharedConstant.NOT_THROWN);
					}
				}

				// Delete incomplete (useless) files from this volume's drives
				for (Drive drive : vDrives) {
					driveFiles.get(drive).forEach((name, file) -> {
						if (!useful.containsKey(name)) {
							try {
								Files.delete(file.toPath());
							} catch (Exception e) {
								logger.error(e, SharedConstant.NOT_THROWN);
							}
						}
					});
				}
			}
		} finally {
			getLockService().getSchedulerLock().writeLock().unlock();
		}
		return result;
	}

} // end RAIDSixDriver