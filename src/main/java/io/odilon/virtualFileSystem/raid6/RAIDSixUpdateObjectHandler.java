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
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.OdilonVersion;
import io.odilon.encryption.EncryptedResult;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.SharedConstant;
import io.odilon.model.VersionControl;
import io.odilon.util.Check;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * RAID 6 Update Object handler
 * </p>
 * <p>
 * Auxiliary class used by {@link RaidSixHandler}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixUpdateObjectHandler extends RAIDSixTransactionObjectHandler {

	private static Logger logger = Logger.getLogger(RAIDSixUpdateObjectHandler.class.getName());

	/**
	 * <p>
	 * Instances of this class are used internally by {@link RAIDSixDriver}
	 * </p>
	 * 
	 * @param driver can not be null
	 */
	protected RAIDSixUpdateObjectHandler(RAIDSixDriver driver, ServerBucket bucket, String objectName) {
		super(driver, bucket, objectName);
	}

	/**
	 * This check must be executed inside the critical section
	 * 
	 * @param bucket
	 * @param objectName
	 * @return
	 */
	/**
	 * <p>
	 * Volume-aware existence check (Bug U1 fix).
	 * </p>
	 * <p>
	 * The original implementation called {@code getObjectMetadataReadDrive()} which,
	 * on a cache-miss, returns a random drive from the <em>active</em> volume. Objects
	 * stored on an older (READONLY) volume have no metadata on that drive → the method
	 * returned {@code false} → {@code checkExistObject} threw
	 * {@link OdilonObjectNotFoundException} → every {@code update()} on an object
	 * created before the last volume switch failed.
	 * </p>
	 * <p>
	 * Delegating to {@link RAIDSixDriver#getDriverObjectMetadataInternal} (which
	 * searches the active volume first, then all older volumes) fixes the problem
	 * and is consistent with every other existence check in the RAID 6 layer.
	 * </p>
	 */
	protected boolean existsObjectMetadata(ServerBucket bucket, String objectName) {
		if (existsCacheObject(bucket, objectName))
			return true;
		// cross-volume search: active volume first, then archives newest-to-oldest
		return getDriver().getDriverObjectMetadataInternal(bucket, objectName, false) != null;
	}

	/**
	 * @param bucket      can not be null
	 * @param objectName  can not be null
	 * @param stream      can not be null
	 * @param srcFileName
	 * @param contentType
	 * @param customTags
	 */
	protected void update(ServerBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType, Optional<List<String>> customTags, Optional<Boolean> o_public) {

		String bucketName = bucket.getName();

		VirtualFileSystemOperation operation = null;
		boolean commitOK = false;
		boolean isMainException = false;

		int beforeHeadVersion = -1;
		int afterHeadVersion = -1;
		ObjectMetadata meta = null;

		getLockService().getObjectLock(bucket, objectName).writeLock().lock();
		try {

			getLockService().getBucketLock(bucket).readLock().lock();
			try (stream) {

				checkExistsBucket(bucket);
				checkExistObject(bucket, objectName);

				meta = getMetadata(bucket, objectName, true);

				beforeHeadVersion = meta.getVersion();

				/** backup */
				backupVersionObjectDataFile(meta, bucket, meta.getVersion());
				backupVersionObjectMetadata(bucket, objectName, meta.getVersion());

				/** start operation */
				operation = getJournalService().updateObject(bucket, objectName, beforeHeadVersion);

				/** copy new version as head version */
				afterHeadVersion = meta.getVersion() + 1;
				RAIDSixBlocks ei = saveObjectDataFile(bucket, objectName, stream);
				saveObjectMetadata(bucket, objectName, ei, srcFileName, contentType, afterHeadVersion, meta.getCreationDate(), customTags, o_public);

				/** commit */
				commitOK = operation.commit();

			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, objectInfo(bucketName, objectName, srcFileName));

			} finally {
				try {
					if (!commitOK) {
						try {
							rollback(operation);
						} catch (Exception e) {
							if (isMainException)
								throw new InternalCriticalException(e, objectInfo(bucketName, objectName, srcFileName));
							else
								logger.error(objectInfo(bucketName, objectName, srcFileName), SharedConstant.NOT_THROWN);
						}
					} else {
						/** TODO Sync by the moment. see how to make it Async */
						cleanUpUpdate(meta, bucket, beforeHeadVersion, afterHeadVersion);
					}
				} finally {
					getLockService().getBucketLock(bucket).readLock().unlock();
				}
			}

		} finally {
			getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
		}
	}

	protected void updateObjectMetadataHeadVersion(ObjectMetadata meta) {
		updateObjectMetadata(meta, true);
	}

	/**
	 * 
	 * @param meta can not be null
	 * 
	 */
	protected void updateObjectMetadata(ObjectMetadata meta, boolean isHead) {

		VirtualFileSystemOperation operation = null;

		boolean done = false;

		ServerBucket bucket = null;

		getLockService().getObjectLock(meta.getBucketId(), meta.getObjectName()).writeLock().lock();
		try {

			getLockService().getBucketLock(meta.getBucketId()).readLock().lock();
			try {

				checkExistsBucket(meta.getBucketId());
				bucket = getBucketCache().get(meta.getBucketId());

				checkExistObject(bucket, meta.getObjectName());

				/** backup */
				backup(meta, bucket);

				/** start operation */
				operation = getJournalService().updateObjectMetadata(bucket, meta.getObjectName(), meta.getVersion());

				saveObjectMetadata(meta, isHead);

				/** commit */
				done = operation.commit();

			} catch (Exception e) {
				done = false;
				throw new InternalCriticalException(e, objectInfo(meta));

			} finally {
				try {
					if (!done) {
						try {
							rollback(operation);
						} catch (Exception e) {
							throw new InternalCriticalException(e, objectInfo(meta));
						}
				} else {
					/**
					 * TODO AT -> Sync by the moment. TODO see how to make it Async
					 */
					cleanUpBackupMetadataDir(bucket, meta);
				}
				} finally {
					getLockService().getBucketLock(meta.getBucketId()).readLock().unlock();
				}
			}
		} finally {
			getLockService().getObjectLock(meta.getBucketId(), meta.getObjectName()).writeLock().unlock();
		}
	}

	/**
	 * @param bucket     can not be null
	 * @param objectName can not be null
	 * 
	 * @return ObjectMetadata of the restored object
	 */
	protected ObjectMetadata restorePreviousVersion(ServerBucket bucket, String objectName) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		String bucketName = bucket.getName();
		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty " + objectInfo(bucket));

		VirtualFileSystemOperation operation = null;
		boolean done = false;

		int beforeHeadVersion = -1;

		boolean isMainException = false;

		ObjectMetadata metaHeadToRemove = null;
		ObjectMetadata metaToRestore = null;

		getLockService().getObjectLock(bucket, objectName).writeLock().lock();
		try {

			getLockService().getBucketLock(bucket).readLock().lock();
			try {

				checkExistsBucket(bucket);
				checkExistObject(bucket, objectName);

				metaHeadToRemove = getMetadata(bucket, objectName, false);

				if ((metaHeadToRemove == null) || (!metaHeadToRemove.isAccesible()))
					throw new OdilonObjectNotFoundException(objectInfo(bucket, objectName));

				if (metaHeadToRemove.getVersion() == VERSION_ZERO)
					throw new IllegalArgumentException("Object does not have versions -> " + objectInfo(bucket, objectName));

				beforeHeadVersion = metaHeadToRemove.getVersion();

				List<ObjectMetadata> metaVersions = new ArrayList<ObjectMetadata>();

				// ── read version metadata from the object's owning volume ─────────
				// getObjectMetadataReadDrive() for a cache-miss returns a drive from the ACTIVE
				// volume. Objects on older (READONLY) volumes have no version metadata on those
				// drives → mv == null for every iteration → metaVersions stays empty →
				// OdilonObjectNotFoundException("previous versions deleted").
				// metaHeadToRemove already carries the correct volumeId from the cross-volume
				// search above, so we use its volume's drive list directly.
				List<Drive> ownerDrives = getDriver().getVolumeForObject(metaHeadToRemove).getDrives();
				Drive vReadDrive = ownerDrives.get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % ownerDrives.size());
				// ─────────────────────────────────────────────────────────────────────────────

				for (int version = 0; version < beforeHeadVersion; version++) {
					ObjectMetadata mv = vReadDrive.getObjectMetadataVersion(bucket, objectName, version);
					if (mv != null)
						metaVersions.add(mv);
				}

				if (metaVersions.isEmpty())
					throw new OdilonObjectNotFoundException(Optional.of(metaHeadToRemove.systemTags).orElse("previous versions deleted"));

				/** backup */
				/**
				 * save current head version MetadataFile .vN and data File vN - no need to
				 * additional backup
				 */
				backupVersionObjectDataFile(metaHeadToRemove, bucket, metaHeadToRemove.getVersion());
				backupVersionObjectMetadata(bucket, objectName, metaHeadToRemove.getVersion());

				/** start operation */
				operation = getJournalService().restoreObjectPreviousVersion(bucket, objectName, beforeHeadVersion);

				/** save previous version as head */
				metaToRestore = metaVersions.get(metaVersions.size() - 1);

				if (!restoreVersionObjectDataFile(metaToRestore, bucket, metaToRestore.getVersion()))
					throw new OdilonObjectNotFoundException(Optional.of(metaHeadToRemove.systemTags).orElse("previous versions deleted"));

				if (!restoreVersionObjectMetadata(bucket, metaToRestore.objectName, metaToRestore.getVersion()))
					throw new OdilonObjectNotFoundException(Optional.of(metaHeadToRemove.systemTags).orElse("previous versions deleted"));

				/** commit */
				done = operation.commit();

				return metaToRestore;

			} catch (Exception e) {
				done = false;
				isMainException = true;
				logger.error(e, SharedConstant.NOT_THROWN);
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));

			} finally {

				try {

					if (!done) {
						try {
							rollback(operation);
						} catch (InternalCriticalException e) {
							if (!isMainException)
								throw new InternalCriticalException(e);
							else
								logger.error(e, objectInfo(bucketName, objectName), SharedConstant.NOT_THROWN);

						} catch (Exception e) {
							if (!isMainException)
								throw new InternalCriticalException(e, objectInfo(bucket, objectName));
							else
								logger.error(e, objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
						}
					} else {
						/**
						 * TODO AT -> Sync by the moment see how to make it Async
						 */
						if ((operation != null) && (metaHeadToRemove != null) && (metaToRestore != null))
							cleanUpRestoreVersion(metaHeadToRemove, bucket, beforeHeadVersion, metaToRestore);
					}
				} finally {
					getLockService().getBucketLock(bucket).readLock().unlock();
				}
			}
		} finally {
			getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
		}
	}

	/**
	 * @param meta
	 * @param versionDiscarded
	 */
	private void cleanUpRestoreVersion(ObjectMetadata metaHeadRemoved, ServerBucket bucket, int versionDiscarded, ObjectMetadata metaNewHeadRestored) {

		try {
			if (versionDiscarded < 0)
				return;

			String objectName = metaHeadRemoved.getObjectName();

			// Only touch drives on the volume that owns this object
			for (Drive drive : getDriver().getVolumeForObject(metaHeadRemoved).getDrives()) {
				FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket, objectName, versionDiscarded));
				FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket, objectName, metaNewHeadRestored.getVersion()));
			}
			{
				List<File> files = getDriver().getObjectDataFiles(metaHeadRemoved, bucket, Optional.of(versionDiscarded));
				files.forEach(file -> FileUtils.deleteQuietly(file));
			}
			{
				List<File> files = getDriver().getObjectDataFiles(metaHeadRemoved, bucket, Optional.of(metaNewHeadRestored.getVersion()));
				files.forEach(file -> FileUtils.deleteQuietly(file));
			}

		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * 
	 * @param bucket
	 * @param objectName
	 * @param version
	 */
	private void backupVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {
		try {
			// Resolve the owning volume via cross-volume search (object exists on disk)
			ObjectMetadata currentMeta = getDriver().getDriverObjectMetadataInternal(bucket, objectName, false);
			List<Drive> drives = (currentMeta != null)
					? getDriver().getVolumeForObject(currentMeta).getDrives()
					: getDriver().getActiveVolume().getDrives();
			for (Drive drive : drives) {
				if (drive.getObjectMetadataFile(bucket, objectName).exists()) {
					ObjectMetadata meta = drive.getObjectMetadata(bucket, objectName);
					meta.setVersion(version);
					drive.saveObjectMetadataVersion(meta);
				}
			}
		} catch (InternalCriticalException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalCriticalException(e, objectInfo(bucket, objectName));
		}
	}

	/**
	 * backup current head version
	 * 
	 * @param bucket
	 * @param objectName
	 * @param version
	 */

	private void backupVersionObjectDataFile(ObjectMetadata meta, ServerBucket bucket, int headVersion) {

		Map<Drive, List<String>> map = getDriver().getObjectDataFilesNames(meta, Optional.empty());

		for (Drive drive : map.keySet()) {
			for (String filename : map.get(drive)) {
				File current = new File(drive.getBucketObjectDataDirPath(bucket), filename);
				String suffix = ".v" + String.valueOf(headVersion);
				File backupFile = new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, filename + suffix);
				try {

					if (current.exists())
						Files.copy(current.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

				} catch (IOException e) {
					throw new InternalCriticalException(e, "src: " + current.getName() + " | back:" + backupFile.getName());
				}
			}
		}
	}

	private void saveObjectMetadata(ServerBucket bucket, String objectName, RAIDSixBlocks ei, String srcFileName, String contentType, int version, OffsetDateTime headCreationDate, Optional<List<String>> customTags,
			Optional<Boolean> o_public) {

		Check.requireNonNullArgument(bucket, "bucket is null");

		List<String> shaBlocks = new ArrayList<String>();
		StringBuilder etag_b = new StringBuilder();
		final String bucketName = bucket.getName();

		ei.getEncodedBlocks().forEach(item -> {
			try {
				shaBlocks.add(OdilonFileUtils.calculateSHA256String(item));
			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucketName, objectName, item.getName()));
			}
		});

		shaBlocks.forEach(item -> etag_b.append(item));
		String etag = null;

		try {
			etag = OdilonFileUtils.calculateSHA256String(etag_b.toString());
		} catch (NoSuchAlgorithmException | IOException e) {
			throw new InternalCriticalException(e, objectInfo(bucketName, objectName, srcFileName));
		}

		OffsetDateTime versionCreationDate = OffsetDateTime.now();

		// ── Write metadata to the ACTIVE volume's drives ──────────────────────────
		// The encoder (RAIDSixEncoder) always writes new data shards to the active
		// volume. Metadata must follow those shards, so we write it to the same set
		// of drives. This means an updated object migrates to the active volume;
		// the previous version's metadata backup stays on the old volume's drives.
		final RAIDSixVolume activeVolume = getDriver().getActiveVolume();
		final List<Drive> drives = activeVolume.getDrives();
		final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

		for (Drive drive : drives) {

			try {
				ObjectMetadata newMeta = new ObjectMetadata(bucket.getId(), objectName);
				newMeta.setFileName(srcFileName);
				newMeta.setAppVersion(OdilonVersion.VERSION);
				newMeta.setContentType(contentType);
				newMeta.setCreationDate(headCreationDate);
				newMeta.setVersion(version);
				newMeta.setVersioncreationDate(versionCreationDate);

				newMeta.setLength(ei.getFileSize());
				newMeta.setSourceLength(ei.getSrcFileSize());

				newMeta.setTotalBlocks(ei.getEncodedBlocks().size());
				newMeta.setSha256Blocks(shaBlocks);
				newMeta.setEtag(etag);
				newMeta.setEncrypt(getVirtualFileSystemService().isEncrypt());
				newMeta.setIntegrityCheck(newMeta.getCreationDate());
				newMeta.setStatus(ObjectStatus.ENABLED);
				newMeta.setDrive(drive.getName());
				newMeta.setPublicAccess(o_public.orElse(Boolean.FALSE));
				newMeta.setRaid(String.valueOf(getRedundancyLevel().getCode()).trim());
				newMeta.setRaidDrives(activeVolume.getTotalShards());
				// The updated object now lives on the active volume.
				newMeta.setVolumeId(activeVolume.getVolumeId());
				if (customTags.isPresent())
					newMeta.setCustomTags(customTags.get());
				list.add(newMeta);

			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));
			}
		}

		saveRAIDSixObjectMetadataToDisk(drives, list, true);
	}

	/**
	 * @param bucket
	 * @param objectName
	 * @param stream
	 * @param srcFileName
	 */
	private RAIDSixBlocks saveObjectDataFile(ServerBucket bucket, String objectName, InputStream stream) {

		Check.requireNonNullArgument(bucket, "bucket is null");

		InputStream sourceStream = null;
		boolean isMainException = false;

		if (isEncrypt()) {

			try {
				EncryptedResult encryptedResult = getEncryptionService().encryptStream(stream);
				sourceStream = encryptedResult.getInputStream();
				RAIDSixEncoder encoder = new RAIDSixEncoder(getDriver());
				RAIDSixBlocks blocks = encoder.encodeHead(sourceStream, bucket, objectName);
				long totalBytesRead = encryptedResult.getCountingStream().getCount();
				blocks.setSrcFileSize(totalBytesRead);
				return blocks;

			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));

			} finally {
				IOException secEx = null;
				try {
					if (sourceStream != null)
						sourceStream.close();

				} catch (IOException e) {
					logger.error(e, (objectInfo(bucket, objectName)) + (isMainException ? SharedConstant.NOT_THROWN : ""));
					secEx = e;
				}
				if (!isMainException && (secEx != null))
					throw new InternalCriticalException(secEx);
			}

		} else {

			try {
				sourceStream = stream;
				RAIDSixEncoder encoder = new RAIDSixEncoder(getDriver());
				return encoder.encodeHead(sourceStream, bucket, objectName);

			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));

			} finally {
				IOException secEx = null;
				try {
					if (sourceStream != null)
						sourceStream.close();

				} catch (IOException e) {
					logger.error(e, (objectInfo(bucket, objectName)) + (isMainException ? SharedConstant.NOT_THROWN : ""));
					secEx = e;
				}
				if (!isMainException && (secEx != null))
					throw new InternalCriticalException(secEx);
			}
		}

	}

	/**
	 * <p>
	 * copy metadata directory <br/>
	 * . back up the full metadata directory (ie. ObjectMetadata for all versions)
	 * </p>
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backup(ObjectMetadata meta, ServerBucket bucket) {
		try {
			// Backup only from the drives that actually hold this object's metadata
			for (Drive drive : getDriver().getVolumeForObject(meta).getDrives()) {
				File src = new File(drive.getObjectMetadataDirPath(bucket, meta.getObjectName()));
				if (src.exists()) {
					File dest = new File(drive.getBucketWorkDirPath(bucket) + File.separator + meta.getObjectName());
					FileUtils.copyDirectory(src, dest);
				}
			}
		} catch (IOException e) {
			throw new InternalCriticalException(e, objectInfo(meta));
		}
	}

	/**
	 * <p>
	 * delete backup Metadata
	 * </p>
	 * 
	 * @param bucketName
	 * @param objectName
	 */
	/**
	 * <p>
	 * Deletes the work-dir backup created by {@link #backup(ObjectMetadata, ServerBucket)}.
	 * </p>
	 * <p>
	 * <b>Bug U3 fix:</b> the original implementation used
	 * {@code getActiveVolume().getDrives()} for the cleanup, but the backup is
	 * written to {@code getVolumeForObject(meta).getDrives()}. For metadata-only
	 * updates of objects on non-active volumes the backup directories were never
	 * removed, leaking work-dir disk space indefinitely.
	 * </p>
	 */
	private void cleanUpBackupMetadataDir(ServerBucket bucket, ObjectMetadata meta) {
		try {
			for (Drive drive : getDriver().getVolumeForObject(meta).getDrives()) {
				FileUtils.deleteQuietly(new File(drive.getBucketWorkDirPath(bucket) + File.separator + meta.getObjectName()));
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	private void cleanUpUpdate(ObjectMetadata meta, ServerBucket bucket, int previousVersion, int currentVersion) {
		if (meta == null)
			return;
		try {
			if (getVersionControl()==VersionControl.DISABLED) {
				// Remove previous-version metadata and data from the object's owning volume
				for (Drive drive : getDriver().getVolumeForObject(meta).getDrives()) {
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket, meta.getObjectName(), previousVersion));
					List<File> files = getDriver().getObjectDataFiles(meta, bucket, Optional.of(previousVersion));
					files.forEach(file -> {
						FileUtils.deleteQuietly(file);
					});
				}
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	private void saveObjectMetadata(ObjectMetadata meta, boolean isHead) {

		Check.requireNonNullArgument(meta, "meta is null");

		// Use the drives of the volume that owns this object, not all drives.
		final List<Drive> drives = getDriver().getVolumeForObject(meta).getDrives();
		final List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
		drives.forEach(d -> list.add(meta));
		saveRAIDSixObjectMetadataToDisk(drives, list, isHead);
	}

	private boolean restoreVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {

		Check.requireNonNullArgument(bucket, "bucket is null");
		Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucket.getName());

		try {
			boolean success = true;
			ObjectMetadata versionMeta = getDriver().getObjectMetadataVersion(bucket, objectName, version);
			// Stay on the same volume as the version being restored.
			List<Drive> drives = getDriver().getVolumeForObject(versionMeta).getDrives();
			for (Drive drive : drives) {
				versionMeta.setDrive(drive.getName());
				drive.saveObjectMetadata(versionMeta);
			}
			return success;

		} catch (InternalCriticalException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalCriticalException(e, objectInfo(bucket, objectName));
		}
	}

	private boolean restoreVersionObjectDataFile(ObjectMetadata meta, ServerBucket bucket, int version) {
		Check.requireNonNullArgument(meta.getBucketName(), "bucketName is null");
		Check.requireNonNullArgument(meta.getObjectName(), "objectName is null or empty | b:" + meta.getBucketName());
		try {
			Map<Drive, List<String>> versionToRestore = getDriver().getObjectDataFilesNames(meta, Optional.of(version));
			for (Drive drive : versionToRestore.keySet()) {
				for (String name : versionToRestore.get(drive)) {
					String arr[] = name.split(".v");
					String headFileName = arr[0];
					try {
						if (new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, name).exists()) {
							Files.copy((new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, name)).toPath(), (new File(drive.getBucketObjectDataDirPath(bucket), headFileName)).toPath(),
									StandardCopyOption.REPLACE_EXISTING);
						}
					} catch (IOException e) {
						throw new InternalCriticalException(e, objectInfo(meta));
					}
				}
			}
			return true;

		} catch (InternalCriticalException e) {
			throw e;

		} catch (Exception e) {
			throw new InternalCriticalException(e, objectInfo(meta));
		}
	}

}
