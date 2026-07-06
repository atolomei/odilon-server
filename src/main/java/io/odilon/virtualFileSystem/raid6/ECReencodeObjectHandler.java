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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import io.odilon.encryption.EncryptedResult;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.IntegrityStatus;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.model.VersionControl;
import io.odilon.util.DateTimeUtil;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * <b>Erasure Coding Re-encode / Integrity Repair handler.</b>
 * </p>
 * <p>
 * Called by {@link ECDriver#checkIntegrity} when a SHA-256 mismatch is detected
 * between the stored source-file hash and the decoded payload. The handler:
 * <ol>
 * <li>Acquires an <em>object write lock</em> (the caller must have released its
 * read lock first — {@link java.util.concurrent.locks.ReentrantReadWriteLock}
 * does not support lock upgrade).</li>
 * <li>Backs up current shard files so rollback can restore them on
 * failure.</li>
 * <li>Opens a {@code UPDATE_OBJECT} journal entry.</li>
 * <li>Decodes the head version via {@link ECDecoder#decodeHead}. If decoding
 * fails the shards are too corrupt to recover → journal is rolled back and
 * {@code false} is returned.</li>
 * <li>Re-encodes the payload through {@link ECEncoder#encodeHead}, producing
 * fresh Reed-Solomon shards with correct parity.</li>
 * <li>Saves updated metadata — same object version (no version bump for a
 * repair), new {@code sha256Blocks}, new {@code etag}, refreshed
 * {@code integrityCheck} timestamp.</li>
 * <li>Commits the journal. On any failure the journal is rolled back and the
 * original shards are restored.</li>
 * </ol>
 * </p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class ECReencodeObjectHandler extends ECTransactionObjectHandler {

	private static Logger logger = Logger.getLogger(ECReencodeObjectHandler.class.getName());
	static private Logger checkerLogger = Logger.getLogger("dataIntegrityCheck");

	protected ECReencodeObjectHandler(ECDriver driver, ServerBucket bucket, String objectName) {
		super(driver, bucket, objectName);
	}

	/**
	 * Re-encodes the head version of the object from its currently-decoded payload,
	 * producing fresh Reed-Solomon shards on the active volume.
	 *
	 * <p>
	 * The caller ({@link ECDriver#checkIntegrity}) MUST have released both its read
	 * locks before calling this method. The method acquires its own write lock
	 * internally.
	 * </p>
	 *
	 * @param bucket     owning bucket (non-null)
	 * @param objectName object key (non-null)
	 * @return {@code true} if shards and metadata were successfully re-encoded and
	 *         committed; {@code false} if the object could not be decoded or the
	 *         commit failed (original state restored via rollback)
	 */
	public boolean reencode(ServerBucket bucket, String objectName) {

		VirtualFileSystemOperation operation = null;
		boolean commitOK = false;
		boolean isMainException = false;
		ObjectMetadata meta = null;

		getLockService().getObjectLock(bucket, objectName).writeLock().lock();
		try {

			getLockService().getBucketLock(bucket).readLock().lock();
			try {

				// Cross-volume metadata search (do NOT add to cache — we are about to repair)
				meta = getDriver().getDriverObjectMetadataInternal(bucket, objectName, false);
				if (meta == null) {
					logger.error("Re-encode: metadata not found -> " + objectInfo(bucket, objectName));
					checkerLogger.error("Re-encode: metadata not found -> " + objectInfo(bucket, objectName));

					return false;
				}

				final int headVersion = meta.getVersion();

				// ── 0. Re-verify corruption under the write lock ─────────────────────────
				// The object write lock was not held between checkIntegrity detecting the
				// mismatch and this method acquiring it, so the shard state may have changed
				// (another thread or a prior repair pass may have fixed it already).
				final int parityShards = getDriver().getVolumeForObject(meta).getParityDrives();
				final List<Integer> corruptShardIndices = identifyCorruptShards(meta, bucket);

				if (corruptShardIndices.isEmpty()) {
					// No corruption found — skip the re-encode entirely.
					logger.info("Re-encode: no corruption detected under write lock (already repaired?) -> " + objectInfo(bucket, objectName));

					checkerLogger.info("Re-encode: no corruption detected under write lock (already repaired?) -> " + objectInfo(bucket, objectName));

					return true;
				}

				if (corruptShardIndices.size() > parityShards) {
					// More corrupt shards than parity can recover — irrecoverable.
					// Persist the status so operators can enumerate affected objects
					// without re-running a full scrub pass.
					logger.error("Re-encode: " + corruptShardIndices.size() + " corrupt shard(s) " + corruptShardIndices + " exceed parity capacity (" + parityShards + ") — irrecoverable | " + objectInfo(bucket, objectName));

					checkerLogger.error("Re-encode: " + corruptShardIndices.size() + " corrupt shard(s) " + corruptShardIndices + " exceed parity capacity (" + parityShards + ") — irrecoverable | " + objectInfo(bucket, objectName));

					persistIntegrityStatus(meta, bucket, IntegrityStatus.IRRECOVERABLE);

					return false;
				}

				logger.info("Re-encode: identified " + corruptShardIndices.size() + " corrupt shard(s) " + corruptShardIndices + " (parity capacity=" + parityShards + ") | " + objectInfo(bucket, objectName));

				checkerLogger.info("Re-encode: identified " + corruptShardIndices.size() + " corrupt shard(s) " + corruptShardIndices + " (parity capacity=" + parityShards + ") | " + objectInfo(bucket, objectName));

				// ── 1. Back up current shard files ───────────────────────────────────────
				backupVersionObjectDataFile(meta, bucket, headVersion);

				// ── 2. Open journal operation ─────────────────────────────────────────────
				// We reuse UPDATE_OBJECT so the existing RAIDSixRollbackUpdateHandler
				// can restore from the .v{headVersion} backup files on failure.
				operation = getJournalService().updateObject(bucket, objectName, headVersion);

				// ── 3. Decode current head ────────────────────────────────────────────────
				// Pass the known-corrupt shard indices so decodeChunk marks them as absent
				// (erasures) before calling rs.decodeMissing(). This forces RS to
				// reconstruct those positions from parity + healthy data shards instead of
				// trusting the corrupt bytes — identical to how a full-disk erasure is
				// handled, restoring the full P-shard recovery capacity.
				ECDecoder decoder = new ECDecoder(getDriver());
				File decodedFile;
				try {
					decodedFile = decoder.decodeHead(meta, bucket, corruptShardIndices);
				} catch (Exception e) {
					logger.error("Re-encode: decodeHead failed (shards too corrupt) -> " + objectInfo(bucket, objectName) + " | " + e.getMessage(), SharedConstant.NOT_THROWN);

					checkerLogger.error("Re-encode: decodeHead failed (shards too corrupt) -> " + objectInfo(bucket, objectName) + " | " + e.getMessage(), SharedConstant.NOT_THROWN);

					// commitOK stays false → rollback in finally
					return false;
				}

				// ── 4. Re-encode (decrypt → re-encrypt if needed) ──────────────────────
				ECShards shards;
				try {
					shards = buildFreshShards(decodedFile, meta, bucket, objectName);
				} catch (Exception e) {
					isMainException = true;
					checkerLogger.error("Re-encode: encoder failed -> " + objectInfo(bucket, objectName));
					throw new InternalCriticalException(e, "Re-encode: encoder failed -> " + objectInfo(bucket, objectName));
				}

				// ── 5. Save updated metadata (same version, new hashes) ────────────────
				saveReencodedMetadata(bucket, objectName, meta, shards);

				// ── 6. Commit ──────────────────────────────────────────────────────────
				commitOK = operation.commit();
				return true;

			} catch (Exception e) {
				isMainException = true;
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));

			} finally {
				try {
					if (!commitOK) {
						// Restore backup shards via the journal rollback mechanism
						if (operation != null) {
							try {
								rollback(operation);
							} catch (Exception e) {
								if (!isMainException)
									throw new InternalCriticalException(e, objectInfo(bucket, objectName));
								else {
									checkerLogger.error(objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
									logger.error(objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
								}
							}
						}
					} else {
						cleanUpBackup(meta, bucket, headVersion(meta));
					}
				} finally {
					getLockService().getBucketLock(bucket).readLock().unlock();
				}
			}

		} finally {
			getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
		}
	}

	// ── Private helpers ───────────────────────────────────────────────────────

	/**
	 * Decrypts the cached decoded file (if the object is encrypted), re-encrypts
	 * it, and feeds the result through the RS encoder. Returns a
	 * {@link ECShards} whose shard files are now on the active volume's drives.
	 */
	private ECShards buildFreshShards(File decodedFile, ObjectMetadata meta, ServerBucket bucket, String objectName) throws IOException {

		ECEncoder encoder = new ECEncoder(getDriver());

		if (isEncrypt()) {
			// decodedFile contains the encrypted bytes; decrypt → re-encrypt → encode
			try (InputStream rawIn = Files.newInputStream(decodedFile.toPath())) {
				InputStream decryptedIn = getEncryptionService().decryptStream(rawIn);
				EncryptedResult er = getEncryptionService().encryptStream(decryptedIn);
				ECShards shards = encoder.encodeHead(er.getInputStream(), bucket, objectName);
				shards.setSrcFileSize(er.getCountingStream().getCount());
				return shards;
			}
		} else {
			try (InputStream in = Files.newInputStream(decodedFile.toPath())) {
				ECShards shards = encoder.encodeHead(in, bucket, objectName);
				shards.setSrcFileSize(shards.getFileSize());
				return shards;
			}
		}
	}

	/**
	 * Saves re-encoded metadata to the active volume's drives. All fields from the
	 * original metadata are preserved; only {@code sha256Blocks}, {@code etag},
	 * {@code length}, {@code sourceLength}, and {@code integrityCheck} are updated
	 * to reflect the fresh shards. The version number is NOT incremented — this is
	 * a repair, not an update.
	 */
	private void saveReencodedMetadata(ServerBucket bucket, String objectName, ObjectMetadata originalMeta, ECShards shards) {

		List<String> shaBlocks = new ArrayList<>();
		StringBuilder etag_b = new StringBuilder();

		shards.getEncodedShards().forEach(item -> {
			try {
				shaBlocks.add(OdilonFileUtils.calculateSHA256String(item));
			} catch (Exception e) {
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));
			}
		});
		shaBlocks.forEach(etag_b::append);

		String etag;
		try {
			etag = OdilonFileUtils.calculateSHA256String(etag_b.toString());
		} catch (NoSuchAlgorithmException | IOException e) {
			throw new InternalCriticalException(e, objectInfo(bucket, objectName));
		}

		final ECVolume activeVolume = getDriver().getActiveVolume();
		final List<Drive> drives = activeVolume.getDrives();
		final List<ObjectMetadata> list = new ArrayList<>();

		for (Drive drive : drives) {
			// Start from a clean copy of the original metadata so all fields
			// (contentType, fileName, customTags, publicAccess, etc.) are preserved.
			ObjectMetadata repaired = originalMeta.copy();
			repaired.setSha256Blocks(shaBlocks);
			repaired.setTotalBlocks(shards.getEncodedShards().size());
			repaired.setEtag(etag);
			repaired.setLength(shards.getFileSize());
			repaired.setSourceLength(shards.getSrcFileSize());
			// Stamp the repair time so the next integrity cycle skips this object.
			repaired.setIntegrityCheck(DateTimeUtil.now());
			// Clear any previous IRRECOVERABLE flag — this object is now healthy.
			repaired.setIntegrityStatus(IntegrityStatus.OK);
			repaired.setDrive(drive.getName());
			repaired.setRaidDrives(activeVolume.getTotalShards());
			// Shards now live on the active volume.
			repaired.setVolumeId(activeVolume.getVolumeId());
			repaired.setSha256(shards.getSrcSha256());
			
			list.add(repaired);
		}

		saveRAIDSixObjectMetadataToDisk(drives, list, true);
	}

	/**
	 * Copies the current head shard files to the
	 * {@link VirtualFileSystemService#VERSION_DIR} sub-directory so they can be
	 * restored on rollback.
	 */
	private void backupVersionObjectDataFile(ObjectMetadata meta, ServerBucket bucket, int headVersion) {

		Map<Drive, List<String>> map = getDriver().getObjectDataFilesNames(meta, Optional.empty());

		for (Drive drive : map.keySet()) {
			for (String filename : map.get(drive)) {
				File current = new File(drive.getBucketObjectDataDirPath(bucket), filename);
				String suffix = ".v" + headVersion;
				File backupFile = new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, filename + suffix);
				try {
					if (current.exists())
						Files.copy(current.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				} catch (IOException e) {
					throw new InternalCriticalException(e, "Re-encode backup: src=" + current.getName() + " back=" + backupFile.getName());
				}
			}
		}
	}

	/**
	 * Removes the backup shard files created by
	 * {@link #backupVersionObjectDataFile} after a successful commit. For
	 * version-controlled objects the backup is intentionally kept as a prior
	 * version.
	 */
	private void cleanUpBackup(ObjectMetadata meta, ServerBucket bucket, int version) {
		if (meta == null)
			return;
		try {
			if (getVersionControl() == VersionControl.DISABLED) {
				for (Drive drive : getDriver().getVolumeForObject(meta).getDrives()) {
					FileUtils.deleteQuietly(drive.getObjectMetadataVersionFile(bucket, meta.getObjectName(), version));
				}
				List<File> files = getDriver().getObjectDataFiles(meta, bucket, Optional.of(version));
				files.forEach(FileUtils::deleteQuietly);
			}
		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * Scans every shard file for the head version and returns the de-duplicated,
	 * ordered list of volume-local disk indices whose SHA-256 no longer matches
	 * {@code meta.sha256Blocks}.
	 *
	 * <p>
	 * The mapping between {@code sha256Blocks[i]} and the shard file is:
	 * 
	 * <pre>
	 *   i = chunk * totalShards + localDiskIndex
	 *   file = objectName.chunk.localDiskIndex
	 * </pre>
	 * 
	 * A disk whose ANY chunk is corrupt is added to the result set once — the whole
	 * disk is treated as an erasure during reconstruction.
	 * </p>
	 *
	 * <p>
	 * Must be called while the object write lock is held.
	 * </p>
	 */
	private List<Integer> identifyCorruptShards(ObjectMetadata meta, ServerBucket bucket) {

		List<String> stored = meta.getSha256Blocks();
		if (stored == null || stored.isEmpty())
			return Collections.emptyList();

		ECVolume volume = getDriver().getVolumeForObject(meta);
		int totalShards = volume.getTotalShards();
		int totalChunks = meta.getTotalBlocks() / totalShards;
		Map<Integer, Drive> driveMap = volume.getDrivesRSDecode();

		// LinkedHashSet preserves discovery order and suppresses duplicates —
		// the same local disk index is added at most once even if multiple
		// chunks on that disk are corrupt.
		Set<Integer> corruptDisks = new LinkedHashSet<>();

		int shardIndex = 0;
		outer: for (int chunk = 0; chunk < totalChunks; chunk++) {
			for (int localDisk = 0; localDisk < totalShards; localDisk++) {

				if (shardIndex >= stored.size())
					break outer;

				Drive drive = driveMap.get(localDisk);
				if (drive == null) {
					// Drive absent from the volume map — treat as missing/corrupt
					corruptDisks.add(localDisk);
					shardIndex++;
					continue;
				}

				File shardFile = new File(drive.getBucketObjectDataDirPath(bucket), meta.getObjectName() + "." + chunk + "." + localDisk);

				if (!shardFile.exists()) {
					corruptDisks.add(localDisk);
				} else {
					try {
						String actual = OdilonFileUtils.calculateSHA256String(shardFile);
						if (!actual.equals(stored.get(shardIndex)))
							corruptDisks.add(localDisk);
					} catch (Exception e) {
						// Unreadable shard — treat as corrupt
						corruptDisks.add(localDisk);
					}
				}
				shardIndex++;
			}
		}
		return new ArrayList<>(corruptDisks);
	}

	/**
	 * Writes {@code status} into {@link ObjectMetadata#integrityStatus} on every
	 * drive that holds the object's current metadata, without opening a journal
	 * operation. This is intentionally a best-effort, non-transactional write: the
	 * flag is informational only and does not affect object data. If the write
	 * fails on some drives it is logged and silently ignored — the next scrub pass
	 * will re-evaluate the object anyway.
	 */
	private void persistIntegrityStatus(ObjectMetadata meta, ServerBucket bucket, IntegrityStatus status) {
		if (meta == null)
			return;
		try {
			for (Drive drive : getDriver().getVolumeForObject(meta).getDrives()) {
				try {
					ObjectMetadata onDisk = drive.getObjectMetadata(bucket, meta.getObjectName());
					if (onDisk != null) {
						onDisk.setIntegrityStatus(status);
						drive.saveObjectMetadata(onDisk);
					}
				} catch (Exception e) {
					logger.error("persistIntegrityStatus: could not write status=" + status + " to drive " + drive.getName() + " | " + objectInfo(bucket, meta.getObjectName()), SharedConstant.NOT_THROWN);

					checkerLogger.error("persistIntegrityStatus: could not write status=" + status + " to drive " + drive.getName() + " | " + objectInfo(bucket, meta.getObjectName()), SharedConstant.NOT_THROWN);

				}
			}
		} catch (Exception e) {
			logger.error("persistIntegrityStatus: " + e.getMessage(), SharedConstant.NOT_THROWN);
			checkerLogger.error("persistIntegrityStatus: " + e.getMessage(), SharedConstant.NOT_THROWN);
		}
	}

	/** Convenience — avoids passing meta.getVersion() repeatedly in finally. */
	private int headVersion(ObjectMetadata meta) {
		return meta != null ? meta.getVersion() : -1;
	}
}
