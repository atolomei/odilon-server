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
 * 
 */
package io.odilon.virtualFileSystem.raid6;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.FileCacheService;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Reed Solomon Erasure Coding decoder for {@link ECDriver}.<br/>
 * Files decoded are stored in {@link FileCacheService}. <br/>
 * If the server uses encryption, the cache contains encrypted files.
 * </p>
 * 
 * <p>
 * Read-repair is performed on any missing shard that is reconstructed, but only
 * if the drive is ENABLED. NOTSYNC drives are intentionally skipped, as
 * RAIDSixDriveSync owns their repair.
 * </p>
 *
 *
 * <p>
 * <b>Silent-corruption detection and repair (N=6 scope):</b> when every shard
 * file is physically present, the Reed-Solomon {@code decodeMissing()} call
 * short-circuits with a no-op — it never consults the parity shards, so
 * byte-level corruption would pass through silently. To close this gap,
 * {@code decodeChunk} calls {@code isParityCorrect()} after every RS operation.
 * 
 * If parity fails, {@link #attemptCorruptionRepair} isolates the bad shard(s)
 * in two phases: Phase 1 tries each shard individually (catches 1 corrupt
 * shard), Phase 2 tries every pair (catches 2 simultaneously corrupt shards).
 * <b>Repair is intentionally capped at 2 corrupt shards</b>, which is the full
 * tolerance of the supported N=6 (4 data + 2 parity) RAID-6 configuration.
 * Larger configurations (N=12/24/48) will detect corruption via
 * {@code isParityCorrect} but will only repair up to 2 corrupt shards; if 3 or
 * more shards are corrupt an {@link InternalCriticalException} is thrown rather
 * than silently serving wrong data. Repaired shards are always written back to
 * their ENABLED drives (read-repair).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ECDecoder extends ECCoder {

	static private Logger logger = Logger.getLogger(ECEncoder.class.getName());

	@JsonIgnore
	static final private DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;

	protected ECDecoder(ECDriver driver) {
		super(driver);
		// Validate that the default volume's RS configuration is sane at startup.
		int d = getVirtualFileSystemService().getServerSettings().getECDataDrives();
		int p = getVirtualFileSystemService().getServerSettings().getECParityDrives();
		if (!driver.isConfigurationValid(d, p))
			throw new InternalCriticalException("Invalid ErasureCoding configuration -> data=" + d + " parity=" + p);
	}

	public BufferPoolService getBullferPoolService() {
		return getVirtualFileSystemService().getBufferPoolService();
	}

	public File decodeHead(ObjectMetadata meta, ServerBucket bucket) {
		return decode(meta, bucket, true, null);
	}

	/**
	 * <p>
	 * Decodes the head version treating the given shard indices as absent
	 * (erasures) even if the shard files are physically present on disk.
	 * </p>
	 * <p>
	 * Use this overload when per-shard SHA-256 comparison has already identified
	 * which shards hold corrupt bytes. Converting known errors to erasures restores
	 * the full RS parity-shard recovery capacity:
	 * <ul>
	 * <li>N=3 (P=1): up to 1 erasure</li>
	 * <li>N=6 (P=2): up to 2 erasures</li>
	 * <li>N=12 (P=4): up to 4 erasures</li>
	 * </ul>
	 * </p>
	 *
	 * @param erasureIndices volume-local shard indices (0-based) to treat as
	 *                       absent; must contain at most {@code parityShards}
	 *                       entries
	 */
	public File decodeHead(ObjectMetadata meta, ServerBucket bucket, List<Integer> erasureIndices) {
		return decode(meta, bucket, true, erasureIndices);
	}

	/**
	 * <p>
	 * {@link ObjectMetadata} must be the one of the version to decode
	 * </p>
	 */
	public File decodeVersion(ObjectMetadata meta, ServerBucket bucket) {
		return decode(meta, bucket, false, null);
	}

	private File decode(ObjectMetadata meta, ServerBucket bucket, boolean isHead, List<Integer> erasureIndices) {

		String bucketName = meta.getBucketName();
		String objectName = meta.getObjectName();

		// Resolve the volume that holds this object's shards.
		// For single-volume deployments (volumeId == 0) this is identical to the
		// previous behaviour; for multi-volume deployments it selects the correct
		// group.
		ECVolume volume = getDriver().getVolumeForObject(meta);
		int volumeTotalShards = volume.getTotalShards();

		int totalChunks = meta.getTotalBlocks() / volumeTotalShards;

		if ((meta.getRaidDrives() > 0) && (meta.getRaidDrives() != volumeTotalShards)) {

			String errStr = "b: " + meta.getBucketName() + " o: " + meta.getObjectName() + " | was stored on " + formatter.format(meta.getLastModified()) + " | with  " + String.valueOf(meta.getRaidDrives()) + " drives | Volume "
					+ volume.getVolumeId() + " has -> " + String.valueOf(volumeTotalShards) + " drives";

			logger.error(errStr);
			logger.error("Volume " + volume.getVolumeId() + " drives:");
			volume.getDrives().forEach(s -> logger.error(s.getRootDirPath()));

			throw new InternalCriticalException(errStr);
		}

		Optional<Integer> ver = isHead ? Optional.empty() : Optional.of(Integer.valueOf(meta.getVersion()));
		int chunk = 0;

		File file = getFileCacheService().get(bucket.getId(), objectName, ver);

		/** if the file is in cache, return this file */
		if ((file != null) && file.exists())
			return file;

		getLockService().getFileCacheLock(bucket.getId(), objectName, ver).writeLock().lock();
		try {

			String tempPath = getFileCacheService().getFileCachePath(bucket.getId(), objectName, ver);

			try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tempPath))) {
				while (chunk < totalChunks) {
					decodeChunk(meta, volume, bucket, chunk++, out, isHead, erasureIndices);
				}
			} catch (FileNotFoundException e) {
				logger.error(objectInfo(bucketName, objectName, tempPath) + " | " + e.getMessage(), SharedConstant.THROWN_WRAPPED);
				throw new InternalCriticalException(e, objectInfo(bucketName, objectName, tempPath));

			} catch (IOException e) {
				logger.error(objectInfo(bucketName, objectName, tempPath) + " | " + e.getMessage(), SharedConstant.THROWN_WRAPPED);
				throw new InternalCriticalException(e, objectInfo(bucketName, objectName, tempPath));
			}
			File decodedFile = new File(tempPath);

			getFileCacheService().put(bucket.getId(), objectName, ver, decodedFile, false);
			return decodedFile;

		} finally {
			getLockService().getFileCacheLock(bucket.getId(), objectName, ver).writeLock().unlock();
		}
	}

	/**
	 * Decodes one RS chunk using volume-local disk indices.
	 * <p>
	 * The shard-file naming convention is
	 * {@code objectName.<chunk>.<volumeLocalDiskIndex>} — where
	 * {@code volumeLocalDiskIndex} is the position of the drive inside the volume's
	 * ordered drive list (0 … totalShards−1), <em>not</em> the global
	 * {@link Drive#getConfigOrder()}.
	 * </p>
	 */
	private boolean decodeChunk(ObjectMetadata meta, ECVolume volume, ServerBucket bucket, int chunk, OutputStream out, boolean isHead, List<Integer> erasureIndices) {

		// Use the volume-local drive map (key = volume-local index 0..totalShards-1)
		Map<Integer, Drive> map = volume.getDrivesRSDecode();
		int totalShards = volume.getTotalShards();
		int dataShards = volume.getDataDrives();

		final byte[][] shards = new byte[totalShards][];
		final boolean[] shardPresent = new boolean[totalShards];

		int shardSize = 0;
		int shardCount = 0;

		// Read available shards using volume-local disk index as the shard index
		for (int localDisk = 0; localDisk < totalShards; localDisk++) {

			Drive drive = map.get(localDisk);
			if (drive == null)
				continue;

			// Shard files are named with the volume-local index, NOT configOrder
			File shardFile = isHead ? new File(drive.getBucketObjectDataDirPath(bucket), meta.getObjectName() + "." + chunk + "." + localDisk)
					: new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, meta.getObjectName() + "." + chunk + "." + localDisk + ".v" + meta.getVersion());

			if (!shardFile.exists())
				continue;

			shardSize = (int) shardFile.length();
			shards[localDisk] = new byte[shardSize];
			shardPresent[localDisk] = true;
			shardCount++;

			try (InputStream in = new BufferedInputStream(new FileInputStream(shardFile))) {
				readFully(in, shards[localDisk]);
			} catch (IOException e) {
				logger.error(objectInfo(meta) + " | f:" + shardFile.getName(), SharedConstant.NOT_THROWN);
				shardPresent[localDisk] = false;
				shards[localDisk] = null;
				shardCount--;
			}
		}

		// Convert known-corrupt shard positions to erasures.
		// The caller identified these positions via per-shard SHA-256 comparison
		// before calling decodeHead(). Marking them absent here lets rs.decodeMissing()
		// reconstruct them from parity + healthy data shards — identical to how a
		// full-disk failure is handled — instead of trusting the corrupt bytes.
		if (erasureIndices != null && !erasureIndices.isEmpty()) {
			for (int idx : erasureIndices) {
				if (idx >= 0 && idx < totalShards && shardPresent[idx]) {
					shardPresent[idx] = false;
					shards[idx] = null;
					shardCount--;
				}
			}
		}

		// ── SHA-256 shard validation fast path ────────────────────────────────────
		// When sha256Blocks is available (objects written after per-shard checksums
		// were introduced) we hash each in-memory shard and compare against the stored
		// value. Corrupt shards are converted to erasures so rs.decodeMissing() can
		// reconstruct them from parity in a single RS call — no brute-force needed.
		// Index mapping: sha256Blocks[ chunk * totalShards + localDisk ]
		//
		// Disable via ec.shardChecksumVerify=false when the underlying filesystem
		// (ZFS, Btrfs) already provides block-level integrity — avoids redundant work.
		List<String> storedSha256 = meta.getSha256Blocks();
		boolean hasSha256Blocks = storedSha256 != null && !storedSha256.isEmpty() && storedSha256.size() % totalShards == 0 && getVirtualFileSystemService().getServerSettings().isECShardChecksumVerifyEnabled();

		if (hasSha256Blocks) {

			long startTime = System.nanoTime();

			for (int localDisk = 0; localDisk < totalShards; localDisk++) {
				if (!shardPresent[localDisk])
					continue; // already an erasure — nothing to check
				int sha256Idx = chunk * totalShards + localDisk;
				if (sha256Idx >= storedSha256.size())
					continue;
				String expected = storedSha256.get(sha256Idx);
				if (expected == null || expected.isEmpty())
					continue;
				String actual = computeSha256Hex(shards[localDisk]);
				if (!actual.equalsIgnoreCase(expected)) {
					logger.warn("Silent corruption detected via SHA-256: shard[" + localDisk + "] chunk[" + chunk + "] — converting to erasure for RS reconstruction | " + objectInfo(meta));
					shardPresent[localDisk] = false;
					shards[localDisk] = null;
					shardCount--;
				}
			}

			if (logger.isDebugEnabled()) {
				long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
				logger.debug("SHA-256 shard validation completed in " + elapsedMs + " ms | " + objectInfo(meta));
			}
		}

		// Validate quorum
		if (shardCount < dataShards) {
			throw new InternalCriticalException("We need at least " + dataShards + " shards to reconstruct | " + objectInfo(meta));
		}

		// Allocate missing shards
		for (int i = 0; i < totalShards; i++) {
			if (!shardPresent[i]) {
				shards[i] = new byte[shardSize];
			}
		}

		// Reconstruct missing shards using volume-local RS parameters
		ReedSolomon rs = new ReedSolomon(dataShards, volume.getParityDrives());
		rs.decodeMissing(shards, shardPresent, 0, shardSize);

		// ── Read-repair ───────────────────────────────────────────────────────────
		// Write each reconstructed shard back to its ENABLED drive so that future
		// reads find the file on disk instead of re-reconstructing it every time.
		// NOTSYNC drives are intentionally skipped — RAIDSixDriveSync owns their
		// repair; writing here would interfere with the sync process.
		for (int localDisk = 0; localDisk < totalShards; localDisk++) {
			if (shardPresent[localDisk])
				continue; // shard was already on disk
			writeRepairedShard(meta, volume, bucket, chunk, isHead, localDisk, shards[localDisk], map);
		}

		// Reassemble directly into pooled buffer
		byte[] allBytes = getBullferPoolService().acquire();
		try {
			for (int i = 0; i < dataShards; i++) {
				System.arraycopy(shards[i], 0, allBytes, i * shardSize, shardSize);
			}
			// Read payload size (no ByteBuffer)
			int fileSize = ((allBytes[0] & 0xFF) << 24) | ((allBytes[1] & 0xFF) << 16) | ((allBytes[2] & 0xFF) << 8) | (allBytes[3] & 0xFF);

			// ── Sanity-check ───────────────────────────────────────────────────────────
			// A corrupt shard causes rs.decodeMissing() to produce garbage bytes, which
			// makes the fileSize field meaningless (negative, or absurdly large).
			// Detecting this here turns silent data corruption into a clear exception.
			int maxDataBytes = shardSize * dataShards - ServerConstant.BYTES_IN_INT;
			if (fileSize < 0 || fileSize > maxDataBytes) {
				throw new InternalCriticalException("Corrupt RS reconstruction: decoded fileSize=" + fileSize + " is out of range [0, " + maxDataBytes + "]" + " — likely caused by a corrupt shard | " + objectInfo(meta));
			}

			// Write payload
			out.write(allBytes, ServerConstant.BYTES_IN_INT, fileSize);

		} catch (IOException e) {
			throw new InternalCriticalException(e, objectInfo(meta));
		} finally {
			getBullferPoolService().release(allBytes);
		}

		return true;
	}

	/**
	 * Writes a single shard byte array to its canonical path on {@code localDisk}.
	 * Used by both the standard read-repair path (missing shards) and the
	 * silent-corruption repair path. Silently skips non-ENABLED drives and logs
	 * (but does not propagate) I/O errors so that a repair failure never aborts an
	 * otherwise successful read.
	 */
	private void writeRepairedShard(ObjectMetadata meta, ECVolume volume, ServerBucket bucket, int chunk, boolean isHead, int localDisk, byte[] data, Map<Integer, Drive> map) {

		Drive repairDrive = map.get(localDisk);
		if (repairDrive == null)
			return;
		if (repairDrive.getDriveInfo() == null || repairDrive.getDriveInfo().getStatus() != DriveStatus.ENABLED)
			return;

		File repairFile = isHead ? new File(repairDrive.getBucketObjectDataDirPath(bucket), meta.getObjectName() + "." + chunk + "." + localDisk)
				: new File(repairDrive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, meta.getObjectName() + "." + chunk + "." + localDisk + ".v" + meta.getVersion());

		try {
			File parentDir = repairFile.getParentFile();
			if (parentDir != null && !parentDir.exists())
				parentDir.mkdirs();
			try (OutputStream repairOut = new BufferedOutputStream(new FileOutputStream(repairFile))) {
				repairOut.write(data);
			}
			logger.info("Read-repair: wrote shard -> " + repairFile.getAbsolutePath() + " | " + objectInfo(meta));
		} catch (IOException e) {
			// Log but do NOT fail the read — the object decoded successfully.
			// The shard will simply be reconstructed/repaired again on the next read.
			logger.error("Read-repair: failed to write shard -> " + repairFile.getAbsolutePath() + " | " + e.getMessage(), SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * Creates an independent deep copy of a shard array so that isolation-repair
	 * attempts can zero-out individual entries without disturbing the original
	 * data.
	 * 
	 * private static byte[][] deepCopyShardsArray(byte[][] src, int total, int
	 * shardSize) { byte[][] copy = new byte[total][]; for (int i = 0; i < total;
	 * i++) { if (src[i] != null) copy[i] = Arrays.copyOf(src[i], shardSize); }
	 * return copy; }
	 */

	/**
	 * Returns the lowercase hex SHA-256 digest of {@code data}. A fresh
	 * {@link MessageDigest} instance is used each time (not thread-safe).
	 */
	private static String computeSha256Hex(byte[] data) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] digest = md.digest(data);
			StringBuilder sb = new StringBuilder(64);
			for (byte b : digest)
				sb.append(String.format("%02x", b & 0xFF));
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new InternalCriticalException(e, "SHA-256 not available");
		}
	}

	private static void readFully(InputStream in, byte[] buffer) throws IOException {

		int offset = 0;
		int remaining = buffer.length;

		while (remaining > 0) {
			int read = in.read(buffer, offset, remaining);
			if (read < 0)
				throw new EOFException("Unexpected EOF");
			offset += read;
			remaining -= read;
		}
	}

	private String objectInfo(String bucketName, String objectName, String tempPath) {
		return getDriver().objectInfo(bucketName, objectName, tempPath);
	}

	private LockService getLockService() {
		return getFileCacheService().getLockService();
	}
}
