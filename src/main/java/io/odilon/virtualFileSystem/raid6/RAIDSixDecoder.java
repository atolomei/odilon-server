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

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.FileCacheService;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Reed Solomon Erasure Coding decoder for {@link RAIDSixDriver}.<br/>
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
 * <p>
 * <b>Silent-corruption detection and repair (N=6 scope):</b> when every shard
 * file is physically present, the Reed-Solomon {@code decodeMissing()} call
 * short-circuits with a no-op — it never consults the parity shards, so
 * byte-level corruption would pass through silently. To close this gap,
 * {@code decodeChunk} calls {@code isParityCorrect()} after every RS operation.
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
public class RAIDSixDecoder extends RAIDSixCoder {

	static private Logger logger = Logger.getLogger(RAIDSixEncoder.class.getName());

	@JsonIgnore
	static final private DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;

	protected RAIDSixDecoder(RAIDSixDriver driver) {
		super(driver);
		// Validate that the default volume's RS configuration is sane at startup.
		int d = getVirtualFileSystemService().getServerSettings().getRAID6DataDrives();
		int p = getVirtualFileSystemService().getServerSettings().getRAID6ParityDrives();
		if (!driver.isConfigurationValid(d, p))
			throw new InternalCriticalException("Invalid RAID 6 configuration -> data=" + d + " parity=" + p);
	}

	public BufferPoolService getBullferPoolService() {
		return getVirtualFileSystemService().getBufferPoolService();
	}

	public File decodeHead(ObjectMetadata meta, ServerBucket bucket) {
		return decode(meta, bucket, true);
	}

	/**
	 * <p>
	 * {@link ObjectMetadata} must be the one of the version to decode
	 * </p>
	 */
	public File decodeVersion(ObjectMetadata meta, ServerBucket bucket) {
		return decode(meta, bucket, false);
	}

	private File decode(ObjectMetadata meta, ServerBucket bucket, boolean isHead) {

		String bucketName = meta.getBucketName();
		String objectName = meta.getObjectName();

		// Resolve the volume that holds this object's shards.
		// For single-volume deployments (volumeId == 0) this is identical to the
		// previous behaviour; for multi-volume deployments it selects the correct
		// group.
		RAIDSixVolume volume = getDriver().getVolumeForObject(meta);
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
		if ((file != null) && file.exists()) {
			getSystemMonitorService().getCacheFileHitCounter().inc();
			return file;
		}

		getSystemMonitorService().getCacheFileMissCounter().inc();

		getLockService().getFileCacheLock(bucket.getId(), objectName, ver).writeLock().lock();

		try {

			String tempPath = getFileCacheService().getFileCachePath(bucket.getId(), objectName, ver);

			try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tempPath))) {
				while (chunk < totalChunks) {
					decodeChunk(meta, volume, bucket, chunk++, out, isHead);
				}
			} catch (FileNotFoundException e) {
				throw new InternalCriticalException(e, objectInfo(bucketName, objectName, tempPath));
			} catch (IOException e) {
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
	private boolean decodeChunk(ObjectMetadata meta, RAIDSixVolume volume, ServerBucket bucket, int chunk, OutputStream out, boolean isHead) {

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

		// ── Silent-corruption detection ───────────────────────────────────────────
		// When all shard files are present, decodeMissing() exits immediately with a
		// no-op — it never consults the parity shards. We call isParityCorrect()
		// explicitly so that byte-level corruption is caught here rather than served
		// silently to the caller. On failure, attemptCorruptionRepair() isolates bad
		// shards via Phase 1 (single shard) and Phase 2 (pair), which is exhaustive for
		// the supported N=6 RAID-6 configuration (max 2 simultaneous corrupt shards).
		// For larger N, detection still fires but repair is capped at 2; if >2 shards
		// are corrupt an InternalCriticalException is thrown.
		//
		// Enabled via raid6.readParityCheck=true in odilon.properties (default: false).
		// The check costs roughly one parity-encode pass per chunk (~8 GF ops/byte for
		// N=6) and should only be turned on when active read-path corruption detection
		// is required.
		if (shardCount == totalShards && getVirtualFileSystemService().getServerSettings().isRAID6ReadParityCheckEnabled()) {
			long startTime = System.currentTimeMillis();
			boolean pc = rs.isParityCorrect(shards, 0, shardSize);
			if (logger.isDebugEnabled()) {
				logger.debug("Parity check time: " + (System.currentTimeMillis() - startTime) + " ms | " + objectInfo(meta));
			}
			if (!pc) {
				if (logger.isDebugEnabled()) {
					startTime = System.currentTimeMillis();
				}
				attemptCorruptionRepair(meta, volume, bucket, chunk, isHead, shards, shardSize, rs, map);
				if (logger.isDebugEnabled()) {
					logger.debug("Corruption repair time: " + (System.currentTimeMillis() - startTime) + " ms | " + objectInfo(meta));
				}
			}
		}

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
	 * Identifies and repairs corrupt shard(s) when
	 * {@link ReedSolomon#isParityCorrect} has already confirmed that at least one
	 * shard is corrupt (despite all shards being physically present on disk).
	 *
	 * <p>
	 * <b>Phase 1 – single-shard isolation</b> (all N): for each shard index
	 * {@code bad}, mark it absent, let RS reconstruct it from the remaining shards,
	 * then verify parity on the result. If parity passes, {@code bad} was the sole
	 * corrupt shard.
	 * </p>
	 *
	 * <p>
	 * <b>Phase 2 – two-shard-pair isolation</b> (N≥6 only, i.e.
	 * {@code parityDrives≥2}): if Phase 1 finds nothing, iterate every
	 * C(totalShards,2) pair and apply the same logic. Phase 2 is <em>skipped
	 * entirely</em> for N=3 ({@code parityDrives=1}) because removing any two
	 * shards leaves fewer than {@code dataShards} shards, making reconstruction
	 * mathematically impossible.
	 * </p>
	 *
	 * <p>
	 * <b>Scope:</b> repair is intentionally capped at <b>2 simultaneously corrupt
	 * shards</b>. For N=3 (1 parity) the cap is effectively 1. For N=12/24/48
	 * ({@code parityDrives>2}), detection still fires but repair beyond 2 shards is
	 * not attempted; a warn-level log is emitted to make this limitation visible.
	 * </p>
	 *
	 * <p>
	 * On success the repaired bytes are copied back into {@code shards[bad]}
	 * in-place and the corrected shard is written to disk (read-repair) on any
	 * ENABLED drive. On failure an {@link InternalCriticalException} is thrown.
	 * </p>
	 */
	private void attemptCorruptionRepair(ObjectMetadata meta, RAIDSixVolume volume, ServerBucket bucket, int chunk, boolean isHead, byte[][] shards, int shardSize, ReedSolomon rs, Map<Integer, Drive> map) {

		int totalShards = volume.getTotalShards();
		int parityDrives = volume.getParityDrives();

		// Warn operators that this volume's tolerance exceeds our repair cap.
		if (parityDrives > 2) {
			logger.warn("Silent-corruption repair is capped at 2 shards; this volume has " + parityDrives + " parity drives — if more than 2 shards are corrupt " + "the repair will fail | " + objectInfo(meta));
		}

		// All shards were present when this method is called
		boolean[] allPresent = new boolean[totalShards];
		Arrays.fill(allPresent, true);

		// ── Phase 1: single corrupt shard ────────────────────────────────────────
		for (int bad = 0; bad < totalShards; bad++) {
			byte[][] candidate = deepCopyShardsArray(shards, totalShards, shardSize);
			boolean[] candPresent = Arrays.copyOf(allPresent, totalShards);
			candPresent[bad] = false;
			candidate[bad] = new byte[shardSize]; // zeroed buffer for reconstruction

			try {
				rs.decodeMissing(candidate, candPresent, 0, shardSize);
			} catch (Exception e) {
				continue; // shouldn't happen — we always have totalShards-1 shards
			}

			if (rs.isParityCorrect(candidate, 0, shardSize)) {
				logger.warn("Read-repair: silent corruption repaired — shard index " + bad + " | " + objectInfo(meta));
				// Patch the live shards array in-place so the caller assembles clean data
				System.arraycopy(candidate[bad], 0, shards[bad], 0, shardSize);
				writeRepairedShard(meta, volume, bucket, chunk, isHead, bad, shards[bad], map);
				return;
			}
		}

		// ── Phase 2: two simultaneously corrupt shards (N≥6 only) ───────────────
		// For N=3 (parityDrives=1), removing any two shards leaves only 1 shard,
		// which is fewer than dataShards=2 — RS reconstruction is impossible.
		// Attempting it would only produce IllegalArgumentExceptions that get swallowed
		// by the catch below, wasting cycles before reaching the final throw.
		if (parityDrives >= 2) {
			for (int bad1 = 0; bad1 < totalShards - 1; bad1++) {
				for (int bad2 = bad1 + 1; bad2 < totalShards; bad2++) {
					byte[][] candidate = deepCopyShardsArray(shards, totalShards, shardSize);
					boolean[] candPresent = Arrays.copyOf(allPresent, totalShards);
					candPresent[bad1] = false;
					candPresent[bad2] = false;
					candidate[bad1] = new byte[shardSize];
					candidate[bad2] = new byte[shardSize];

					try {
						rs.decodeMissing(candidate, candPresent, 0, shardSize);
					} catch (Exception e) {
						continue;
					}

					if (rs.isParityCorrect(candidate, 0, shardSize)) {
						logger.warn("Read-repair: silent corruption repaired — shard indices " + bad1 + " and " + bad2 + " | " + objectInfo(meta));
						System.arraycopy(candidate[bad1], 0, shards[bad1], 0, shardSize);
						System.arraycopy(candidate[bad2], 0, shards[bad2], 0, shardSize);
						writeRepairedShard(meta, volume, bucket, chunk, isHead, bad1, shards[bad1], map);
						writeRepairedShard(meta, volume, bucket, chunk, isHead, bad2, shards[bad2], map);
						return;
					}
				}
			}
		}

		// Corruption exceeds the 2-shard repair cap (Phase 1 and Phase 2 exhausted).
		// For N=6 (p=2) this means truly unrecoverable corruption.
		// For N=12/24/48 (p>2) repair of 3+ simultaneous corrupt shards is not
		// implemented — detection fires but recovery does not.
		throw new InternalCriticalException("Parity mismatch: silent corruption repair failed — " + "repair is capped at 2 simultaneous corrupt shards"
				+ (parityDrives > 2 ? " (this volume has " + parityDrives + " parity drives but repair supports max 2)" : " (exceeds N=6 RAID-6 tolerance)") + " | " + objectInfo(meta));
	}

	/**
	 * Writes a single shard byte array to its canonical path on {@code localDisk}.
	 * Used by both the standard read-repair path (missing shards) and the
	 * silent-corruption repair path. Silently skips non-ENABLED drives and logs
	 * (but does not propagate) I/O errors so that a repair failure never aborts an
	 * otherwise successful read.
	 */
	private void writeRepairedShard(ObjectMetadata meta, RAIDSixVolume volume, ServerBucket bucket, int chunk, boolean isHead, int localDisk, byte[] data, Map<Integer, Drive> map) {

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
	 */
	private static byte[][] deepCopyShardsArray(byte[][] src, int total, int shardSize) {
		byte[][] copy = new byte[total][];
		for (int i = 0; i < total; i++) {
			if (src[i] != null)
				copy[i] = Arrays.copyOf(src[i], shardSize);
		}
		return copy;
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

	private SystemMonitorService getSystemMonitorService() {
		return getDriver().getVirtualFileSystemService().getSystemMonitorService();
	}

	private String objectInfo(String bucketName, String objectName, String tempPath) {
		return getDriver().objectInfo(bucketName, objectName, tempPath);
	}

	private LockService getLockService() {
		return getFileCacheService().getLockService();
	}
}
