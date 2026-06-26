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
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Reed Solomon erasure coding decoder for {@link RAIDSixDriver}.<br/>
 * Files decoded are stored in {@link FileCacheService}. <br/>
 * If the server uses encryption, the cache contains encrypted files.
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
		// previous behaviour; for multi-volume deployments it selects the correct group.
		RAIDSixVolume volume = getDriver().getVolumeForObject(meta);
		int volumeTotalShards = volume.getTotalShards();

		int totalChunks = meta.getTotalBlocks() / volumeTotalShards;

		if ((meta.getRaidDrives() > 0) && (meta.getRaidDrives() != volumeTotalShards)) {
			String errStr = "b: " + meta.getBucketName() + " o: " + meta.getObjectName() + " | was stored on " + formatter.format(meta.getLastModified()) + " | with  " + String.valueOf(meta.getRaidDrives())
					+ " drives | Volume " + volume.getVolumeId() + " has -> " + String.valueOf(volumeTotalShards) + " drives";

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
	 * {@code volumeLocalDiskIndex} is the position of the drive inside the
	 * volume's ordered drive list (0 … totalShards−1), <em>not</em> the global
	 * {@link Drive#getConfigOrder()}.
	 * </p>
	 */
	private boolean decodeChunk(ObjectMetadata meta, RAIDSixVolume volume, ServerBucket bucket, int chunk, OutputStream out, boolean isHead) {

		// Use the volume-local drive map (key = volume-local index 0..totalShards-1)
		Map<Integer, Drive> map = volume.getDrivesRSDecode();
		int totalShards = volume.getTotalShards();
		int dataShards  = volume.getDataDrives();

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
			File shardFile = isHead
					? new File(drive.getBucketObjectDataDirPath(bucket),
							meta.getObjectName() + "." + chunk + "." + localDisk)
					: new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR,
							meta.getObjectName() + "." + chunk + "." + localDisk + ".v" + meta.getVersion());

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

		// Reassemble directly into pooled buffer
		byte[] allBytes = getBullferPoolService().acquire();
		try {
			for (int i = 0; i < dataShards; i++) {
				System.arraycopy(shards[i], 0, allBytes, i * shardSize, shardSize);
			}
			// Read payload size (no ByteBuffer)
			int fileSize = ((allBytes[0] & 0xFF) << 24) | ((allBytes[1] & 0xFF) << 16) | ((allBytes[2] & 0xFF) << 8) | (allBytes[3] & 0xFF);

			// Write payload
			out.write(allBytes, ServerConstant.BYTES_IN_INT, fileSize);

		} catch (IOException e) {
			throw new InternalCriticalException(e, objectInfo(meta));
		} finally {
			getBullferPoolService().release(allBytes);
		}

		return true;
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

	private final Map<Integer, Drive> getMapDrivesRSDecode() {
		return getDriver().getVirtualFileSystemService().getMapDrivesRSDecode();
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

/**
 * private boolean decodeChunkV2(ObjectMetadata meta, ServerBucket bucket, int
 * chunk, OutputStream out, boolean isHead) {
 * 
 * 
 * final byte[][] shards = new byte[this.total_shards][]; // BUFFER 3 final
 * boolean[] shardPresent = new boolean[this.total_shards];
 * 
 * for (int i = 0; i < shardPresent.length; i++) shardPresent[i] = false;
 * 
 * int shardSize = 0; int shardCount = 0;
 * 
 * Map<Integer, Drive> map = this.getMapDrivesRSDecode();
 * 
 * for (int counter = 0; counter < getTotalShards(); counter++) {
 * 
 * 
 * File shardFile = null;
 * 
 * Drive drive = map.get(Integer.valueOf(counter));
 * 
 * if (drive != null) { int disk = drive.getConfigOrder();
 * 
 * shardFile = (isHead) ? (new File(drive.getBucketObjectDataDirPath(bucket),
 * meta.getObjectName() + "." + String.valueOf(chunk) + "." +
 * String.valueOf(disk))) : (new File(drive.getBucketObjectDataDirPath(bucket) +
 * File.separator + VirtualFileSystemService.VERSION_DIR, meta.getObjectName() +
 * "." + String.valueOf(chunk) + "." + String.valueOf(disk) + ".v" +
 * String.valueOf(meta.getVersion()))); }
 * 
 * if ((shardFile != null) && (shardFile.exists())) {
 * 
 * int disk = drive.getConfigOrder(); shardSize = (int) shardFile.length();
 * shards[disk] = new byte[shardSize]; // BUFFER 4 shardPresent[disk] = true;
 * shardCount += 1;
 * 
 * try (InputStream in = new BufferedInputStream(new
 * FileInputStream(shardFile))) { in.read(shards[disk], 0, shardSize); } catch
 * (FileNotFoundException e) { logger.error(getDriver().objectInfo(meta) + " |
 * f:" + shardFile.getName() + (isHead ? "" : (" v:" +
 * String.valueOf(meta.getVersion()))), SharedConstant.NOT_THROWN);
 * shardPresent[disk] = false; } catch (IOException e) {
 * logger.error(objectInfo(meta) + " | f:" + shardFile.getName() + (isHead ? ""
 * : (" v:" + String.valueOf(meta.getVersion()))), SharedConstant.NOT_THROWN);
 * shardPresent[disk] = false; } } }
 * 
 * 
 * if (shardCount < this.data_shards) { throw new InternalCriticalException("We
 * need at least " + String.valueOf(this.data_shards) + " shards to be able to
 * reconstruct the data file | " + objectInfo(meta) + " | f:" + (isHead ? "" :
 * (" v:" + String.valueOf(meta.version))) + " | shardCount: " +
 * String.valueOf(shardCount)); }
 * 
 * 
 * for (int i = 0; i < this.total_shards; i++) { if (!shardPresent[i]) shards[i]
 * = new byte[shardSize]; // BUFFER 5 }
 * 
 * 
 * ReedSolomon reedSolomon = new ReedSolomon(this.data_shards,
 * this.parity_shards);
 * 
 * reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);
 * 
 * byte[] allBytes = new byte[shardSize * this.data_shards]; // BUFFER 6 for
 * (int i = 0; i < this.data_shards; i++) System.arraycopy(shards[i], 0,
 * allBytes, shardSize * i, shardSize);
 * 
 * 
 * int fileSize = ByteBuffer.wrap(allBytes).getInt();
 * 
 * try { out.write(allBytes, ServerConstant.BYTES_IN_INT, fileSize); } catch
 * (IOException e) { throw new InternalCriticalException(e, objectInfo(meta)); }
 * return true; }
 */
