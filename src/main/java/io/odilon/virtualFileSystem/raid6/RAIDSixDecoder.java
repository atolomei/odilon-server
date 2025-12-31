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

	private final int data_shards;
	private final int parity_shards;
	private final int total_shards;

	private final ReedSolomon reedSolomon;

	protected RAIDSixDecoder(RAIDSixDriver driver) {
		super(driver);

		this.data_shards = getVirtualFileSystemService().getServerSettings().getRAID6DataDrives();
		this.parity_shards = getVirtualFileSystemService().getServerSettings().getRAID6ParityDrives();
		this.total_shards = data_shards + parity_shards;

		if (!driver.isConfigurationValid(data_shards, parity_shards))
			throw new InternalCriticalException("Invalid configuration -> " + this.toString());

		reedSolomon = new ReedSolomon(data_shards, parity_shards);

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

		int totalChunks = meta.getTotalBlocks() / getTotalShards();

		if ((meta.getRaidDrives() > 0) && (meta.getRaidDrives() != this.getTotalShards())) {
			String errStr = "b: " + meta.getBucketName() + " o: " + meta.getObjectName() + " | was stored on " + formatter.format(meta.getLastModified()) + " | with  " + String.valueOf(meta.getRaidDrives())
					+ " drives | Server is currently set up with -> " + String.valueOf(getTotalShards()) + " drives";

			logger.error(errStr);
			logger.error("RAID Drives");
			getDriver().getDrivesEnabled().forEach(s -> logger.error(s.getRootDirPath()));
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
					decodeChunk(meta, bucket, chunk++, out, isHead);
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

	private boolean decodeChunk(ObjectMetadata meta, ServerBucket bucket, int chunk, OutputStream out, boolean isHead) {

		final byte[][] shards = new byte[this.total_shards][];
		final boolean[] shardPresent = new boolean[this.total_shards];

		int shardSize = 0;
		int shardCount = 0;

		Map<Integer, Drive> map = this.getMapDrivesRSDecode();

		// 1️⃣ Read available shards
		for (int counter = 0; counter < getTotalShards(); counter++) {

			Drive drive = map.get(counter);
			if (drive == null)
				continue;

			int disk = drive.getConfigOrder();
			File shardFile = isHead ? new File(drive.getBucketObjectDataDirPath(bucket), meta.getObjectName() + "." + chunk + "." + disk)
					: new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, meta.getObjectName() + "." + chunk + "." + disk + ".v" + meta.getVersion());

			if (!shardFile.exists())
				continue;

			shardSize = (int) shardFile.length();
			shards[disk] = new byte[shardSize];
			shardPresent[disk] = true;
			shardCount++;

			try (InputStream in = new BufferedInputStream(new FileInputStream(shardFile))) {
				readFully(in, shards[disk]);
			} catch (IOException e) {
				logger.error(objectInfo(meta) + " | f:" + shardFile.getName(), SharedConstant.NOT_THROWN);
				shardPresent[disk] = false;
				shards[disk] = null;
				shardCount--;
			}
		}

		// Validate quorum
		if (shardCount < this.data_shards) {
			throw new InternalCriticalException("We need at least " + this.data_shards + " shards to reconstruct | " + objectInfo(meta));
		}

		// ️Allocate missing shards
		for (int i = 0; i < this.total_shards; i++) {
			if (!shardPresent[i]) {
				shards[i] = new byte[shardSize];
			}
		}

		// Reconstruct missing shards
		reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

		// Reassemble directly into pooled buffer
		byte[] allBytes = getBullferPoolService().acquire();
		try {
			for (int i = 0; i < this.data_shards; i++) {
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

	/**
	 * @param meta
	 * @param bucket
	 * @param chunk
	 * @param out    note that this OutputStream is not closed by this method.
	 * @param isHead
	 * @return
	 */
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

	private int getTotalShards() {
		return this.total_shards;
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
