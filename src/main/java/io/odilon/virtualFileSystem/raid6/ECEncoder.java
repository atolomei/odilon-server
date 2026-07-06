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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.odilon.errors.InternalCriticalException;
import io.odilon.file.ParallelFileCoypAgent;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * ErasureCoding Reed Solomon encoder
 * </p>
 * <p>
 * Encodes {@link InputStream} into multiple block files in the File System
 * using {@link https://en.wikipedia.org/wiki/Erasure_code}. A chunk/stripe is a
 * portion of the file that is encoded into multiple shards (data and parity).
 * Each shard is stored in a different disk.
 * 
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ECEncoder extends ECCoder {

	private static Logger logger = Logger.getLogger(ECEncoder.class.getName());

	@JsonIgnore
	private long fileSize = 0;

	@JsonIgnore
	private int stripe = 0;

	@JsonIgnore
	private final int data_shards;

	@JsonIgnore
	private final int partiy_shards;

	@JsonIgnore
	private final int total_shards;

	@JsonIgnore
	private ECShards encodedInfo;

	@JsonIgnore
	private List<Drive> r6Drives;

	@JsonIgnore
	private final ReedSolomon reedSolomon;

	/**
	 * <p>
	 * Used by {@link RAIDSixDrive}, can not be created directly.
	 * </p>
	 */

	protected ECEncoder(ECDriver driver) {
		this(driver, null);
	}

	/**
	 * <p>
	 * We use drivesAll to encode, assuming that drives that are in state
	 * {@link DriveStatys.NOT_SYNC} are in the process of becoming enabled (via an
	 * async process in {@link ECDriveSync}.
	 * </p>
	 * 
	 * <p>
	 * Used by {@link RAIDSixDrive}, can not be created directly.
	 * </p>
	 */
	protected ECEncoder(ECDriver driver, List<Drive> udrives) {
		super(driver);
		// When no explicit drive list is given, use the active volume's drive list
		// so that new objects are always written to the currently-active volume.
		// If the VolumeManager is unavailable (e.g. during drive-sync bootstrap)
		// we fall back to the global drivesAll list.
		if (udrives != null) {
			this.r6Drives = udrives;
		} else {
			try {
				ECVolume activeVolume = driver.getActiveVolume();
				this.r6Drives = activeVolume.getDrives();
			} catch (Exception e) {
				// fallback: volume manager not yet initialized (e.g. drive sync bootstrap)
				this.r6Drives = driver.getDrivesAll();
			}
		}
		this.data_shards = getVirtualFileSystemService().getServerSettings().getECDataDrives();
		this.partiy_shards = getVirtualFileSystemService().getServerSettings().getECParityDrives();
		this.total_shards = data_shards + partiy_shards;
		this.reedSolomon = new ReedSolomon(getDataShards(), getPartityShards());
	}

	/**
	 * <p>
	 * We can not use the {@link ObjectMetadata} here because <b>it may not exist
	 * yet</b>. The steps to upload objects are: <br/>
	 * <ul>
	 * <li>upload binary data</li>
	 * <li>create ObjectMetadata</li>
	 * </ul>
	 * </p>
	 * 
	 * <p>
	 * <b>Shard naming convention Head version</b><br/>
	 * objectName.[shard].[disk]<br/>
	 * <br/>
	 * <b>Previous version</b><br/>
	 * objectName.[shard].[disk].v[version]<br/>
	 * </p>
	 * 
	 * @param is
	 * @param bucketId
	 * @param objectName
	 * 
	 * @return the size of the source file in bytes (note that the disk used by the
	 *         shards is more (16 bytes for the file size plus the padding to make
	 *         every shard multiple of 4)
	 */

	public ECShards encodeHead(InputStream is, ServerBucket bucket, String objectName) {
		return encode(is, bucket, objectName, Optional.empty());
	}

	public ECShards encodeVersion(InputStream is, ServerBucket bucket, String objectName, int version) {
		return encode(is, bucket, objectName, Optional.of(version));

	}

	protected ECShards encode(InputStream is, ServerBucket bucket, String objectName, Optional<Integer> version) {

		Check.requireNonNull(is);
		Check.requireNonNull(objectName);
		Check.requireNonNull(bucket);

		if (!getDriver().isConfigurationValid(data_shards, partiy_shards))
			throw new InternalCriticalException("Incorrect configuration for ErasureCoding -> data: " + String.valueOf(data_shards) + " | parity:" + String.valueOf(partiy_shards));

		if (getDrives().size() < getTotalShards())
			throw new InternalCriticalException("There are not enough drives to encode the file in ErasureCoding -> drives: " + String.valueOf(getDrives().size()) + " | required: " + String.valueOf(total_shards));

		this.fileSize = 0;
		this.stripe = 0;
		this.encodedInfo = new ECShards();

		boolean done = false;

		try (is) {
			while (!done)
				done = encodeStripe(is, bucket, objectName, stripe++, version);
		} catch (Exception e) {
			logger.error(e, "Error encoding object -> " + objectName, SharedConstant.THROWN_WRAPPED);
			throw new InternalCriticalException(e, "o:" + objectName);
		}
		this.encodedInfo.setFileSize(this.fileSize);
		return this.encodedInfo;
	}

	public BufferPoolService getBullferPoolService() {
		return getVirtualFileSystemService().getBufferPoolService();
	}

	public boolean encodeStripe(InputStream is, ServerBucket bucket, String objectName, int nStripe, Optional<Integer> o_version) {

		byte[] allBytes = getBullferPoolService().acquire();
		boolean eof = false;

		try {
			int totalBytesRead = 0;
			final int maxBytesToRead = ServerConstant.MAX_STRIPE_SIZE - ServerConstant.BYTES_IN_INT;

			while (totalBytesRead < maxBytesToRead) {
				int bytesRead = is.read(allBytes, ServerConstant.BYTES_IN_INT + totalBytesRead, maxBytesToRead - totalBytesRead);
				if (bytesRead < 0) {
					eof = true;
					break;
				}
				totalBytesRead += bytesRead;
			}

			if (totalBytesRead == 0)
				return true;

			this.fileSize += totalBytesRead;

			// write size header manually (no ByteBuffer allocation)
			allBytes[0] = (byte) (totalBytesRead >>> 24);
			allBytes[1] = (byte) (totalBytesRead >>> 16);
			allBytes[2] = (byte) (totalBytesRead >>> 8);
			allBytes[3] = (byte) totalBytesRead;

			final int storedSize = totalBytesRead + ServerConstant.BYTES_IN_INT;
			final int shardSize = (storedSize + data_shards - 1) / data_shards;

			final int totalShards = getTotalShards();
			final int dataShards = getDataShards();

			// Allocate shard arrays (can be pooled later if needed)
			byte[][] shards = new byte[totalShards][shardSize];

			// Fill data shards
			for (int n = 0; n < dataShards; n++) {
				System.arraycopy(allBytes, n * shardSize, shards[n], 0, shardSize);
			}

			// Encode parity
			this.reedSolomon.encodeParity(shards, 0, shardSize);

			// Prepare destinations
			List<File> destination = new ArrayList<>(totalShards);
			Boolean[] requiresCopy = new Boolean[totalShards];

			for (int diskOrder = 0; diskOrder < totalShards; diskOrder++) {
				if (isWrite(diskOrder)) {

					String dirPath = getDrives().get(diskOrder).getBucketObjectDataDirPath(bucket) + (o_version.isEmpty() ? "" : File.separator + VirtualFileSystemService.VERSION_DIR);

					// Last-resort guard: ensure the target directory exists before handing the
					// File to ParallelFileCoypAgent. RAIDSixDriveSetup.createBuckets() should
					// have created it during drive-setup, but if that step was incomplete
					// (e.g. after a partial previous sync run) the FileOutputStream inside
					// ParallelFileCoypAgent throws FileNotFoundException with a misleading
					// "No such file or directory" message that obscures the real root cause.
					File dir = new File(dirPath);
					if (!dir.exists() && !dir.mkdirs())
						throw new InternalCriticalException("Cannot create shard directory: " + dirPath + " | d:" + getDrives().get(diskOrder).getName());

					String name = objectName + "." + nStripe + "." + diskOrder + (o_version.isEmpty() ? "" : ".v" + o_version.get());

					destination.add(new File(dirPath, name));
					requiresCopy[diskOrder] = Boolean.TRUE;
				} else {
					requiresCopy[diskOrder] = Boolean.FALSE;
				}
			}

			ParallelFileCoypAgent agent = new ParallelFileCoypAgent(shards, destination, requiresCopy);

			agent.setExecutor(getVirtualFileSystemService().getExecutorService());

			if (!agent.execute()) {
				throw new InternalCriticalException(objectInfo(bucket, objectName));
			}

			destination.forEach(f -> this.encodedInfo.getEncodedShards().add(f));

			return eof;

		} catch (IOException e) {
			logger.error(e, "Error encoding object -> " + objectName, SharedConstant.THROWN_WRAPPED);
			throw new InternalCriticalException(e, objectInfo(bucket, objectName));
		} finally {
			getBullferPoolService().release(allBytes);
		}
	}

	/**
	 * For normal encoding all disk must be written with RS blocks. However, this
	 * method is overriden by {@link ECDriveSyncEncoder} the class that syncs new
	 * disks, which just writes blocks on the newly installed disks, leaving
	 * existing blocks untouched.
	 * 
	 * @param diskOrder
	 * @return
	 */
	protected boolean isWrite(int diskOrder) {
		return true;
	}

	protected List<Drive> getDrives() {
		return r6Drives;
	}

	private int getTotalShards() {
		return this.total_shards;
	}

	private int getDataShards() {
		return this.data_shards;
	}

	private int getPartityShards() {
		return this.partiy_shards;
	}
}
