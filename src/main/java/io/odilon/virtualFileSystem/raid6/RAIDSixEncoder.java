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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.odilon.errors.InternalCriticalException;
import io.odilon.file.ParallelFileCoypAgent;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * RAID 6 Reed Solomon encoder
 * </p>
 * <p>
 * Encodes {@link InputStream} into multiple block files in the File System
 * using {@link https://en.wikipedia.org/wiki/Erasure_code}.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixEncoder extends RAIDSixCoder {

    @JsonIgnore
    private long fileSize = 0;

    @JsonIgnore
    private int chunk = 0;

    @JsonIgnore
    private final int data_shards;

    @JsonIgnore
    private final int partiy_shards;

    @JsonIgnore
    private final int total_shards;

    @JsonIgnore
    private RAIDSixBlocks encodedInfo;

    @JsonIgnore
    private List<Drive> r6Drives;

    /**
     * <p>
     * Used by {@link RAIDSixDrive}, can not be created directly.
     * </p>
     */

    protected RAIDSixEncoder(RAIDSixDriver driver) {
        this(driver, null);
    }

    /**
     * <p>
     * We use drivesAll to encode, assuming that drives that are in state
     * {@link DriveStatys.NOT_SYNC} are in the process of becoming enabled (via an
     * async process in {@link RAIDSixDriveSync}.
     * </p>
     * 
     * <p>
     * Used by {@link RAIDSixDrive}, can not be created directly.
     * </p>
     */
    protected RAIDSixEncoder(RAIDSixDriver driver, List<Drive> udrives) {
        super(driver);
        this.r6Drives = (udrives != null) ? udrives : driver.getDrivesAll();
        this.data_shards = getVirtualFileSystemService().getServerSettings().getRAID6DataDrives();
        this.partiy_shards = getVirtualFileSystemService().getServerSettings().getRAID6ParityDrives();
        this.total_shards = data_shards + partiy_shards;
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
     * <b>Block naming convention Head version</b><br/>
     * objectName.[block].[disk]<br/>
     * <br/>
     * <b>Previous version</b><br/>
     * objectName.[block].[disk].v[version]<br/>
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

    public RAIDSixBlocks encodeHead(InputStream is, ServerBucket bucket, String objectName) {
        return encode(is, bucket, objectName, Optional.empty());
    }

    public RAIDSixBlocks encodeVersion(InputStream is, ServerBucket bucket, String objectName, int version) {
        return encode(is, bucket, objectName, Optional.of(version));

    }

    protected RAIDSixBlocks encode(InputStream is, ServerBucket bucket, String objectName, Optional<Integer> version) {

        Check.requireNonNull(is);
        Check.requireNonNull(objectName);
        Check.requireNonNull(bucket);

        if (!getDriver().isConfigurationValid(data_shards, partiy_shards))
            throw new InternalCriticalException("Incorrect configuration for RAID 6 -> data: " + String.valueOf(data_shards)
                    + " | parity:" + String.valueOf(partiy_shards));

        if (getDrives().size() < getTotalShards())
            throw new InternalCriticalException("There are not enough drives to encode the file in RAID 6 -> drives: "
                    + String.valueOf(getDrives().size()) + " | required: " + String.valueOf(total_shards));

        this.fileSize = 0;
        this.chunk = 0;
        this.encodedInfo = new RAIDSixBlocks();

        boolean done = false;

        try (is) {
            while (!done)
                done = encodeChunk(is, bucket, objectName, chunk++, version);
        } catch (Exception e) {
            throw new InternalCriticalException(e, "o:" + objectName);
        }
        this.encodedInfo.setFileSize(this.fileSize);
        return this.encodedInfo;
    }

    /**
     * 
     * @param is
     */
    public boolean encodeChunk(InputStream is, ServerBucket bucket, String objectName, int chunk, Optional<Integer> o_version) {

        // BUFFER 1
        final byte[] allBytes = new byte[ServerConstant.MAX_CHUNK_SIZE];
        int totalBytesRead = 0;
        boolean eof = false;
        try {
            final int maxBytesToRead = ServerConstant.MAX_CHUNK_SIZE - ServerConstant.BYTES_IN_INT;
            boolean done = false;
            int bytesRead = 0;
            while (!done) {
                bytesRead = is.read(allBytes, ServerConstant.BYTES_IN_INT + totalBytesRead, maxBytesToRead - totalBytesRead);
                if (bytesRead > 0)
                    totalBytesRead += bytesRead;
                else
                    eof = true;
                done = eof || (totalBytesRead == maxBytesToRead);
            }
        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }

        if (totalBytesRead == 0)
            return true;

        this.fileSize += totalBytesRead;

        ByteBuffer.wrap(allBytes).putInt(totalBytesRead);

        final int storedSize = totalBytesRead + ServerConstant.BYTES_IN_INT;
        final int shardSize = (storedSize + data_shards - 1) / data_shards;

        // BUFFER 2
        int totalShards = getTotalShards();
        int dataShards = getDataShards();
        int parityShards = getPartityShards();
        
        
        byte[][] shards = new byte[totalShards][shardSize];

        /** Fill in the data shards */
        for (int n = 0; n < dataShards ; n++)
            System.arraycopy(allBytes, n * shardSize, shards[n], 0, shardSize);

        /** Use Reed-Solomon to calculate the parity. */
        ReedSolomon reedSolomon = new ReedSolomon(dataShards, parityShards);
        reedSolomon.encodeParity(shards, 0, shardSize);

        /**
         * Write out the resulting files. zDrives is DrivesEnabled normally, or
         * DrivesAll when it is called from an RaidSixDriveImporter (in the process to
         * become "enabled")
         */

        /**
         * Parallel copy
         */
        List<File> destination = new ArrayList<File>();
        int total = getTotalShards();
        Boolean requiresCopy[] = new Boolean[total];

        for (int diskOrder = 0; diskOrder < getTotalShards(); diskOrder++) {

            if (isWrite(diskOrder)) {
                
                String dirPath = getDrives().get(diskOrder).getBucketObjectDataDirPath(bucket)
                        + ((o_version.isEmpty()) ? "" : (File.separator + VirtualFileSystemService.VERSION_DIR));
                
                String name = objectName + "." + String.valueOf(chunk) + "." + String.valueOf(diskOrder)
                        + (o_version.isEmpty() ? "" : "v." + String.valueOf(o_version.get().intValue()));
                
                destination.add(new File(dirPath, name));
                requiresCopy[diskOrder] = Boolean.valueOf(true);
            } else {
                requiresCopy[diskOrder] = Boolean.valueOf(false);
            }
        }

        /** save in parallel (using the VirtualFileSystem 's ExecutorService) */
        ParallelFileCoypAgent agent = new ParallelFileCoypAgent(shards, destination, requiresCopy);
        agent.setExecutor(getVirtualFileSystemService().getExecutorService());


        boolean isOk = agent.execute();
        
        destination.forEach(file -> this.encodedInfo.getEncodedBlocks().add(file));
        
        if (!isOk)
            throw new InternalCriticalException(objectInfo(bucket, objectName));
        
        return eof;
        
    }
    /**
     * For normal encoding all disk must be written with RS blocks.
     * However, this method is overriden by {@link RAIDSixSDriveSyncEncoder} the class that syncs new disks,
     * this class just writes blocks on the newly installed disks, leaving existing blocks untouched. 
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
