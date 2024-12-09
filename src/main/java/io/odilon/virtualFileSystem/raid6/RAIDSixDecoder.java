/*
 * Odilon Object Storage
 * (C) Novamens 
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseObject;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * 
 * <p>
 * Reed Solomon erasure coding decoder for {@link RAIDSixDriver}.<br/>
 * Files decoded are stored in {@link FileCacheService}. <br/>
 * If the server uses encryption, the cache contains encrypted files
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixDecoder extends BaseObject {

    static private Logger logger = Logger.getLogger(RAIDSixEncoder.class.getName());

    @JsonIgnore
    private RAIDSixDriver driver;

    private final int data_shards;

    private final int parity_shards;

    private final int total_shards;

    protected RAIDSixDecoder(RAIDSixDriver driver) {
        Check.requireNonNull(driver);
        this.driver = driver;
        this.data_shards = getVirtualFileSystemService().getServerSettings().getRAID6DataDrives();
        this.parity_shards = getVirtualFileSystemService().getServerSettings().getRAID6ParityDrives();
        this.total_shards = data_shards + parity_shards;

        if (!driver.isConfigurationValid(data_shards, parity_shards))
            throw new InternalCriticalException("Invalid configuration -> " + this.toString());
    }

    public File decodeHead(ObjectMetadata meta) {
        return decode(meta, true);
    }

    /**
     * <p>
     * {@link ObjectMetadata} must be the one of the version to decode
     * </p>
     * 
     */
    public File decodeVersion(ObjectMetadata meta) {
        return decode(meta, false);
    }

    public RAIDSixDriver getDriver() {
        return this.driver;
    }

    public VirtualFileSystemService getVirtualFileSystemService() {
        return this.driver.getVirtualFileSystemService();
    }

    private File decode(ObjectMetadata meta, boolean isHead) {

        Long bucketId = meta.getBucketId();
        String bucketName = meta.getBucketName();
        String objectName = meta.getObjectName();

        int totalChunks = meta.getTotalBlocks() / this.getTotalShards();

        Optional<Integer> ver = isHead ? Optional.empty() : Optional.of(Integer.valueOf(meta.getVersion()));
        int chunk = 0;

        File file = getVirtualFileSystemService().getFileCacheService().get(bucketId, objectName, ver);

        if (file != null) {
            getDriver().getVirtualFileSystemService().getSystemMonitorService().getCacheFileHitCounter().inc();
            return file;
        }
        getDriver().getVirtualFileSystemService().getSystemMonitorService().getCacheFileMissCounter().inc();
        getVirtualFileSystemService().getFileCacheService().getLockService().getFileCacheLock(bucketId, objectName, ver)
                .writeLock().lock();

        try {
            String tempPath = getVirtualFileSystemService().getFileCacheService().getFileCachePath(bucketId, objectName,
                    ver);

            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tempPath))) {
                while (chunk < totalChunks) {
                    decodeChunk(meta, chunk++, out, isHead);
                }
            } catch (FileNotFoundException e) {
                throw new InternalCriticalException(e, getDriver().objectInfo(bucketName, objectName, tempPath));
            } catch (IOException e) {
                throw new InternalCriticalException(e, getDriver().objectInfo(bucketName, objectName, tempPath));
            }
            File decodedFile = new File(tempPath);
            getVirtualFileSystemService().getFileCacheService().put(bucketId, objectName, ver, decodedFile, false);
            return decodedFile;

        } finally {
            getVirtualFileSystemService().getFileCacheService().getLockService()
                    .getFileCacheLock(bucketId, objectName, ver).writeLock().unlock();
        }
    }

    /**
     * @param bucketName
     * @param objectName
     * @param chunk
     * @param out
     * @return
     */
    private boolean decodeChunk(ObjectMetadata meta, int chunk, OutputStream out, boolean isHead) {

        // Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)

        final byte[][] shards = new byte[this.total_shards][]; // BUFFER 3
        final boolean[] shardPresent = new boolean[this.total_shards];

        int shardSize = 0;
        int shardCount = 0;

        for (int disk = 0; disk < this.total_shards; disk++) {

            /**
             * encode -> DrivesAll, 
             * decode -> DrivesEnabled
             */

            File shardFile = null;
            Drive drive = this.getMapDrivesRSDecode().get(Integer.valueOf(disk));

            if (drive != null) {
                shardFile = (isHead)
                        ? (new File(drive.getBucketObjectDataDirPath(meta.getBucketId()),
                                meta.getObjectName() + "." + String.valueOf(chunk) + "." + String.valueOf(disk)))
                        : (new File(
                                drive.getBucketObjectDataDirPath(meta.getBucketId()) + File.separator
                                        + VirtualFileSystemService.VERSION_DIR,
                                meta.getObjectName() + "." + String.valueOf(chunk) + "." + String.valueOf(disk) + ".v"
                                        + String.valueOf(meta.getVersion())));
            }

            if ((shardFile != null) && (shardFile.exists())) {
                shardSize = (int) shardFile.length();
                shards[disk] = new byte[shardSize]; // BUFFER 4
                shardPresent[disk] = true;
                shardCount += 1;
                try (InputStream in = new BufferedInputStream(new FileInputStream(shardFile))) {
                    in.read(shards[disk], 0, shardSize);
                } catch (FileNotFoundException e) {
                    logger.error(
                            getDriver().objectInfo(meta) + " | f:" + shardFile.getName()
                                    + (isHead ? "" : (" v:" + String.valueOf(meta.getVersion()))),
                            SharedConstant.NOT_THROWN);
                    shardPresent[disk] = false;
                } catch (IOException e) {
                    logger.error(
                            getDriver().objectInfo(meta) + " | f:" + shardFile.getName()
                                    + (isHead ? "" : (" v:" + String.valueOf(meta.getVersion()))),
                            SharedConstant.NOT_THROWN);
                    shardPresent[disk] = false;
                }
            }
        }

        /** We need at least DATA_SHARDS to be able to reconstruct the file */
        if (shardCount < this.data_shards) {
            throw new InternalCriticalException("We need at least " + String.valueOf(this.data_shards)
                    + " shards to be able to reconstruct the data file | " + objectInfo(meta) + " | f:"
                    + (isHead ? "" : (" v:" + String.valueOf(meta.version))) + " | shardCount: "
                    + String.valueOf(shardCount));
        }

        /** Make empty buffers for the missing shards */
        for (int i = 0; i < this.total_shards; i++) {
            if (!shardPresent[i]) {
                shards[i] = new byte[shardSize]; // BUFFER 5
            }
        }

        /** Use Reed-Solomon to fill in the missing shards */
        ReedSolomon reedSolomon = new ReedSolomon(this.data_shards, this.parity_shards);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        /**
         * Combine the data shards into one buffer for convenience. TBA: we have to
         * change this to improve performance
         */
        byte[] allBytes = new byte[shardSize * this.data_shards]; // BUFFER 6
        for (int i = 0; i < this.data_shards; i++)
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);

        /** Extract the file length */
        int fileSize = ByteBuffer.wrap(allBytes).getInt();

        try {
            out.write(allBytes, ServerConstant.BYTES_IN_INT, fileSize);
        } catch (IOException e) {
            throw new InternalCriticalException(e, "decodeChunk | " + objectInfo(meta));
        }
        return true;
    }

    private final Map<Integer, Drive> getMapDrivesRSDecode() {
        return getDriver().getVirtualFileSystemService().getMapDrivesRSDecode();
    }

    private int getTotalShards() {
        return this.total_shards;
    }

    private String objectInfo(ObjectMetadata meta) {
        return getDriver().objectInfo(meta);
    }

}
