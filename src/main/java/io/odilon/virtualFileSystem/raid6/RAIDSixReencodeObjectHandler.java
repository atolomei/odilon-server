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

import io.odilon.encryption.EncryptedResult;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.model.VersionControl;
import io.odilon.util.OdilonFileUtils;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * <b>RAID 6 Re-encode / Integrity Repair handler.</b>
 * </p>
 * <p>
 * Called by {@link RAIDSixDriver#checkIntegrity} when a SHA-256 mismatch is
 * detected between the stored source-file hash and the decoded payload. The
 * handler:
 * <ol>
 * <li>Acquires an <em>object write lock</em> (the caller must have released its
 * read lock first — {@link java.util.concurrent.locks.ReentrantReadWriteLock}
 * does not support lock upgrade).</li>
 * <li>Backs up current shard files so rollback can restore them on failure.</li>
 * <li>Opens a {@code UPDATE_OBJECT} journal entry.</li>
 * <li>Decodes the head version via {@link RAIDSixDecoder#decodeHead}. If
 * decoding fails the shards are too corrupt to recover → journal is rolled
 * back and {@code false} is returned.</li>
 * <li>Re-encodes the payload through {@link RAIDSixEncoder#encodeHead},
 * producing fresh Reed-Solomon shards with correct parity.</li>
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
public class RAIDSixReencodeObjectHandler extends RAIDSixTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDSixReencodeObjectHandler.class.getName());

    protected RAIDSixReencodeObjectHandler(RAIDSixDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    /**
     * Re-encodes the head version of the object from its currently-decoded
     * payload, producing fresh Reed-Solomon shards on the active volume.
     *
     * <p>
     * The caller ({@link RAIDSixDriver#checkIntegrity}) MUST have released both
     * its read locks before calling this method. The method acquires its own
     * write lock internally.
     * </p>
     *
     * @param bucket     owning bucket (non-null)
     * @param objectName object key (non-null)
     * @return {@code true} if shards and metadata were successfully re-encoded
     *         and committed; {@code false} if the object could not be decoded
     *         or the commit failed (original state restored via rollback)
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
                    return false;
                }

                final int headVersion = meta.getVersion();

                // ── 1. Back up current shard files ───────────────────────────────────────
                backupVersionObjectDataFile(meta, bucket, headVersion);

                // ── 2. Open journal operation ─────────────────────────────────────────────
                // We reuse UPDATE_OBJECT so the existing RAIDSixRollbackUpdateHandler
                // can restore from the .v{headVersion} backup files on failure.
                operation = getJournalService().updateObject(bucket, objectName, headVersion);

                // ── 3. Decode current head → cached file (may be in encrypted form) ───────
                RAIDSixDecoder decoder = new RAIDSixDecoder(getDriver());
                File decodedFile;
                try {
                    decodedFile = decoder.decodeHead(meta, bucket);
                } catch (Exception e) {
                    logger.error("Re-encode: decodeHead failed (shards too corrupt) -> "
                            + objectInfo(bucket, objectName) + " | " + e.getMessage(),
                            SharedConstant.NOT_THROWN);
                    // commitOK stays false → rollback in finally
                    return false;
                }

                // ── 4. Re-encode (decrypt → re-encrypt if needed) ──────────────────────
                RAIDSixShards shards;
                try {
                    shards = buildFreshShards(decodedFile, meta, bucket, objectName);
                } catch (Exception e) {
                    isMainException = true;
                    throw new InternalCriticalException(e,
                            "Re-encode: encoder failed -> " + objectInfo(bucket, objectName));
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
                                else
                                    logger.error(objectInfo(bucket, objectName), SharedConstant.NOT_THROWN);
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
     * it, and feeds the result through the RS encoder. Returns a {@link RAIDSixShards}
     * whose shard files are now on the active volume's drives.
     */
    private RAIDSixShards buildFreshShards(File decodedFile, ObjectMetadata meta,
            ServerBucket bucket, String objectName) throws IOException {

        RAIDSixEncoder encoder = new RAIDSixEncoder(getDriver());

        if (isEncrypt()) {
            // decodedFile contains the encrypted bytes; decrypt → re-encrypt → encode
            try (InputStream rawIn = Files.newInputStream(decodedFile.toPath())) {
                InputStream decryptedIn = getEncryptionService().decryptStream(rawIn);
                EncryptedResult er = getEncryptionService().encryptStream(decryptedIn);
                RAIDSixShards shards = encoder.encodeHead(er.getInputStream(), bucket, objectName);
                shards.setSrcFileSize(er.getCountingStream().getCount());
                return shards;
            }
        } else {
            try (InputStream in = Files.newInputStream(decodedFile.toPath())) {
                RAIDSixShards shards = encoder.encodeHead(in, bucket, objectName);
                shards.setSrcFileSize(shards.getFileSize());
                return shards;
            }
        }
    }

    /**
     * Saves re-encoded metadata to the active volume's drives.
     * All fields from the original metadata are preserved; only
     * {@code sha256Blocks}, {@code etag}, {@code length}, {@code sourceLength},
     * and {@code integrityCheck} are updated to reflect the fresh shards.
     * The version number is NOT incremented — this is a repair, not an update.
     */
    private void saveReencodedMetadata(ServerBucket bucket, String objectName,
            ObjectMetadata originalMeta, RAIDSixShards shards) {

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

        final RAIDSixVolume activeVolume = getDriver().getActiveVolume();
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
            repaired.setIntegrityCheck(OffsetDateTime.now());
            repaired.setDrive(drive.getName());
            repaired.setRaidDrives(activeVolume.getTotalShards());
            // Shards now live on the active volume.
            repaired.setVolumeId(activeVolume.getVolumeId());
            list.add(repaired);
        }

        saveRAIDSixObjectMetadataToDisk(drives, list, true);
    }

    /**
     * Copies the current head shard files to the {@link VirtualFileSystemService#VERSION_DIR}
     * sub-directory so they can be restored on rollback.
     */
    private void backupVersionObjectDataFile(ObjectMetadata meta, ServerBucket bucket, int headVersion) {

        Map<Drive, List<String>> map = getDriver().getObjectDataFilesNames(meta, Optional.empty());

        for (Drive drive : map.keySet()) {
            for (String filename : map.get(drive)) {
                File current = new File(drive.getBucketObjectDataDirPath(bucket), filename);
                String suffix = ".v" + headVersion;
                File backupFile = new File(
                        drive.getBucketObjectDataDirPath(bucket)
                                + File.separator + VirtualFileSystemService.VERSION_DIR,
                        filename + suffix);
                try {
                    if (current.exists())
                        Files.copy(current.toPath(), backupFile.toPath(),
                                StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new InternalCriticalException(e,
                            "Re-encode backup: src=" + current.getName()
                                    + " back=" + backupFile.getName());
                }
            }
        }
    }

    /**
     * Removes the backup shard files created by {@link #backupVersionObjectDataFile}
     * after a successful commit. For version-controlled objects the backup is
     * intentionally kept as a prior version.
     */
    private void cleanUpBackup(ObjectMetadata meta, ServerBucket bucket, int version) {
        if (meta == null) return;
        try {
            if (getVersionControl() == VersionControl.DISABLED) {
                for (Drive drive : getDriver().getVolumeForObject(meta).getDrives()) {
                    FileUtils.deleteQuietly(
                            drive.getObjectMetadataVersionFile(bucket, meta.getObjectName(), version));
                }
                List<File> files = getDriver().getObjectDataFiles(meta, bucket, Optional.of(version));
                files.forEach(FileUtils::deleteQuietly);
            }
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    /** Convenience — avoids passing meta.getVersion() repeatedly in finally. */
    private int headVersion(ObjectMetadata meta) {
        return meta != null ? meta.getVersion() : -1;
    }
}
