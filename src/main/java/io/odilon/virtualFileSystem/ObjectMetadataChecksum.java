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
package io.odilon.virtualFileSystem;

import tools.jackson.databind.ObjectMapper;

import io.odilon.model.ObjectMetadata;
import io.odilon.util.OdilonFileUtils;

/**
 * <p>
 * Utility class for computing and verifying the {@code metaChecksum} field of
 * {@link ObjectMetadata}.
 * </p>
 *
 * <p>
 * The checksum is the SHA-256 of the JSON serialization of the metadata record
 * <em>with the {@code metaChecksum} field set to {@code null} before
 * hashing</em>. This makes the checksum stable: the field that stores the
 * checksum is excluded from the computation, so the hash can be written back
 * into the same record without invalidating itself.
 * </p>
 *
 * <p>
 * <b>Backward compatibility</b>: a {@code null} {@code metaChecksum} on a
 * loaded record means the object was written before this feature was
 * introduced. {@link #verify} returns {@code true} in that case — absent
 * checksum is treated as <em>unverified</em>, not corrupt.
 * </p>
 *
 * <p>
 * This class lives in the server module so that {@link ObjectMapper} is not a
 * dependency of {@code odilon-model}.
 * </p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public final class ObjectMetadataChecksum {

    private ObjectMetadataChecksum() { }

    /**
     * Computes the SHA-256 checksum of {@code meta} by temporarily nulling the
     * {@code metaChecksum} field, serializing to JSON bytes, hashing them, and
     * then restoring the original value before returning.
     *
     * <p>
     * The caller must hold the object <em>write lock</em> if the instance is
     * shared across threads, because this method temporarily mutates the object.
     * Inside {@link io.odilon.virtualFileSystem.OdilonDrive#saveObjectMetadata}
     * the lock is always held, so the mutation window is safe.
     * </p>
     *
     * @param meta   the metadata to hash; must not be {@code null}
     * @param mapper the shared {@link ObjectMapper}; must not be {@code null}
     * @return the lowercase hex SHA-256 string
     * @throws Exception if serialization or hashing fails
     */
    public static String compute(ObjectMetadata meta, ObjectMapper mapper) throws Exception {
        String saved = meta.metaChecksum;
        meta.metaChecksum = null;
        try {
            byte[] bytes = mapper.writeValueAsBytes(meta);
            return OdilonFileUtils.calculateSHA256String(bytes);
        } finally {
            meta.metaChecksum = saved; // always restore — object is caller-owned
        }
    }

    /**
     * Verifies the stored checksum against a freshly computed one.
     *
     * <ul>
     *   <li>Returns {@code true} if {@code meta.metaChecksum} is {@code null}
     *       (legacy object — treated as unverified, not corrupt).</li>
     *   <li>Returns {@code true} if the stored checksum matches the computed one.</li>
     *   <li>Returns {@code false} if the checksums differ (metadata is corrupt).</li>
     * </ul>
     *
     * @param meta   the metadata to verify; must not be {@code null}
     * @param mapper the shared {@link ObjectMapper}; must not be {@code null}
     * @return {@code true} if the record is intact or unverified; {@code false} if corrupt
     */
    public static boolean verify(ObjectMetadata meta, ObjectMapper mapper) {
        if (meta.metaChecksum == null)
            return true;  // legacy — absent checksum is not a failure
        try {
            String expected = compute(meta, mapper);
            return expected.equals(meta.metaChecksum);
        } catch (Exception e) {
            return false;
        }
    }
}
