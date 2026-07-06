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

import io.odilon.log.Logger;
import io.odilon.model.IntegrityStatus;
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
 * <b>Absent-checksum compatibility</b>: a {@code null} {@code metaChecksum} on a
 * loaded record means the object was written before this feature was
 * introduced. {@link #verify} returns {@code true} in that case — absent
 * checksum is treated as <em>unverified</em>, not corrupt.
 * </p>
 *
 * <p>
 * <b>Schema-evolution compatibility</b>: when a new field with a non-null
 * default is added to {@link ObjectMetadata} <em>after</em> {@code metaChecksum}
 * was introduced, objects stored before that addition carry a stored checksum
 * that does not include the new field.  {@link #verify} handles this via a
 * <em>legacy retry</em> that temporarily sets {@code integrityStatus} to
 * {@code null} before hashing, reproducing the JSON that was on disk when the
 * checksum was first written ({@code @JsonInclude(NON_NULL)} excludes null
 * fields from serialization).  If the retry matches, the object is treated as
 * valid; its checksum is refreshed automatically on the next
 * {@code OdilonDrive.saveObjectMetadata()} call.
 * </p>
 *
 * <p>
 * <b>Rule for future field additions</b>: any field added to
 * {@link ObjectMetadata} with a non-null default <strong>must</strong> also be
 * added to the legacy-retry block in {@link #verify}, or its default must be
 * changed to {@code null} so {@code NON_NULL} naturally excludes it from the
 * serialized form of older objects.
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

    private static final Logger logger = Logger.getLogger(ObjectMetadataChecksum.class.getName());

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
     *   <li>Returns {@code true} if the stored checksum matches the current
     *       full-schema computation.</li>
     *   <li>Returns {@code true} if the stored checksum matches a legacy
     *       computation with {@code integrityStatus} excluded (objects stored
     *       before that field was added to {@link ObjectMetadata}).  The object
     *       is valid; its checksum is refreshed on the next write.</li>
     *   <li>Returns {@code false} if the checksums differ under all known
     *       computations — metadata is likely corrupt on this drive.</li>
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
          	
            
        	// ── Primary check: current full-schema computation ─────────────────────
            String expected = compute(meta, mapper);
            
            if (expected.equals(meta.metaChecksum))
                return true;

            // ── Legacy migration retry ─────────────────────────────────────────────
            // The integrityStatus field was added to ObjectMetadata after metaChecksum
            // was introduced.  Objects stored before that addition carry a stored
            // checksum computed without "integrityStatus" in the JSON, because
            // @JsonInclude(NON_NULL) on ObjectMetadata excluded it when it was null
            // (absent).  When loaded today, Jackson sets the Java-default
            // IntegrityStatus.OK, which serializes as "integrityStatus":1 via
            // @JsonValue — causing a spurious mismatch on every drive simultaneously.
            //
            // Temporarily setting integrityStatus = null reproduces the original JSON.
            // A match here means the object is structurally valid; the mismatch is
            // purely a schema-evolution artefact.  The checksum refreshes on the next
            // saveObjectMetadata() call (e.g. on the next put/update).
            IntegrityStatus savedStatus = meta.integrityStatus;
            meta.integrityStatus = null;
            try {
                String legacy = compute(meta, mapper);
                if (legacy.equals(meta.metaChecksum)) {
                    logger.debug("metaChecksum: legacy-schema match"
                            + " (integrityStatus was absent when checksum was stored;"
                            + " will refresh on next write)"
                            + " | b:" + meta.bucketId + " o:" + meta.objectName);
                    return true;
                }
            } finally {
                meta.integrityStatus = savedStatus; // always restore
            }

            return false; // no known computation matched — likely actual corruption

        } catch (Exception e) {
            return false;
        }
    }
}