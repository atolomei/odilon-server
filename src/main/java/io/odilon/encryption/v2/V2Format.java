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
package io.odilon.encryption.v2;

/**
 * <p>
 * Odilon encryption format <b>v2</b>: record-based AES-GCM-SIV (RFC 8452).
 * </p>
 *
 * <p>
 * The object is encrypted as one object-level header followed by a sequence of
 * bounded-size, independently authenticated records. Memory use is bounded by
 * the record size, so objects larger than 2 GiB can be encrypted and decrypted
 * with a bounded heap (unlike the legacy whole-object AES/GCM format).
 * </p>
 *
 * <h2>Object header</h2>
 * 
 * <pre>
 * magic       4 bytes   'O' 'E' 'N' '2'
 * version     1 byte    0x02
 * flags       1 byte    0x00 (reserved, must be 0)
 * recordSize  4 bytes   big-endian, plaintext bytes per full record (&gt; 0)
 * wrapIv     12 bytes   IV used by the KeyEncryptor to wrap the DEK
 * dekLen      2 bytes   big-endian, length of the wrapped DEK
 * wrappedDek  dekLen    DEK wrapped by the KeyEncryptor (master key or KMS)
 * baseNonce   8 bytes   random per object
 * </pre>
 *
 * <h2>Records</h2>
 * 
 * <pre>
 * lenAndFlag  4 bytes   big-endian; bit 31 = "final record" flag,
 *                       bits 0..30 = ciphertext length (plaintext + 16-byte tag)
 * ciphertext  len bytes AES-GCM-SIV output (includes the 16-byte tag)
 * </pre>
 *
 * <ul>
 * <li>One fresh random 256-bit DEK per object, wrapped once in the header
 * (envelope encryption is preserved).</li>
 * <li>Record nonce (12 bytes) = {@code baseNonce(8) || BE32(recordIndex)}.
 * Nonces are unique per record under the same DEK by construction; GCM-SIV
 * additionally tolerates nonce misuse.</li>
 * <li>AAD = {@code magic(4) || version(1) || finalFlag(1) || BE64(recordIndex)
 * || BE64(totalPlaintextLength if final else 0)}. This authenticates record
 * ordering (index), the end-of-object marker (final flag) and the total object
 * length, so reordering, substitution, duplication, truncation and missing
 * records all cause an authentication failure.</li>
 * <li>Every non-final record carries exactly {@code recordSize} plaintext
 * bytes. Exactly one final record (0..recordSize plaintext bytes) terminates
 * the stream; an empty object is a single empty final record. Any bytes after
 * the final record are rejected.</li>
 * </ul>
 *
 * <p>
 * The encrypted stream is opaque to the storage layer: RAID 0 / RAID 1 store
 * it as a single file, Erasure Coding splits it into its usual 32 MiB storage
 * stripes. Encryption-record boundaries are independent of EC stripe
 * boundaries.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public final class V2Format {

    /** value stored in {@code ObjectMetadata.encryptionFormat} */
    public static final int FORMAT_VERSION = 2;

    public static final byte[] MAGIC = { 'O', 'E', 'N', '2' };

    public static final byte VERSION_BYTE = 0x02;

    /** AES-GCM-SIV authentication tag length (bytes) */
    public static final int TAG_SIZE = 16;

    /** AES-GCM-SIV nonce length (bytes), RFC 8452 */
    public static final int NONCE_SIZE = 12;

    /** random per-object base nonce stored in the header (bytes) */
    public static final int BASE_NONCE_SIZE = 8;

    /** IV length used to wrap the DEK (AES/GCM under the master key / KMS) */
    public static final int WRAP_IV_SIZE = 12;

    /** DEK size: AES-256 */
    public static final int DEK_SIZE = 32;

    /** default plaintext bytes per record: 4 MiB */
    public static final int DEFAULT_RECORD_SIZE = 4 * 1024 * 1024;

    /** lower/upper bounds for a valid record size (sanity limits) */
    public static final int MIN_RECORD_SIZE = 4 * 1024;
    public static final int MAX_RECORD_SIZE = 64 * 1024 * 1024;

    /** sanity bound for the wrapped DEK length field */
    public static final int MAX_WRAPPED_DEK_SIZE = 1024;

    /** bit 31 of the record length word marks the final record */
    public static final int FINAL_RECORD_FLAG = 0x80000000;

    /** per-record framing overhead: 4-byte length word + 16-byte tag */
    public static final int RECORD_OVERHEAD = 4 + TAG_SIZE;

    /** AAD length: magic(4) + version(1) + finalFlag(1) + index(8) + total(8) */
    public static final int AAD_SIZE = 22;

    private V2Format() {
    }

    /** header size in bytes for a given wrapped-DEK length */
    public static int headerSize(int wrappedDekLength) {
        return 4 + 1 + 1 + 4 + WRAP_IV_SIZE + 2 + wrappedDekLength + BASE_NONCE_SIZE;
    }

    /** number of records for a given plaintext size (always &ge; 1) */
    public static long recordCount(long plainSize, int recordSize) {
        if (plainSize <= 0)
            return 1; // empty object -> single empty final record
        long full = plainSize / recordSize;
        long rem = plainSize % recordSize;
        return (rem == 0) ? full : full + 1;
    }

    /**
     * Total encrypted stream size (header + framed records) for a known
     * plaintext size.
     */
    public static long encryptedSize(long plainSize, int recordSize, int wrappedDekLength) {
        return headerSize(wrappedDekLength) + plainSize + recordCount(plainSize, recordSize) * (long) RECORD_OVERHEAD;
    }

    /** builds the 12-byte record nonce: baseNonce(8) || BE32(recordIndex) */
    public static byte[] recordNonce(byte[] baseNonce, long recordIndex) {
        byte[] nonce = new byte[NONCE_SIZE];
        System.arraycopy(baseNonce, 0, nonce, 0, BASE_NONCE_SIZE);
        nonce[8] = (byte) (recordIndex >>> 24);
        nonce[9] = (byte) (recordIndex >>> 16);
        nonce[10] = (byte) (recordIndex >>> 8);
        nonce[11] = (byte) recordIndex;
        return nonce;
    }

    /**
     * builds the record AAD: magic(4) || version(1) || finalFlag(1) ||
     * BE64(recordIndex) || BE64(totalPlaintext if final else 0)
     */
    public static byte[] recordAAD(long recordIndex, boolean finalRecord, long totalPlainLength) {
        byte[] aad = new byte[AAD_SIZE];
        System.arraycopy(MAGIC, 0, aad, 0, 4);
        aad[4] = VERSION_BYTE;
        aad[5] = (byte) (finalRecord ? 1 : 0);
        putLongBE(aad, 6, recordIndex);
        putLongBE(aad, 14, finalRecord ? totalPlainLength : 0L);
        return aad;
    }

    static void putLongBE(byte[] b, int off, long v) {
        for (int i = 0; i < 8; i++)
            b[off + i] = (byte) (v >>> (8 * (7 - i)));
    }
}
