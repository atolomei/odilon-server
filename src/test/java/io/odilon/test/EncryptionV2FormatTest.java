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
package io.odilon.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.odilon.encryption.KeyEncryptor;
import io.odilon.encryption.v2.V2DecryptingInputStream;
import io.odilon.encryption.v2.V2EncryptingInputStream;
import io.odilon.encryption.v2.V2Format;

/**
 * <p>
 * Unit tests for the v2 record-based AES-GCM-SIV encryption format
 * ({@code io.odilon.encryption.v2}). No Spring context required: the streams
 * depend only on the {@link KeyEncryptor} interface, stubbed here with an
 * identity wrapper.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class EncryptionV2FormatTest {

    /** small record size so multi-record cases are cheap to exercise */
    private static final int RECORD_SIZE = V2Format.MIN_RECORD_SIZE; // 4 KiB

    /** identity KeyEncryptor: wrapped DEK == DEK (tests only) */
    private static final KeyEncryptor IDENTITY = new KeyEncryptor() {
        @Override
        public byte[] encryptKey(byte[] key, byte[] iv) {
            return key.clone();
        }

        @Override
        public byte[] decryptKey(byte[] key, byte[] iv) {
            return key.clone();
        }
    };

    // ── helpers ────────────────────────────────────────────────────────────

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        new Random(42 + n).nextBytes(b);
        return b;
    }

    private static byte[] encrypt(byte[] plain) throws IOException {
        try (InputStream enc = V2EncryptingInputStream.create(new ByteArrayInputStream(plain), IDENTITY, RECORD_SIZE)) {
            return enc.readAllBytes();
        }
    }

    private static byte[] decrypt(byte[] cipher) throws IOException {
        try (InputStream dec = new V2DecryptingInputStream(new ByteArrayInputStream(cipher), IDENTITY)) {
            return dec.readAllBytes();
        }
    }

    private static void assertRoundTrip(int plainSize) throws IOException {
        byte[] plain = randomBytes(plainSize);
        byte[] cipher = encrypt(plain);
        assertEquals(V2Format.encryptedSize(plainSize, RECORD_SIZE, V2Format.DEK_SIZE), cipher.length,
                "encrypted size accounting (plainSize=" + plainSize + ")");
        assertArrayEquals(plain, decrypt(cipher), "round trip (plainSize=" + plainSize + ")");
    }

    // ── round trips ────────────────────────────────────────────────────────

    @Test
    public void testEmptyObject() throws IOException {
        assertRoundTrip(0);
    }

    @Test
    public void testSmallerThanOneRecord() throws IOException {
        assertRoundTrip(100);
        assertRoundTrip(RECORD_SIZE - 1);
    }

    @Test
    public void testExactlyOneRecord() throws IOException {
        assertRoundTrip(RECORD_SIZE);
    }

    @Test
    public void testMultipleFullRecords() throws IOException {
        assertRoundTrip(3 * RECORD_SIZE);
    }

    @Test
    public void testFinalPartialRecord() throws IOException {
        assertRoundTrip(3 * RECORD_SIZE + 17);
        assertRoundTrip(RECORD_SIZE + 1);
    }

    @Test
    public void testShortReadingSource() throws IOException {
        byte[] plain = randomBytes(2 * RECORD_SIZE + 333);
        /** source returns at most 7 bytes per read */
        InputStream shortReader = new ByteArrayInputStream(plain) {
            @Override
            public synchronized int read(byte[] b, int off, int len) {
                return super.read(b, off, Math.min(len, 7));
            }
        };
        byte[] cipher;
        try (InputStream enc = V2EncryptingInputStream.create(shortReader, IDENTITY, RECORD_SIZE)) {
            cipher = enc.readAllBytes();
        }
        /** short-read the ciphertext on decryption too */
        InputStream shortCipher = new ByteArrayInputStream(cipher) {
            @Override
            public synchronized int read(byte[] b, int off, int len) {
                return super.read(b, off, Math.min(len, 5));
            }
        };
        try (InputStream dec = new V2DecryptingInputStream(shortCipher, IDENTITY)) {
            assertArrayEquals(plain, dec.readAllBytes());
        }
    }

    // ── tamper / corruption detection ──────────────────────────────────────

    @Test
    public void testCorruptedCiphertextFails() throws IOException {
        byte[] cipher = encrypt(randomBytes(2 * RECORD_SIZE + 100));
        /** flip one byte inside the first record's ciphertext */
        int headerSize = V2Format.headerSize(V2Format.DEK_SIZE);
        cipher[headerSize + 4 + 100] ^= 0x01;
        assertThrows(IOException.class, () -> decrypt(cipher));
    }

    @Test
    public void testCorruptedTagFails() throws IOException {
        byte[] cipher = encrypt(randomBytes(100));
        cipher[cipher.length - 1] ^= 0x01; // last byte of the final record's tag
        assertThrows(IOException.class, () -> decrypt(cipher));
    }

    @Test
    public void testTruncatedFinalRecordFails() throws IOException {
        byte[] cipher = encrypt(randomBytes(2 * RECORD_SIZE + 100));
        byte[] truncated = Arrays.copyOf(cipher, cipher.length - 10);
        assertThrows(IOException.class, () -> decrypt(truncated));
    }

    @Test
    public void testMissingFinalRecordFails() throws IOException {
        /** cut the stream right after the last full record: final record missing */
        byte[] cipher = encrypt(randomBytes(2 * RECORD_SIZE + 100));
        int headerSize = V2Format.headerSize(V2Format.DEK_SIZE);
        int recordOnDisk = 4 + RECORD_SIZE + V2Format.TAG_SIZE;
        byte[] truncated = Arrays.copyOf(cipher, headerSize + 2 * recordOnDisk);
        assertThrows(IOException.class, () -> decrypt(truncated));
    }

    @Test
    public void testReorderedRecordsFail() throws IOException {
        byte[] cipher = encrypt(randomBytes(3 * RECORD_SIZE)); // 3 records, last flagged final
        int headerSize = V2Format.headerSize(V2Format.DEK_SIZE);
        int recordOnDisk = 4 + RECORD_SIZE + V2Format.TAG_SIZE;
        /** swap records 0 and 1 */
        byte[] tampered = cipher.clone();
        System.arraycopy(cipher, headerSize + recordOnDisk, tampered, headerSize, recordOnDisk);
        System.arraycopy(cipher, headerSize, tampered, headerSize + recordOnDisk, recordOnDisk);
        assertThrows(IOException.class, () -> decrypt(tampered));
    }

    @Test
    public void testDuplicatedRecordFails() throws IOException {
        byte[] cipher = encrypt(randomBytes(2 * RECORD_SIZE)); // record 0 + final record 1
        int headerSize = V2Format.headerSize(V2Format.DEK_SIZE);
        int recordOnDisk = 4 + RECORD_SIZE + V2Format.TAG_SIZE;
        /** replace record 1 with a copy of record 0 (index mismatch in AAD) */
        byte[] tampered = cipher.clone();
        System.arraycopy(cipher, headerSize, tampered, headerSize + recordOnDisk, recordOnDisk);
        assertThrows(IOException.class, () -> decrypt(tampered));
    }

    @Test
    public void testSubstitutedRecordFromOtherObjectFails() throws IOException {
        byte[] cipherA = encrypt(randomBytes(2 * RECORD_SIZE));
        byte[] cipherB = encrypt(randomBytes(2 * RECORD_SIZE));
        int headerSize = V2Format.headerSize(V2Format.DEK_SIZE);
        int recordOnDisk = 4 + RECORD_SIZE + V2Format.TAG_SIZE;
        /** splice record 0 of object B into object A (different DEK) */
        byte[] tampered = cipherA.clone();
        System.arraycopy(cipherB, headerSize, tampered, headerSize, recordOnDisk);
        assertThrows(IOException.class, () -> decrypt(tampered));
    }

    @Test
    public void testTrailingDataAfterFinalRecordFails() throws IOException {
        byte[] cipher = encrypt(randomBytes(100));
        byte[] extended = Arrays.copyOf(cipher, cipher.length + 5);
        assertThrows(IOException.class, () -> decrypt(extended));
    }

    // ── format / header validation ─────────────────────────────────────────

    @Test
    public void testBadMagicFails() throws IOException {
        byte[] cipher = encrypt(randomBytes(100));
        cipher[0] = 'X';
        assertThrows(IOException.class, () -> decrypt(cipher));
    }

    @Test
    public void testUnsupportedVersionFails() throws IOException {
        byte[] cipher = encrypt(randomBytes(100));
        cipher[4] = 99; // version byte
        assertThrows(IOException.class, () -> decrypt(cipher));
    }

    @Test
    public void testMalformedHeaderFails() {
        /** truncated header */
        byte[] garbage = Arrays.copyOf(V2Format.MAGIC, 10);
        assertThrows(IOException.class, () -> decrypt(garbage));
    }

    @Test
    public void testLegacyStreamIsRejectedByV2Decryptor() {
        /** a legacy stream starts with a JSON header, not the v2 magic */
        byte[] legacyLike = "{\"@class\":\"io.odilon.encryption.JCipherStreamEncryptorInfo\"}".getBytes();
        assertThrows(IOException.class, () -> decrypt(legacyLike));
    }

    // ── size accounting ────────────────────────────────────────────────────

    @Test
    public void testEncryptedSizeAccounting() throws IOException {
        for (int size : new int[] { 0, 1, RECORD_SIZE - 1, RECORD_SIZE, RECORD_SIZE + 1, 5 * RECORD_SIZE, 5 * RECORD_SIZE + 123 }) {
            byte[] cipher = encrypt(randomBytes(size));
            assertEquals(V2Format.encryptedSize(size, RECORD_SIZE, V2Format.DEK_SIZE), cipher.length, "size=" + size);
        }
    }

    // ── large object (bounded memory) ──────────────────────────────────────

    /**
     * <p>
     * Encrypts and decrypts a synthetic stream substantially larger than 2 GiB
     * without allocating the object in memory (the source is generated on the
     * fly, the output is digested and discarded). Excluded from routine CI:
     * run with {@code -Dodilon.test.largeObject=true}.
     * </p>
     */
    @Test
    @EnabledIfSystemProperty(named = "odilon.test.largeObject", matches = "true")
    public void testLargeObjectBoundedMemory() throws Exception {

        final long SIZE = 2_500_000_000L; // 2.5 GB
        final int recordSize = V2Format.DEFAULT_RECORD_SIZE;

        /** deterministic synthetic stream */
        InputStream synthetic = new InputStream() {
            private long pos = 0;

            @Override
            public int read() {
                if (pos >= SIZE)
                    return -1;
                return (int) ((pos++ * 31) & 0xFF);
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (pos >= SIZE)
                    return -1;
                int n = (int) Math.min(len, SIZE - pos);
                for (int i = 0; i < n; i++)
                    b[off + i] = (byte) ((pos + i) * 31);
                pos += n;
                return n;
            }
        };

        MessageDigest mdIn = MessageDigest.getInstance("SHA-256");
        MessageDigest mdOut = MessageDigest.getInstance("SHA-256");

        long cipherBytes = 0;
        try (InputStream enc = V2EncryptingInputStream.create(new java.security.DigestInputStream(synthetic, mdIn), IDENTITY, recordSize)) {
            /** pipe ciphertext straight into the decryptor */
            try (InputStream dec = new V2DecryptingInputStream(enc, IDENTITY)) {
                byte[] buf = new byte[64 * 1024];
                int r;
                long total = 0;
                while ((r = dec.read(buf)) >= 0) {
                    mdOut.update(buf, 0, r);
                    total += r;
                }
                assertEquals(SIZE, total, "decrypted size");
            }
        }
        assertArrayEquals(mdIn.digest(), mdOut.digest(), "plaintext digest after >2GB round trip");
        assertTrue(cipherBytes >= 0);
    }

    // ── misc stream behavior ───────────────────────────────────────────────

    @Test
    public void testSingleByteReads() throws IOException {
        byte[] plain = randomBytes(RECORD_SIZE + 10);
        byte[] cipher = encrypt(plain);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream dec = new V2DecryptingInputStream(new ByteArrayInputStream(cipher), IDENTITY)) {
            int b;
            while ((b = dec.read()) >= 0)
                out.write(b);
        }
        assertArrayEquals(plain, out.toByteArray());
    }

    @Test
    public void testNoFurtherReadsAfterAuthFailure() throws IOException {
        byte[] cipher = encrypt(randomBytes(2 * RECORD_SIZE));
        int headerSize = V2Format.headerSize(V2Format.DEK_SIZE);
        cipher[headerSize + 4 + 5] ^= 0x01; // corrupt record 0
        V2DecryptingInputStream dec = new V2DecryptingInputStream(new ByteArrayInputStream(cipher), IDENTITY);
        assertThrows(IOException.class, () -> dec.readAllBytes());
        assertThrows(IOException.class, () -> dec.read());
        dec.close();
    }
}
