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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.modes.GCMSIVBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;

import io.odilon.encryption.KeyEncryptor;

/**
 * <p>
 * Streaming decryptor for the {@link V2Format v2 record-based AES-GCM-SIV
 * format}. Wraps the encrypted stream and exposes the plaintext.
 * </p>
 *
 * <p>
 * Fail-closed behavior: a record's plaintext is only exposed after its
 * authentication tag has been verified. Reordering, substitution, duplication
 * and corruption are detected per record (nonce + AAD mismatch); truncation
 * (missing final record or partial record) is detected via the authenticated
 * final-record flag; trailing bytes after the final record are rejected. Any
 * violation throws an {@link IOException} and the stream becomes unusable.
 * </p>
 *
 * <p>
 * Memory use is bounded by the record size declared in the header (validated
 * against {@link V2Format#MAX_RECORD_SIZE}).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class V2DecryptingInputStream extends InputStream {

    private final InputStream source;

    /** GCM-SIV over AES (the no-arg constructor uses an AES engine) */
    private final GCMSIVBlockCipher cipher = new GCMSIVBlockCipher();

    private byte[] dek;
    private byte[] baseNonce;
    private int recordSize;

    /** ciphertext buffer for the record being read */
    private byte[] cipherBuf;

    /** verified plaintext of the current record */
    private byte[] plainBuf;
    private int plainPos;
    private int plainLen;

    private long recordIndex = 0;
    private long totalPlain = 0;

    private boolean finalRecordSeen = false;
    private boolean closed = false;
    private boolean failed = false;

    /**
     * Parses and validates the object header, and unwraps the DEK with the
     * given {@link KeyEncryptor}.
     *
     * @throws IOException if the header is malformed or truncated
     */
    public V2DecryptingInputStream(InputStream source, KeyEncryptor keyEncryptor) throws IOException {

        if (source == null)
            throw new IllegalArgumentException("source is null");
        if (keyEncryptor == null)
            throw new IllegalArgumentException("keyEncryptor is null");

        this.source = source;
        readHeader(keyEncryptor);
    }

    private void readHeader(KeyEncryptor keyEncryptor) throws IOException {

        byte[] fixed = new byte[4 + 1 + 1 + 4 + V2Format.WRAP_IV_SIZE + 2];
        readFully(fixed, 0, fixed.length, "object header");

        if (fixed[0] != V2Format.MAGIC[0] || fixed[1] != V2Format.MAGIC[1] || fixed[2] != V2Format.MAGIC[2] || fixed[3] != V2Format.MAGIC[3])
            throw new IOException("not an Odilon v2 encrypted stream (bad magic)");

        if (fixed[4] != V2Format.VERSION_BYTE)
            throw new IOException("unsupported encryption format version -> " + (fixed[4] & 0xFF));

        if (fixed[5] != 0)
            throw new IOException("unsupported header flags -> " + (fixed[5] & 0xFF));

        this.recordSize = ((fixed[6] & 0xFF) << 24) | ((fixed[7] & 0xFF) << 16) | ((fixed[8] & 0xFF) << 8) | (fixed[9] & 0xFF);
        if (this.recordSize < V2Format.MIN_RECORD_SIZE || this.recordSize > V2Format.MAX_RECORD_SIZE)
            throw new IOException("invalid record size in header -> " + this.recordSize);

        byte[] wrapIv = new byte[V2Format.WRAP_IV_SIZE];
        System.arraycopy(fixed, 10, wrapIv, 0, V2Format.WRAP_IV_SIZE);

        int dekLen = ((fixed[10 + V2Format.WRAP_IV_SIZE] & 0xFF) << 8) | (fixed[11 + V2Format.WRAP_IV_SIZE] & 0xFF);
        if (dekLen <= 0 || dekLen > V2Format.MAX_WRAPPED_DEK_SIZE)
            throw new IOException("invalid wrapped DEK length -> " + dekLen);

        byte[] wrappedDek = new byte[dekLen];
        readFully(wrappedDek, 0, dekLen, "wrapped DEK");

        this.baseNonce = new byte[V2Format.BASE_NONCE_SIZE];
        readFully(this.baseNonce, 0, V2Format.BASE_NONCE_SIZE, "base nonce");

        /** unwrapping is authenticated (AES/GCM under master key or KMS) */
        this.dek = keyEncryptor.decryptKey(wrappedDek, wrapIv);
        if (this.dek == null || this.dek.length != V2Format.DEK_SIZE)
            throw new IOException("invalid unwrapped DEK");

        this.cipherBuf = new byte[this.recordSize + V2Format.TAG_SIZE];
        this.plainBuf = new byte[this.recordSize];
        this.plainPos = 0;
        this.plainLen = 0;
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return (n < 0) ? -1 : (one[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (this.closed)
            throw new IOException("stream closed");
        if (this.failed)
            throw new IOException("stream failed authentication, no further reads allowed");
        if (len == 0)
            return 0;

        while (this.plainPos >= this.plainLen) {
            if (this.finalRecordSeen)
                return -1;
            decryptNextRecord();
            /** loop: a final record may be empty */
        }
        int n = Math.min(len, this.plainLen - this.plainPos);
        System.arraycopy(this.plainBuf, this.plainPos, b, off, n);
        this.plainPos += n;
        return n;
    }

    private void decryptNextRecord() throws IOException {
        try {
            decryptNextRecordInternal();
        } catch (IOException e) {
            this.failed = true;
            throw e;
        }
    }

    private void decryptNextRecordInternal() throws IOException {

        byte[] lenBytes = new byte[4];
        readFully(lenBytes, 0, 4, "record header (truncated stream: final record missing)");

        int lenAndFlag = ((lenBytes[0] & 0xFF) << 24) | ((lenBytes[1] & 0xFF) << 16) | ((lenBytes[2] & 0xFF) << 8) | (lenBytes[3] & 0xFF);
        boolean finalRecord = (lenAndFlag & V2Format.FINAL_RECORD_FLAG) != 0;
        int cipherLen = lenAndFlag & ~V2Format.FINAL_RECORD_FLAG;

        if (cipherLen < V2Format.TAG_SIZE || cipherLen > this.recordSize + V2Format.TAG_SIZE)
            throw new IOException("invalid record length -> " + cipherLen + " (record " + this.recordIndex + ")");

        if (!finalRecord && cipherLen != this.recordSize + V2Format.TAG_SIZE)
            throw new IOException("non-final record must be full size (record " + this.recordIndex + ")");

        readFully(this.cipherBuf, 0, cipherLen, "record ciphertext (truncated record " + this.recordIndex + ")");

        int expectedPlainLen = cipherLen - V2Format.TAG_SIZE;
        long expectedTotal = this.totalPlain + expectedPlainLen;

        byte[] nonce = V2Format.recordNonce(this.baseNonce, this.recordIndex);
        byte[] aad = V2Format.recordAAD(this.recordIndex, finalRecord, expectedTotal);

        this.cipher.reset();
        this.cipher.init(false, new AEADParameters(new KeyParameter(this.dek), V2Format.TAG_SIZE * 8, nonce, aad));

        int n;
        try {
            n = this.cipher.processBytes(this.cipherBuf, 0, cipherLen, this.plainBuf, 0);
            n += this.cipher.doFinal(this.plainBuf, n);
        } catch (InvalidCipherTextException e) {
            throw new IOException("record authentication failed (record " + this.recordIndex
                    + "): data corrupted, reordered, substituted or truncated", e);
        }

        if (n != expectedPlainLen)
            throw new IOException("unexpected plaintext length (record " + this.recordIndex + ")");

        this.totalPlain = expectedTotal;
        this.plainPos = 0;
        this.plainLen = n;
        this.recordIndex++;

        if (finalRecord) {
            this.finalRecordSeen = true;
            /** reject any trailing bytes after the authenticated final record */
            if (this.source.read() >= 0)
                throw new IOException("unexpected data after final record");
        }
    }

    private void readFully(byte[] b, int off, int len, String what) throws IOException {
        int n = 0;
        while (n < len) {
            int r = this.source.read(b, off + n, len - n);
            if (r < 0)
                throw new EOFException("unexpected end of stream reading " + what);
            n += r;
        }
    }

    @Override
    public int available() {
        return this.plainLen - this.plainPos;
    }

    @Override
    public void close() throws IOException {
        if (this.closed)
            return;
        this.closed = true;
        try {
            this.source.close();
        } finally {
            if (this.dek != null)
                Arrays.fill(this.dek, (byte) 0);
            if (this.plainBuf != null)
                Arrays.fill(this.plainBuf, (byte) 0);
        }
    }
}
