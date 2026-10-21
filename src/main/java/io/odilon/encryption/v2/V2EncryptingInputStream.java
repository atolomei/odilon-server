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

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.Arrays;

import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.modes.GCMSIVBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;

import io.odilon.encryption.KeyEncryptor;

/**
 * <p>
 * Streaming encryptor for the {@link V2Format v2 record-based AES-GCM-SIV
 * format}. Wraps a plaintext {@link InputStream} and produces the encrypted
 * stream (object header + authenticated records) lazily as it is read.
 * </p>
 *
 * <p>
 * Memory use is bounded by the record size: at most one plaintext record and
 * one ciphertext record are buffered at any time, regardless of object size.
 * Short reads from the source stream are handled (each record is filled with a
 * read loop). Closing this stream closes the source stream and zeroes the DEK.
 * </p>
 *
 * <p>
 * Not thread-safe (as all {@link InputStream}s).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class V2EncryptingInputStream extends InputStream {

    private static final SecureRandom secureRandom = new SecureRandom();

    private final InputStream source;
    private final byte[] dek;
    private final byte[] baseNonce;
    private final int recordSize;

    /** GCM-SIV over AES (the no-arg constructor uses an AES engine) */
    private final GCMSIVBlockCipher cipher = new GCMSIVBlockCipher();

    /** plaintext buffer for the record being built */
    private final byte[] plainBuf;

    /** output buffer: header first, then [len(4)][ciphertext+tag] per record */
    private byte[] outBuf;
    private int outPos;
    private int outLen;

    private long recordIndex = 0;
    private long totalPlain = 0;

    /** one-byte lookahead used to detect the final record */
    private int carry = -1;
    private boolean sourceEof = false;
    private boolean finalRecordEmitted = false;
    private boolean closed = false;

    /**
     * Creates the encryptor generating a fresh DEK, wrap IV and base nonce, and
     * wrapping the DEK with the given {@link KeyEncryptor}.
     */
    public static V2EncryptingInputStream create(InputStream source, KeyEncryptor keyEncryptor, int recordSize) {
        byte[] dek = new byte[V2Format.DEK_SIZE];
        byte[] wrapIv = new byte[V2Format.WRAP_IV_SIZE];
        byte[] baseNonce = new byte[V2Format.BASE_NONCE_SIZE];
        secureRandom.nextBytes(dek);
        secureRandom.nextBytes(wrapIv);
        secureRandom.nextBytes(baseNonce);
        byte[] wrappedDek = keyEncryptor.encryptKey(dek, wrapIv);
        return new V2EncryptingInputStream(source, dek, wrappedDek, wrapIv, baseNonce, recordSize);
    }

    V2EncryptingInputStream(InputStream source, byte[] dek, byte[] wrappedDek, byte[] wrapIv, byte[] baseNonce, int recordSize) {

        if (source == null)
            throw new IllegalArgumentException("source is null");
        if (recordSize < V2Format.MIN_RECORD_SIZE || recordSize > V2Format.MAX_RECORD_SIZE)
            throw new IllegalArgumentException("invalid recordSize -> " + recordSize);
        if (wrappedDek == null || wrappedDek.length == 0 || wrappedDek.length > V2Format.MAX_WRAPPED_DEK_SIZE)
            throw new IllegalArgumentException("invalid wrapped DEK");

        this.source = source;
        this.dek = dek;
        this.baseNonce = baseNonce;
        this.recordSize = recordSize;
        this.plainBuf = new byte[recordSize];

        /** output buffer must hold the header or one framed record */
        int maxRecordOut = 4 + recordSize + V2Format.TAG_SIZE;
        this.outBuf = new byte[Math.max(maxRecordOut, V2Format.headerSize(wrappedDek.length))];

        writeHeader(wrappedDek, wrapIv);
    }

    private void writeHeader(byte[] wrappedDek, byte[] wrapIv) {
        int p = 0;
        System.arraycopy(V2Format.MAGIC, 0, this.outBuf, p, 4);
        p += 4;
        this.outBuf[p++] = V2Format.VERSION_BYTE;
        this.outBuf[p++] = 0; // flags
        this.outBuf[p++] = (byte) (this.recordSize >>> 24);
        this.outBuf[p++] = (byte) (this.recordSize >>> 16);
        this.outBuf[p++] = (byte) (this.recordSize >>> 8);
        this.outBuf[p++] = (byte) this.recordSize;
        System.arraycopy(wrapIv, 0, this.outBuf, p, V2Format.WRAP_IV_SIZE);
        p += V2Format.WRAP_IV_SIZE;
        this.outBuf[p++] = (byte) (wrappedDek.length >>> 8);
        this.outBuf[p++] = (byte) wrappedDek.length;
        System.arraycopy(wrappedDek, 0, this.outBuf, p, wrappedDek.length);
        p += wrappedDek.length;
        System.arraycopy(this.baseNonce, 0, this.outBuf, p, V2Format.BASE_NONCE_SIZE);
        p += V2Format.BASE_NONCE_SIZE;
        this.outPos = 0;
        this.outLen = p;
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return (n < 0) ? -1 : (one[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed)
            throw new IOException("stream closed");
        if (len == 0)
            return 0;
        if (this.outPos >= this.outLen) {
            if (this.finalRecordEmitted)
                return -1;
            encryptNextRecord();
        }
        int n = Math.min(len, this.outLen - this.outPos);
        System.arraycopy(this.outBuf, this.outPos, b, off, n);
        this.outPos += n;
        return n;
    }

    /**
     * Fills {@link #plainBuf} with the next record's plaintext, handling short
     * reads, and determines whether it is the final record via a one-byte
     * lookahead.
     */
    private int fillRecord() throws IOException {
        int n = 0;
        if (this.carry >= 0) {
            this.plainBuf[n++] = (byte) this.carry;
            this.carry = -1;
        }
        while (n < this.recordSize) {
            int r = this.source.read(this.plainBuf, n, this.recordSize - n);
            if (r < 0) {
                this.sourceEof = true;
                break;
            }
            n += r;
        }
        if (!this.sourceEof) {
            int b = this.source.read();
            if (b < 0)
                this.sourceEof = true;
            else
                this.carry = b;
        }
        return n;
    }

    private void encryptNextRecord() throws IOException {

        int plainLen = fillRecord();
        boolean finalRecord = this.sourceEof;
        this.totalPlain += plainLen;

        byte[] nonce = V2Format.recordNonce(this.baseNonce, this.recordIndex);
        byte[] aad = V2Format.recordAAD(this.recordIndex, finalRecord, this.totalPlain);

        this.cipher.reset();
        this.cipher.init(true, new AEADParameters(new KeyParameter(this.dek), V2Format.TAG_SIZE * 8, nonce, aad));

        int cipherLen;
        try {
            int off = 4 + this.cipher.processBytes(this.plainBuf, 0, plainLen, this.outBuf, 4);
            cipherLen = off - 4 + this.cipher.doFinal(this.outBuf, off);
        } catch (InvalidCipherTextException e) {
            throw new IOException("encryption failed (record " + this.recordIndex + ")", e);
        }

        int lenAndFlag = cipherLen | (finalRecord ? V2Format.FINAL_RECORD_FLAG : 0);
        this.outBuf[0] = (byte) (lenAndFlag >>> 24);
        this.outBuf[1] = (byte) (lenAndFlag >>> 16);
        this.outBuf[2] = (byte) (lenAndFlag >>> 8);
        this.outBuf[3] = (byte) lenAndFlag;

        this.outPos = 0;
        this.outLen = 4 + cipherLen;
        this.recordIndex++;
        if (finalRecord)
            this.finalRecordEmitted = true;
    }

    @Override
    public int available() {
        return this.outLen - this.outPos;
    }

    @Override
    public void close() throws IOException {
        if (this.closed)
            return;
        this.closed = true;
        try {
            this.source.close();
        } finally {
            Arrays.fill(this.dek, (byte) 0);
            Arrays.fill(this.plainBuf, (byte) 0);
        }
    }
}
