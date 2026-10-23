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
package io.odilon.encryption;

import java.io.InputStream;

import io.odilon.service.SystemService;

/**
 * 
 * @see {@link OdilonEncryptionService}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface EncryptionService extends SystemService {

	static public final String ENCRYPTION_ALGORITHM = "AES";
	static public final String ENCRYPTION_ALGORITHM_METHOD = "AES/GCM/NoPadding";
	static public final int IV_LENGTH_BIT = 128;

	static final public int AES_KEY_SIZE_BITS = 256; // 32 bytes — new installations only; existing 128-bit keys remain valid
	static final public int AES_IV_SIZE_BITS = 96; // 12 bytes
	static final public int AES_KEY_SALT_SIZE_BITS = 512;
	static final public int HMAC_SIZE = 32;

	/**
	 * <p>
	 * Stamped on {@code ObjectMetadata.systemTags} when encryption is enabled
	 * but the Object was stored <b>unencrypted</b> because its size exceeds
	 * {@code encryption.maxObjectSize}. The SunJCE AES/GCM provider can not
	 * process a single stream larger than {@code Integer.MAX_VALUE} bytes
	 * (~2 GiB). Only applies to objects written with the legacy format; the v2
	 * record-based format has no object-size limit.
	 * </p>
	 */
	static final public String SYSTEM_TAG_TOO_LARGE_TO_ENCRYPT = "encryption-skipped-too-large-for-gcm";

	/**
	 * Record-based AES-GCM-SIV format (see {@code io.odilon.encryption.v2}).
	 * Value stored in {@code ObjectMetadata.encryptionFormat}. A {@code null}
	 * metadata value means the legacy whole-object format.
	 */
	static final public int ENCRYPTION_FORMAT_V2 = 2;

	public EncryptedResult encryptStream(InputStream inputStream);

	public InputStream decryptStream(InputStream inputStream);

	/**
	 * Deterministic variant when you know plaintext length in advance.
	 */
	EncryptedResult encryptStream(InputStream inputStream, long plainSize);

	/**
	 * <p>
	 * Encrypts with the v2 record-based AES-GCM-SIV format. Bounded memory:
	 * supports objects of any size. Used for all new object writes.
	 * </p>
	 */
	public EncryptedResult encryptStreamV2(InputStream inputStream);

	/**
	 * <p>
	 * Decrypts dispatching on the object's stored encryption format
	 * ({@code ObjectMetadata.encryptionFormat}): {@code null} &rarr; legacy
	 * format, {@code 2} &rarr; v2 record-based format. Any other value fails
	 * explicitly.
	 * </p>
	 */
	public InputStream decryptStream(InputStream inputStream, Integer encryptionFormat);

}
