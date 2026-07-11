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

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class JCipherStreamEncryptor implements StreamEncryptor {

	@JsonIgnore
	static SecureRandom secRandom = new SecureRandom();

	@JsonIgnore
	private KeyEncryptor keyEncryptor;

	@JsonIgnore
	byte[] ivs;

	private String encryptionAlgorithm;

	private String keyAlgorithm;

	/**
	 * @param encryptionAlgorithm
	 * @param keyAlgorithm
	 * @param keyEncryptor
	 */
	public JCipherStreamEncryptor(String encryptionAlgorithm, String keyAlgorithm, KeyEncryptor keyEncryptor) {
		this.encryptionAlgorithm = encryptionAlgorithm;
		this.keyAlgorithm = keyAlgorithm;
		this.keyEncryptor = keyEncryptor;
		this.ivs = new byte[EncryptionService.IV_LENGTH_BIT / 8];
		secRandom.nextBytes(ivs);
	}

	@Override
	public String getNewKey() {
		try {
			KeyGenerator keyGen = KeyGenerator.getInstance(getKeyAlgorithm());
			keyGen.init(EncryptionService.AES_KEY_SIZE_BITS, secRandom);
			Key key = keyGen.generateKey();
			return Base64.getEncoder().encodeToString(key.getEncoded());
		} catch (NoSuchAlgorithmException e) {
			throw new InternalCriticalException(e, "genNewKey");
		}
	}

	@Override
	public StreamEncryptorInfo getStreamEncryptionInfo(String key, String ivStr) {

		byte[] decodedKey = Base64.getDecoder().decode(key);
		byte[] ivec = Base64.getDecoder().decode(ivStr);

		String encryptKey = Base64.getEncoder().encodeToString(getKeyEncryptor().encryptKey(decodedKey, ivec));
		return new JCipherStreamEncryptorInfo(this, encryptKey, Base64.getEncoder().encodeToString(ivec));
	}

	@Override
	public EncryptedInputStream encrypt(InputStream inputStream, String key, String ivString) {
		byte[] decodedKey = null;
		try {

			decodedKey = Base64.getDecoder().decode(key);
			byte[] ivec = Base64.getDecoder().decode(ivString);

			InputStream encryptedStream = processStream(inputStream, Cipher.ENCRYPT_MODE, decodedKey, ivec);

			StreamEncryptorInfo streamEncryptionInfo = this.getStreamEncryptionInfo(key, ivString);

			return new EncryptedInputStream(encryptedStream, streamEncryptionInfo);

		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
			throw new InternalCriticalException(e, "encrypt");
		} finally {
			if (decodedKey != null)
				Arrays.fill(decodedKey, (byte) 0);
		}
	}

	@Override
	public InputStream decrypt(InputStream inputStream, String encryptedKey, String ivString) {
		byte[] key = null;
		try {

			byte[] decodedEncryptedkey = Base64.getDecoder().decode(encryptedKey);
			byte[] ivec = Base64.getDecoder().decode(ivString);

			key = getKeyEncryptor().decryptKey(decodedEncryptedkey, ivec);

			return processStream(inputStream, Cipher.DECRYPT_MODE, key, ivec);

		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
			throw new InternalCriticalException(e, " decrypt");
		} finally {
			if (key != null)
				Arrays.fill(key, (byte) 0);
		}
	}

	@Override
	public String getIV() {
		return Base64.getEncoder().encodeToString(getIVS());
	}

	public String getEncryptionAlgorithm() {
		return encryptionAlgorithm;
	}

	public void setEncryptionAlgorithm(String encryptionAlgorithm) {
		this.encryptionAlgorithm = encryptionAlgorithm;
	}

	public String getKeyAlgorithm() {
		return keyAlgorithm;
	}

	public void setKeyAlgorithm(String keyAlgorithm) {
		this.keyAlgorithm = keyAlgorithm;
	}

	public KeyEncryptor getKeyEncryptor() {
		return keyEncryptor;
	}

	public void setKeyEncryptor(KeyEncryptor keyEncryptor) {
		this.keyEncryptor = keyEncryptor;
	}

	private InputStream processStream(InputStream inputStream, int encryptMode, byte[] key, byte[] iv) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {

		SecretKeySpec secretKeySpec = new SecretKeySpec(key, getKeyAlgorithm());

		Cipher c = Cipher.getInstance(getEncryptionAlgorithm());

		try {
			c.init(encryptMode, secretKeySpec, new GCMParameterSpec(EncryptionService.IV_LENGTH_BIT, iv));
		} catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
			throw new InternalCriticalException(e, "processStream: cipher init failed (bad key or IV)");
		}
		return new CipherInputStream(inputStream, c);

	}

	private byte[] getIVS() {
		return this.ivs;
	}

}
