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
 */
package io.odilon.encryption;


import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class JCipherStreamEncryptor implements StreamEncryptor {

	private static Logger logger = Logger.getLogger(JCipherStreamEncryptor.class.getName());

    /** Creating a SecureRandom object */
	@JsonIgnore
    static SecureRandom secRandom = new SecureRandom();

	private String encryptionAlgorithm; // = "AES/ECB/PKCS5Padding";
	private String keyAlgorithm; 		// = "AES"; 
	
	@JsonIgnore
	private KeyEncryptor keyEncryptor; 	 

	/**
	 * 
	 * @param encryptionAlgorithm
	 * @param keyAlgorithm
	 * @param keyEncryptor
	 */
	public JCipherStreamEncryptor(String encryptionAlgorithm, String keyAlgorithm, KeyEncryptor keyEncryptor) {
		 this.encryptionAlgorithm = encryptionAlgorithm;
	     this.keyAlgorithm = keyAlgorithm;
	     this.keyEncryptor = keyEncryptor;
	}
	
    public String genNewKey(){
        try {
            //Creating a KeyGenerator object
            KeyGenerator keyGen = KeyGenerator.getInstance(this.keyAlgorithm);

            //Initializing the KeyGenerator
            keyGen.init(secRandom);
            
            //Creating/Generating a key
            Key key = keyGen.generateKey();
            
            return Base64.getEncoder().encodeToString(key.getEncoded());
            
        }catch (NoSuchAlgorithmException e){
        	logger.error(e);
        	throw new InternalCriticalException(e);
        }
    }

    @Override
    public StreamEncryptorInfo getStreamEncryptionInfo(String key) {
        byte[] decodedKey = Base64.getDecoder().decode(key);
        String encryptKey = Base64.getEncoder().encodeToString(keyEncryptor.encryptKey(decodedKey));
        return new JCipherStreamEncryptorInfo(this, encryptKey);
    }

    @Override
    public EncryptedInputStream encrypt(InputStream inputStream, String key) {
        try {
            byte[] decodedKey = Base64.getDecoder().decode(key);
            InputStream encryptedStream = processStream(inputStream, Cipher.ENCRYPT_MODE, decodedKey);
            StreamEncryptorInfo streamEncryptionInfo = this.getStreamEncryptionInfo(key);
            return new EncryptedInputStream(encryptedStream, streamEncryptionInfo);
            
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
        	logger.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream decrypt(InputStream inputStream, String encryptedKey) {
        try {
            byte[] decodedEncryptedkey = Base64.getDecoder().decode(encryptedKey);
            byte[] key = this.keyEncryptor.decryptKey(decodedEncryptedkey);
            return processStream(inputStream, Cipher.DECRYPT_MODE, key);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
        	logger.error(e);
        	throw new RuntimeException(e);
        }
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

    
    private InputStream processStream(InputStream inputStream, int encryptMode, byte[] key) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        SecretKeySpec secretKeySpec = new SecretKeySpec(key, getKeyAlgorithm());
        Cipher c = Cipher.getInstance(getEncryptionAlgorithm());
        c.init(encryptMode, secretKeySpec);
        return new CipherInputStream(inputStream, c);
    }


}
