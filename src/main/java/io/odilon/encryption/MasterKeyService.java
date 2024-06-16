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


import jakarta.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.service.util.ByteToString;

/**
 * <p>The variable <b>encryption.key</b> in <b>odilon.properties</b> contain a AES key used to decrypt the
 * Server Master Key on server's startup. </p>
 * 
 * <p>The Master Key is unique for each server and can not be changed.</p>
 *  
 * <p>It used by the {@link EncryptionService} to encrypt/decrypt files. Strictly speaking the
 * Master Key is used by the EncryptionService to encrypt/decrypt the key used to encrypt/decrypt every 
 * Object. Each Object has its own unique encryption key</p>
 *
 *<p>The procedure is:</p>
 *
 * <b>SERVER STARTUP</b>
 * <ul>
 * <li>Odilon decrypts the MasterKEy using the key provided in variable encryption.key in odilon.properties</li>
 * </ul>
 * 
 * 
 * <br/>
 * <b>PUT OBJECT</b>
 * <ul>
 * <li>Odilon generates a new key for every Object (objKey)</li>
 * <li>The Object is encrypted using AES with key objKey</li>
 * <li>The obkKey is encrypted by {@link OdilonKeyEncryptorService} using AES with the server Master Key or by the KMS if enabled, and saved in disk as a prefix of the Object</li>
 * </ul>
 * 
 * 
 * <br/>
 * 
 * <b>GET OBJECT</b>
 * </ul>
 * <li>Odilon reads the stored Object and extracts the objKey</li>
 * <li>objKey is decrypted using MasterKey or KMS if enabled</li>
 * <li>Object is decrypted using objKey</li>
 * </ul>
 *
 *<br/>
 *<br/>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class MasterKeyService extends BaseService  {

	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(OdilonKeyEncryptorService.class.getName());

	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
   @JsonIgnore
   private byte[] keyToEncryptMasterKey;  /** encrpytion key from odilon.properties */ 
   
   @JsonIgnore
   private byte[] iv; /** encrpytion IV from odilon.properties */
   
   public MasterKeyService(ServerSettings serverSettings) {
	this.serverSettings=serverSettings;
   }
    
    /**
     * 
     */
    public byte[] encryptKey(byte[] keyWithSalt, byte[] iv) {
        try {
        	
        	SecretKeySpec secretKeySpec = new SecretKeySpec(keyToEncryptMasterKey, EncryptionService.ENCRYPTION_ALGORITHM);
        	Cipher enc = Cipher.getInstance(EncryptionService.ENCRYPTION_ALGORITHM_METHOD);
			enc.init(Cipher.ENCRYPT_MODE, secretKeySpec, new GCMParameterSpec(EncryptionService.IV_LENGTH_BIT, iv));
			return enc.doFinal( keyWithSalt);
			
        } catch (Exception e){
            throw new InternalCriticalException(e, "encryptKey");
        }
    }

    
    public byte[] decryptKey(byte[] keyWithSalt) {
    	return decryptKey(keyWithSalt, this.iv);
    }
    
    
    private byte[] decryptKey(byte[] keyWithSalt, byte[] iv) {
        try {
        	SecretKeySpec secretKeySpec = new SecretKeySpec(keyToEncryptMasterKey, EncryptionService.ENCRYPTION_ALGORITHM);
			Cipher dec = Cipher.getInstance(EncryptionService.ENCRYPTION_ALGORITHM_METHOD);
			dec.init(Cipher.DECRYPT_MODE, secretKeySpec, new GCMParameterSpec(EncryptionService.IV_LENGTH_BIT, iv));
			return dec.doFinal( keyWithSalt);
			
			
        } catch (Exception e){
        	throw new InternalCriticalException(e, "decryptKey");
        }
    }
    
    /**
     * @param encKey
     */
    public synchronized void setKeyToEncryptMasterKey(byte [] encKey, byte [] iv) {
    		this.iv = iv;
			this.keyToEncryptMasterKey = encKey;
	}

    public ServerSettings getServerSettings() {
		return this.serverSettings;
	}
    
    /**
     *
     * 
     */
	@PostConstruct
	protected void onInitialize() {

		synchronized (this) {
				
				setStatus(ServiceStatus.STARTING);

				if (this.serverSettings.getEncryptionKey()!=null) {
					
					String s_key = getServerSettings().getEncryptionKey();
					String s_iv = getServerSettings().getEncryptionIV();
					
					byte a_encKey 	[] = ByteToString.hexStringToByte(s_key);
					byte a_iv 		[] = ByteToString.hexStringToByte(s_iv);
					
					setKeyToEncryptMasterKey(a_encKey, a_iv);
				}
				
				startuplogger.debug("Started -> " + this.getClass().getSimpleName());
				setStatus(ServiceStatus.RUNNING);

		}
	}

}
