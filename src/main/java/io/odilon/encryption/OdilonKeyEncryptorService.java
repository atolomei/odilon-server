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

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.ServerSettings;

import javax.annotation.PostConstruct;
import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;



/**
 * 
 * <p>Encrypts key for each file using the Master Key</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class OdilonKeyEncryptorService extends BaseService implements KeyEncryptor {
			
	private static final Logger logger = Logger.getLogger(OdilonKeyEncryptorService.class.getName());

	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	static final String keyEncryptionAlgorithm = ServerConstant.DEFAULT_ENCRYPT_ALGORITHM;
	static final String keyAlgorithm = ServerConstant.DEFAULT_KEY_ALGORITHM;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
   @JsonIgnore
   private byte[] masterKey;  
   
   @JsonIgnore
   private Cipher enc;
   
   @JsonIgnore
   private Cipher dec;
   
    public OdilonKeyEncryptorService(ServerSettings serverSettings) {
    	this.serverSettings=serverSettings;
    }
    
    
    public void setMasterKey(byte[] key) {
    	
    	synchronized (this) {
				
			try {
				
	    		this.masterKey = key;
				
				SecretKeySpec secretKeySpec = new SecretKeySpec(masterKey, keyAlgorithm);
				
				this.enc = Cipher.getInstance(keyEncryptionAlgorithm);
				this.enc.init(Cipher.ENCRYPT_MODE, secretKeySpec);
			
				this.dec = Cipher.getInstance(keyEncryptionAlgorithm);
				this.dec.init(Cipher.DECRYPT_MODE, secretKeySpec);

				startuplogger.debug("Started -> " + this.getClass().getSimpleName());
				setStatus(ServiceStatus.RUNNING);
			
			} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
				logger.error(e);
				setStatus(ServiceStatus.STOPPED);
				throw new InternalCriticalException(e);
			}
    	}
    }
    
    /**
     * 
     */
    @Override
    public byte[] encryptKey(byte[] key) {
        try {
            return processBytes(key, Cipher.ENCRYPT_MODE, masterKey);
        } catch (Exception e){
        	logger.error(e);
            throw new InternalCriticalException(e);
        }
    }

    /**
     * 
     */
    @Override
    public byte[] decryptKey(byte[] key) {
        try {
            return processBytes(key,Cipher.DECRYPT_MODE, masterKey);
        } catch (Exception e){
        	logger.error(e);
        	throw new InternalCriticalException(e);
        }
    }
    
    /**
     * 
     */
	@PostConstruct
	protected void onInitialize() {

		synchronized (this) {
				
				try {

					setStatus(ServiceStatus.STARTING);
					
					//this.encryptorKey = this.serverSettings.getEncryptorKey().getBytes();
					
					/**
					SecretKeySpec secretKeySpec = new SecretKeySpec(encryptorKey, keyAlgorithm);
					
					this.enc = Cipher.getInstance(keyEncryptionAlgorithm);
					this.enc.init(Cipher.ENCRYPT_MODE, secretKeySpec);
				
					this.dec = Cipher.getInstance(keyEncryptionAlgorithm);
					this.dec.init(Cipher.DECRYPT_MODE, secretKeySpec);
					**/

				
				} catch (Exception e) {
					startuplogger.error(e);
					logger.error(e);
					setStatus(ServiceStatus.STOPPED);
					throw new InternalCriticalException(e);
				}
		}
	}
		   
	/**
	 * 
	 * @param bytes
	 * @param encryptMode
	 * @param key
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
    private byte[] processBytes(byte[] bytes, int encryptMode, byte[] key) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        if (encryptMode==Cipher.ENCRYPT_MODE)
        	return this.enc.doFinal(bytes);
        else
        	return this.dec.doFinal(bytes);
    }
}


