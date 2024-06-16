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
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import jakarta.annotation.PostConstruct;
import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * <p>Encrypts key for each file using the Master Key</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class OdilonKeyEncryptorService extends BaseService implements KeyEncryptor {
			
	private static final Logger logger = Logger.getLogger(OdilonKeyEncryptorService.class.getName());

	static private Logger startuplogger = Logger.getLogger("StartupLogger");


	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
   @JsonIgnore
   private byte[] masterKey = null;  
   

   
    public OdilonKeyEncryptorService(ServerSettings serverSettings) {
    	this.serverSettings=serverSettings;
    }
    

        public void setMasterKey(byte[] key) {
    	
    	synchronized (this) {
				
	    		this.masterKey = key;
	    		startuplogger.debug("Started -> " + this.getClass().getSimpleName());
				setStatus(ServiceStatus.RUNNING);
    	}
    }


    @Override
    public byte[] encryptKey(byte[] key, byte[] iv) {
        try {
        	SecretKeySpec secretKeySpec = new SecretKeySpec(masterKey, EncryptionService.ENCRYPTION_ALGORITHM);
			Cipher enc = Cipher.getInstance(EncryptionService.ENCRYPTION_ALGORITHM_METHOD);
			enc.init(Cipher.ENCRYPT_MODE, secretKeySpec, new GCMParameterSpec(EncryptionService.IV_LENGTH_BIT, iv));
			return enc.doFinal(key);
        } catch (Exception e){
        	throw new InternalCriticalException(e, "encryptKey");
        }
    }


    @Override
    public byte[] decryptKey(byte[] key, byte[] iv) {
        try {
        	
        	SecretKeySpec secretKeySpec = new SecretKeySpec(masterKey, EncryptionService.ENCRYPTION_ALGORITHM);
			Cipher dec = Cipher.getInstance(EncryptionService.ENCRYPTION_ALGORITHM_METHOD);
			dec.init(Cipher.DECRYPT_MODE, secretKeySpec, new GCMParameterSpec(EncryptionService.IV_LENGTH_BIT, iv));
			byte [] decKey = dec.doFinal(key);
        	return decKey;
        	
        } catch (Exception e){
        	throw new InternalCriticalException(e, "decryptKey");
        }
    }
    
    
	@PostConstruct
	protected void onInitialize() {
		synchronized (this) {
				try {
					setStatus(ServiceStatus.STARTING);
				} catch (Exception e) {
					startuplogger.error(e.getClass().getName() + " | " + e.getMessage());
					logger.error(e.getClass().getName() + " | " + e.getMessage());
					setStatus(ServiceStatus.STOPPED);
					throw new InternalCriticalException(e, "onInitialize");
				}
		}
	}

}


