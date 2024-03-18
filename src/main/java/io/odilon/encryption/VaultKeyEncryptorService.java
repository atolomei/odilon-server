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

import java.nio.charset.StandardCharsets;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Base64Utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServiceStatus;
import io.odilon.security.VaultService;
import io.odilon.service.ServerSettings;


/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class VaultKeyEncryptorService extends BaseService implements KeyEncryptor {

   static private Logger startuplogger = Logger.getLogger("StartupLogger");

    @JsonProperty("keyId")
	private String keyID;
    
	@Autowired
	@JsonIgnore
    private VaultService vaultService;
	
	@Autowired
	@JsonIgnore
	ServerSettings serverSettings;
	
    public VaultKeyEncryptorService(VaultService vaultService, ServerSettings serverSettings ) {
    	this.vaultService=vaultService;
    	this.serverSettings=serverSettings;
    }
    /**
	 * 
	 */
	@PostConstruct
	protected void onInitialize() {
			synchronized (this) {
				setStatus(ServiceStatus.STARTING);
				this.keyID = "transit/" + serverSettings.getVaultKeyId();
				startuplogger.debug("Started -> " + VaultKeyEncryptorService.class.getSimpleName());
				setStatus(ServiceStatus.RUNNING);
			}
	}
    
    @Override
    public byte[] encryptKey(byte[] key) {
        String keyStr = Base64Utils.encodeToString(key);
        return getVaultService().encrypt(this.getKeyID(), keyStr).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] decryptKey(byte[] key) {
        String keyStr = new String(key, StandardCharsets.UTF_8);
        return Base64Utils.decodeFromString(getVaultService().decrypt(this.getKeyID(), keyStr));
    }

    public String getKeyID() {
        return keyID;
    }

    public void setKeyID(String keyID) {
        this.keyID = keyID;
    }

    public VaultService getVaultService() {
        return vaultService;
    }

}
