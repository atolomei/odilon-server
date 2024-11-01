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

import java.util.Optional;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.service.ServerSettings;

/**
 * <p>Wrapper object that is Serialized with the binary object stored in disk</p>
 * 
 *   @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
public class KeyEncriptorWrapper implements KeyEncryptor, ApplicationContextAware {
			
	
	@JsonIgnore
	private static Logger logger = Logger.getLogger(KeyEncriptorWrapper.class.getName());
	
	@JsonIgnore
	private static ApplicationContext applicationContext;

	@JsonIgnore
	private static VaultKeyEncryptorService vaultKeyEncryptor;
	
	@JsonIgnore
	private static OdilonKeyEncryptorService odilonKeyEncryptor;

	@JsonIgnore 	
	private Optional<String> vaultUrl; 


	public KeyEncriptorWrapper () {
	}
 

	@Override
    public byte[] encryptKey(byte[] key, byte [] iv) {
    	if ((getApplicationContext().getBean(ServerSettings.class).isUseVaultNewFiles()) &&  (this.vaultUrl.isPresent())) {  
    			return (getVaultKeyEncryptor()).encryptKey(key, iv); 
    	}
    	else {
    			return (getOdilonKeyEncryptor()).encryptKey(key, iv);
    	}
    }

    @Override
    public byte[] decryptKey(byte[] key, byte []iv) { 
    	boolean useVault = (new String(key)).startsWith("vault:");
    	if (useVault)
    		return (getVaultKeyEncryptor()).decryptKey(key, iv);
    	else
    		return getOdilonKeyEncryptor().decryptKey(key, iv);
    }
    
    
	public VaultKeyEncryptorService getVaultKeyEncryptor() {
		return vaultKeyEncryptor;
	}

	public OdilonKeyEncryptorService getOdilonKeyEncryptor() {		
		return odilonKeyEncryptor;
	}
	
	@JsonIgnore
	private boolean vault() {
		return this.vaultUrl.isPresent();
	}
	
	@PostConstruct
	protected void onInitialize() {
		
	  		this.vaultUrl = applicationContext.getBean(ServerSettings.class).getVaultUrl();
	  		
			if (vaultKeyEncryptor==null)
				vaultKeyEncryptor=getApplicationContext().getBean(VaultKeyEncryptorService.class);
			
			if (odilonKeyEncryptor==null)
				odilonKeyEncryptor=getApplicationContext().getBean(OdilonKeyEncryptorService.class);
	}
	
	   
	public ApplicationContext getApplicationContext()  {
		return applicationContext;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext appContext) throws BeansException {
		if (applicationContext==null) 
			applicationContext=appContext;
	}

}
