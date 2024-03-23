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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Optional;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ServerSettings;


/**
 * 
 * 
 * <p>Master Key
 * 
 * Key -> Local, Vault
 * Data
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class ODEncryptionService extends BaseService implements EncryptionService  {
		
	   @SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(ODEncryptionService.class.getName());
	   static private Logger startuplogger = Logger.getLogger("StartupLogger");
	   
	    @JsonIgnore
	    @Autowired
		private final ServerSettings serverSettings;

	    @JsonIgnore
	    @Autowired
		private final SystemMonitorService monitoringService;
		
	    @JsonIgnore
	    @Autowired
		private final KeyEncriptorWrapper odilonKeyEncriptorWrapper;
 
	    @JsonProperty("encryptionAlgorithm")
		private String encryptionAlgorithm = ServerConstant.DEFAULT_ENCRYPT_ALGORITHM;
	    
	    @JsonProperty("keyAlgorithm")
	    private String keyAlgorithm = ServerConstant.DEFAULT_KEY_ALGORITHM; 

	    @JsonIgnore
	    private String dataEncryptionKey = "YU3t6v9y$B&E)H@M";
	    
	    /**
		 * 
		 * @param serverSettings
		 * @param montoringService
		 * @param odilonKeyEncriptorWrapper
		 */
	    public ODEncryptionService(	ServerSettings serverSettings, 
									SystemMonitorService montoringService,
									KeyEncriptorWrapper odilonKeyEncriptorWrapper) {
			
			this.serverSettings=serverSettings;
			this.monitoringService=montoringService;
			this.odilonKeyEncriptorWrapper=odilonKeyEncriptorWrapper;
		}

	    
	    public void setEncryptionKey(String key) {
	    	dataEncryptionKey = key;
	    }
	    
	    public String getEncryptionKey() {
	    	return dataEncryptionKey;
	    }
	    
	   /**
	    * 
	    */
	   @Override
	   public InputStream encryptStream(InputStream inputStream) {
	        try {
	            															
	        	StreamEncryptor streamEncryption = new JCipherStreamEncryptor(this.encryptionAlgorithm, this.keyAlgorithm, this.odilonKeyEncriptorWrapper);
	        	
	        	String key = streamEncryption.genNewKey();
	        													
	            EncryptedInputStream odilonEncryptedInputStream = streamEncryption.encrypt(inputStream, key);

	            String jsonStreamEncryptionInfo = getObjectMapper().writeValueAsString(odilonEncryptedInputStream.getStreamEncryptorInfo());
	            InputStream jsonStreamEncryptionInfoStream = new ByteArrayInputStream(jsonStreamEncryptionInfo.getBytes());

	            getSystemMonitorService().getEncrpytFileMeter().mark();
	            
	            return new SequenceInputStream(jsonStreamEncryptionInfoStream, odilonEncryptedInputStream);
	            
	        } catch (Exception e) {
	            throw new InternalCriticalException(e, "encryptStream");
	        }
	    }
	   
	   @Override
	   public InputStream decryptStream(InputStream inputStream) {
	        try {
	            
	        	JsonFactory f = new MappingJsonFactory();
	            f.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
	            JsonParser parser = f.createParser(inputStream);
	            String json = parser.readValueAsTree().toString();

	            StreamEncryptorInfo streamEncryptionInfo  = new ObjectMapper().readValue(json, StreamEncryptorInfo.class);
	            String key = streamEncryptionInfo.getEncryptedKey();
	            StreamEncryptor streamEncryption = streamEncryptionInfo.getStreamEncryption();

	            ByteArrayOutputStream remainderOutputStream = new ByteArrayOutputStream();
	            parser.releaseBuffered(remainderOutputStream);
	            ByteArrayInputStream  remainderInputStream = new ByteArrayInputStream(remainderOutputStream.toByteArray());

	            InputStream encryptedStream = new SequenceInputStream(remainderInputStream, inputStream);

	            getSystemMonitorService().getDecryptFileMeter().mark();
	            
	            return streamEncryption.decrypt(encryptedStream, key);
	            
	        } catch (IOException  e) {
	        	throw new InternalCriticalException(e, "decryptStream");
	        }
	    }
	   
	   /**
	    * 
	    * 
	    */
	   public SystemMonitorService getSystemMonitorService() {
			return  monitoringService;
		}

	   /**
	    * 
	    */
	   @PostConstruct
	   protected void onInitialize() {
			synchronized (this) {
				setStatus(ServiceStatus.STARTING);
				this.encryptionAlgorithm = Optional.ofNullable(this.serverSettings.getEncryptionAlgorithm()).orElseGet(() -> ServerConstant.DEFAULT_ENCRYPT_ALGORITHM);
				this.keyAlgorithm = Optional.ofNullable(this.serverSettings.getKeyAlgorithm()).orElseGet(() -> ServerConstant.DEFAULT_KEY_ALGORITHM);
				startuplogger.debug("Started -> " + EncryptionService.class.getSimpleName());
				setStatus(ServiceStatus.RUNNING);
			}
		}
}


