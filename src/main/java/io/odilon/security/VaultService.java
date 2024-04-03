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
package io.odilon.security;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.vault.authentication.AppRoleAuthentication;
import org.springframework.vault.authentication.AppRoleAuthenticationOptions;
import org.springframework.vault.authentication.AppRoleAuthenticationOptions.RoleId;
import org.springframework.vault.authentication.AppRoleAuthenticationOptions.SecretId;
import org.springframework.vault.client.VaultClients;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.web.client.RestOperations;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;
import io.odilon.util.Check;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class VaultService extends BaseService implements SystemService {
			
	private static final Logger logger = Logger.getLogger(VaultService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	@Autowired
	private ServerSettings serverSettings;

	@JsonIgnore
    @Autowired
	private SystemMonitorService monitoringService;
	
	@JsonIgnore
	private VaultTemplate vaultTemplate=null;

	public VaultService (ServerSettings serverSettings, SystemMonitorService montoringService) {
    		this.serverSettings=serverSettings;
			this.monitoringService=montoringService;
	}
    
    public String encrypt(String keyID, String key) {
    	return encrypt(keyID, key, true);
    }

    public String encrypt(String keyID, String key, boolean countMetric) {

	Check.requireNonNullStringArgument(keyID, "Vault keyID is null");
	Check.requireNonNullStringArgument(key, "encrypt key is null");
	
    if (keyID.startsWith("/"))
          keyID = keyID.substring(keyID.indexOf("/") + 1);

        String[] keySplit = keyID.split("/", 2);
        Check.checkTrue(keySplit.length==2, "Invalid Vault keyID. It must have 2 parts separated by a '/' | received keyID -> " + keyID );

        String path = keySplit[0];
        String keyName = keySplit[1];
        String result = getVaultTemplate().opsForTransit(path).encrypt(keyName, key);
        
        if (countMetric) {
	        try {
	        	getSystemMonitorService().getMeterVaultEncrypt().mark();
	        } catch (Exception e) {
	        	logger.error(e, ServerConstant.NOT_THROWN);
	        }
        }
         return result;
    }
    
public String decrypt(String keyID, String key) {
	if (keyID.startsWith("/"))
        keyID = keyID.substring(keyID.indexOf("/") + 1);
    String[] keySplit = keyID.split("/", 2);
    String path = keySplit[0];
    String keyName = keySplit[1];
    
    String result;
    
    try {
        result = getVaultTemplate().opsForTransit(path).decrypt(keyName, key);
        getSystemMonitorService().getMeterVaultDecrypt().mark();
    } 
    catch (Exception e) {
    	vaultTemplate = null;
    	throw e;
    }
    
    return result;
}

public Optional<String> getUrl() {
   return serverSettings.getVaultUrl();
}


/**

<p>El uso normal del vault es como una hash table. secreto->valor
cada secreto tiene un path done el primer termino del path se corresponde con un repositorio
el transit es un repositorio especial que funciona para servicios
en este caso el servicio pedido es de encriptacion
donde especificas una clave que se configura en el proceso documentado de setup el vault
entonces (transit/clave, string) retorna el string encriptado
de hecho lo unico que hay en el repositorio del vault es esta clave kbee-kee
de la que el vault podria manejar  rotaciones
</p>

odilon-key
transit/odilon-key

es el nombre asignado en vault a esa clave
en el proceso de setup del vault

*/

public String getRoleId() {
	return serverSettings.getRoleId();
}

/**
 * 
 

el vault tiene varios metodos de autenticación
uno de ellos esta pensado para aplicaciones
este metodo de autenticacion para aplicaciones tiene dos parametros
que son esos uno es el rol que tiene la aplicacion 
rol que tiene asociados una serie de permisos 
permisos que habilitan al kbee a consultar el backend transit
y el secretid es una credencial para el kbee
esos dos tokens se configuran en el proceso de setup

*/

public String getSecretId() {
	return serverSettings.getSecretId();
}




public String ping() {

	String getVaultKeyId = null;
	try {
		
		getVaultKeyId="transit/"+serverSettings.getVaultKeyId();

		@SuppressWarnings("unused")
		String e=encrypt(getVaultKeyId, "odilon", false);
		return "ok";
		
	} catch (Exception e) {
		logger.error("Ping Vault  | Vault Key Id: " + Optional.ofNullable(getVaultKeyId).orElse("null"));
		logger.error(e);
		return 	e.getClass().getName() +  
				(Optional.ofNullable(e.getMessage()).isPresent() ? (" | " + e.getMessage())	:"")  + 
				" | Ping Vault  | Vault Key Id: " + (Optional.ofNullable(getVaultKeyId).orElse("null"));
	}
}

public SystemMonitorService getSystemMonitorService() {
	return  monitoringService;
}

@PostConstruct
protected void onInitialize() {		
	
	synchronized (this) {
		setStatus(ServiceStatus.STARTING);
		startuplogger.debug("Started -> " + VaultService.class.getSimpleName());
		setStatus(ServiceStatus.RUNNING);
	}
}

private VaultTemplate getVaultTemplate() {

	if (this.vaultTemplate == null) {
        try {
        	if (!getUrl().isPresent())
        		throw new InternalCriticalException("vaultUrl is null");
        		
        	String roleId =  getRoleId();
        	String secretId =  getSecretId();

        	VaultEndpoint endpoint = VaultEndpoint.from(new URI( getUrl().get() ));
            RestOperations restOperations = VaultClients.createRestTemplate(endpoint, new SimpleClientHttpRequestFactory());
            AppRoleAuthenticationOptions appRoleAuthenticationOptions = AppRoleAuthenticationOptions.builder()
                    .path(AppRoleAuthenticationOptions.DEFAULT_APPROLE_AUTHENTICATION_PATH)
                    .roleId(RoleId.provided(roleId))
                    .secretId(SecretId.provided(secretId))
                    .build();
            AppRoleAuthentication app =  new AppRoleAuthentication(appRoleAuthenticationOptions, restOperations);
            this.vaultTemplate = new VaultTemplate(endpoint, app);
        } 
        catch (URISyntaxException e) {
        	throw new InternalCriticalException(e, VaultTemplate.class.getName() + " cannot be initialized");
        	
        }
    }
    return this.vaultTemplate;
}


	
}
