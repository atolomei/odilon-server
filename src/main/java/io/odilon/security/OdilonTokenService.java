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

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.Optional;

import jakarta.annotation.PostConstruct;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.odilon.encryption.EncryptionService;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServiceStatus;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * This service is used to generate tokens for presigned urls. 
 * Presigned urls are valid until their expiration date, even if 
 * the server is restarted.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class OdilonTokenService extends BaseService implements TokenService, ApplicationContextAware {

	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	//static private String salt = randomString(20);

	@JsonIgnore
	private String s_salt;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	@JsonIgnore
	@Autowired
	private final EncryptionService encrpytionService;

	@JsonIgnore
	@Autowired
	private final SystemMonitorService monitoringService;

	@JsonIgnore
	@Autowired
	private final VirtualFileSystemService virtualFileSystemService;

	@JsonIgnore
	private ApplicationContext applicationContext;

	@JsonIgnore
	private IvParameterSpec ivspec;

	@JsonIgnore
	private SecretKeyFactory factory;

	@JsonIgnore
	private KeySpec spec;

	@JsonIgnore
	private SecretKeySpec secretKeySpec;

	@JsonIgnore
	private String secretKey;

	public OdilonTokenService(ServerSettings serverSettings, SystemMonitorService montoringService,
			EncryptionService encrpytionService, VirtualFileSystemService vfs) {

		this.serverSettings = serverSettings;
		this.monitoringService = montoringService;
		this.encrpytionService = encrpytionService;
		this.virtualFileSystemService = vfs;
	}

	/**
	 * 
	 */
	public String encrypt(AuthToken token) {

		try {

			Cipher encCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			encCipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivspec);
			return Base64.getEncoder().encodeToString(encCipher.doFinal(token.toJSON().getBytes("UTF-8")));

		} catch (IllegalBlockSizeException | BadPaddingException | UnsupportedEncodingException
				| NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
				| InvalidAlgorithmParameterException e) {
			throw new InternalCriticalException(e, "encrypt");
		}

	}

	/**
	 * 
	 */
	public AuthToken decrypt(String enc) {
		String str;
		try {

			Cipher decCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			decCipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivspec);

			str = new String(decCipher.doFinal(Base64.getDecoder().decode(enc)));

		} catch (IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException | NoSuchPaddingException
				| InvalidKeyException | InvalidAlgorithmParameterException e1) {
			throw new InternalCriticalException(e1, "decrypt");
		}

		try {

			return getObjectMapper().readValue(str, AuthToken.class);

		} catch (JsonProcessingException e) {
			throw new InternalCriticalException(e, "decrypt");
		}
	}

	/**
	 * 
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * 
	 */
	@PostConstruct
	protected void onInitialize() {

		synchronized (this) {
			try {
				setStatus(ServiceStatus.STARTING);
				
				this.s_salt = this.serverSettings.getPresignedSalt();
				
				String str = this.serverSettings.getSecretKey();

				if (str.length() < 8) {
					StringBuilder sb = new StringBuilder();
					for (int n = str.length(); n < 8; n++)
						sb.append("0");
					str = str + sb.toString();
				}
				
				this.secretKey = str.substring(0, 8);

				// Key has to be of length 8
				if (secretKey == null || secretKey.length() != 8)
					throw new RuntimeException("Invalid key length - 8 bytes key needed -> "
							+ Optional.ofNullable(secretKey).orElse("null"));

				byte[] iv = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
				this.ivspec = new IvParameterSpec(iv);
				this.factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
				
				//this.spec = new PBEKeySpec(this.secretKey.toCharArray(), salt.getBytes(), 65536, 256);
				this.spec = new PBEKeySpec(this.secretKey.toCharArray(), s_salt.getBytes(), 65536, 256);
				
				this.secretKeySpec = new SecretKeySpec(factory.generateSecret(this.spec).getEncoded(), "AES");

				startuplogger.debug("Started -> " + TokenService.class.getSimpleName());
				setStatus(ServiceStatus.RUNNING);
			}

			catch (InternalCriticalException e) {
				setStatus(ServiceStatus.STOPPED);
				throw e;
			}

			catch (Exception e) {
				setStatus(ServiceStatus.STOPPED);
				throw new InternalCriticalException(e, "onInitialize");
			}

		}
	}

}
