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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServiceStatus;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;

/**
 * <p>
 * Object Encryption Service 
 * </p>
 * <p>
 * The variable <b>encryption.key</b> in <b>odilon.properties</b> contain a AES
 * key used to decrypt the Server Master Key on server's startup.
 * </p>
 * 
 * <p>
 * The Master Key is unique for each server and can not be changed.
 * </p>
 * 
 * <p>
 * It used by the {@link EncryptionService} to encrypt/decrypt
 * the key used to encrypt/decrypt every Object. <br/>
 * Each Object has its own unique encryption key
 * </p>
 *
 * <p>
 * The procedure is:
 * </p>
 *
 * <b>SERVER STARTUP</b>
 * <ul>
 * <li>Odilon decrypts the MasterKEy using the key provided in variable
 * encryption.key in odilon.properties</li>
 * </ul>
 * <br/>
 * <b>PUT OBJECT</b>
 * <ul>
 * <li>Odilon generates a new key for every Object (objKey)</li>
 * <li>The Object is encrypted using AES with key objKey</li>
 * <li>The obkKey is encrypted by {@link OdilonKeyEncryptorService} using AES
 * with the server Master Key or by the KMS if enabled, and saved in disk as a
 * prefix of the Object</li>
 * </ul>
 * <br/>
 * <b>GET OBJECT</b>
 * <ul>
 * <li>Odilon reads the stored Object and extracts the objKey</li>
 * <li>objKey is decrypted using MasterKey or KMS if enabled</li>
 * <li>Object is decrypted using objKey</li>
 * </ul>
 * <br/>
 * <br/>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class OdilonEncryptionService extends BaseService implements EncryptionService {

	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	public final class EncryptionSizeCalculator {
		private static final int GCM_SIV_TAG_LENGTH = 16; // bytes

		private EncryptionSizeCalculator() {
		}

		public static long encryptedPayloadSizeGcmSiv(long plainSize) {
			return plainSize + GCM_SIV_TAG_LENGTH;
		}
	}

	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	@JsonIgnore
	@Autowired
	private final SystemMonitorService monitoringService;

	@JsonIgnore
	@Autowired
	private final KeyEncriptorWrapper odilonKeyEncriptorWrapper;

	/**
	 * @param serverSettings
	 * @param montoringService
	 * @param odilonKeyEncriptorWrapper
	 */
	public OdilonEncryptionService(ServerSettings serverSettings, SystemMonitorService montoringService, KeyEncriptorWrapper odilonKeyEncriptorWrapper) {

		this.serverSettings = serverSettings;
		this.monitoringService = montoringService;
		this.odilonKeyEncriptorWrapper = odilonKeyEncriptorWrapper;
	}

	@Override
	public EncryptedResult encryptStream(InputStream inputStream) {

		try {

			StreamEncryptor streamEnc = new JCipherStreamEncryptor(EncryptionService.ENCRYPTION_ALGORITHM_METHOD, EncryptionService.ENCRYPTION_ALGORITHM, this.odilonKeyEncriptorWrapper);

			String key = streamEnc.getNewKey();
			String iv = streamEnc.getIV();

			CountingInputStream countingInputStream = new CountingInputStream(inputStream);
			EncryptedInputStream encryptedStream = streamEnc.encrypt(countingInputStream, key, iv);

			String json = getObjectMapper().writeValueAsString(encryptedStream.getStreamEncryptorInfo());
			byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
			InputStream jsonStreamEncryptionInfoStream = new ByteArrayInputStream(jsonBytes);

			InputStream finalStream = new SequenceInputStream(jsonStreamEncryptionInfoStream, encryptedStream);
			getSystemMonitorService().getEncrpytFileMeter().mark();

			return new EncryptedResult(finalStream, countingInputStream);

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

			StreamEncryptorInfo streamEncryptionInfo = new ObjectMapper().readValue(json, StreamEncryptorInfo.class);

			String key = streamEncryptionInfo.getEncryptedKey();
			String iv = streamEncryptionInfo.getIV();

			StreamEncryptor streamEncryption = streamEncryptionInfo.getStreamEncryption();

			ByteArrayOutputStream remainderOutputStream = new ByteArrayOutputStream();
			parser.releaseBuffered(remainderOutputStream);
			ByteArrayInputStream remainderInputStream = new ByteArrayInputStream(remainderOutputStream.toByteArray());

			InputStream encryptedStream = new SequenceInputStream(remainderInputStream, inputStream);

			getSystemMonitorService().getDecryptFileMeter().mark();

			return streamEncryption.decrypt(encryptedStream, key, iv);

		} catch (IOException e) {
			throw new InternalCriticalException(e, "decryptStream");
		}
	}

	public SystemMonitorService getSystemMonitorService() {
		return this.monitoringService;
	}

	@PostConstruct
	protected void onInitialize() {
		synchronized (this) {
			setStatus(ServiceStatus.STARTING);
			startuplogger.debug("Started -> " + EncryptionService.class.getSimpleName());
			setStatus(ServiceStatus.RUNNING);
		}
	}

	/**
	 * Deterministic variant when you know plaintext length in advance.
	 */

	@Override
	public EncryptedResult encryptStream(InputStream inputStream, long plainSize) {
		if (plainSize < 0)
			throw new IllegalArgumentException("plainSize must be >= 0");

		try {
			StreamEncryptor streamEnc = new JCipherStreamEncryptor(EncryptionService.ENCRYPTION_ALGORITHM_METHOD, EncryptionService.ENCRYPTION_ALGORITHM, this.odilonKeyEncriptorWrapper);

			String key = streamEnc.getNewKey();
			String iv = streamEnc.getIV();

			InputStream encryptedStream = streamEnc.encrypt(inputStream, key, iv);

			String json = getObjectMapper().writeValueAsString(streamEnc.getStreamEncryptionInfo(key, iv));
			byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
			InputStream jsonStream = new ByteArrayInputStream(jsonBytes);

			InputStream finalStream = new SequenceInputStream(jsonStream, encryptedStream);

			// long encryptedPayloadSize =
			// EncryptionSizeCalculator.encryptedPayloadSizeGcmSiv(plainSize);
			// long totalEncryptedSize = jsonBytes.length + encryptedPayloadSize;

			getSystemMonitorService().getEncrpytFileMeter().mark();

			return new EncryptedResult(finalStream, plainSize);

		} catch (Exception e) {
			throw new InternalCriticalException(e, "encryptStream");
		}
	}

	/*
	 * @Override public InputStream encryptStream(InputStream inputStream) { try {
	 * 
	 * StreamEncryptor streamEnc = new
	 * JCipherStreamEncryptor(EncryptionService.ENCRYPTION_ALGORITHM_METHOD,
	 * EncryptionService.ENCRYPTION_ALGORITHM, this.odilonKeyEncriptorWrapper);
	 * 
	 * String key = streamEnc.getNewKey(); String iv = streamEnc.getIV();
	 * 
	 * EncryptedInputStream odilonEncryptedInputStream =
	 * streamEnc.encrypt(inputStream, key, iv);
	 * 
	 * String jsonStreamEncryptionInfo = getObjectMapper()
	 * .writeValueAsString(odilonEncryptedInputStream.getStreamEncryptorInfo());
	 * InputStream jsonStreamEncryptionInfoStream = new
	 * ByteArrayInputStream(jsonStreamEncryptionInfo.getBytes());
	 * 
	 * getSystemMonitorService().getEncrpytFileMeter().mark();
	 * 
	 * return new SequenceInputStream(jsonStreamEncryptionInfoStream,
	 * odilonEncryptedInputStream);
	 * 
	 * } catch (Exception e) { throw new InternalCriticalException(e,
	 * "encryptStream"); } }
	 * 
	 */

}
