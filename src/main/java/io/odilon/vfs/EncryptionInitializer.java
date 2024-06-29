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
package io.odilon.vfs;

import java.io.File;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.context.ConfigurableApplicationContext;

import com.google.common.io.Files;

import io.odilon.encryption.EncryptionService;
import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ODModelObject;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.service.util.ByteToString;
import io.odilon.vfs.model.IODriver;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Initialize the encryption service</p>
 * <p>An instance of this class is created by the <b>initialize encryption</b> script:<br/>
 * ./bin/enable-encryption.sh (Linux) or .\bin\enable-encryption.bat (Windows) <br/>
 * <br/>  
 * and also by the <b>rekey</b> script: <br/>
 * ./rekey-encryption.sh -m masterkey (Linux) or rekey-encryption.bat -m masterkey (Windows).</p>
 *  
 * <p>Both scripts start up the application with one (initialize) or two (rekey) parameters: <br/>
 * <br/>
 * To initialze encryption:<br/>
 * -DinitializeEncryption=true<br/>
 * <br/>
 * The rekey script also adds this parameter (the master key must be provided to generate a new encryption key):<br/>
 * -DmasterKey=ab5d6e5d6f9523eba7208b5b3ec875ba<br/>
 * </p>
 * 
 * <p>See <a href="https://odilon.io/configuration-advanced.html">https://odilon.io/configuration-advanced.html</a>
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class EncryptionInitializer extends ODModelObject {
		
	static private Logger logger = Logger.getLogger(EncryptionInitializer.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	private Optional<String> providedMasterKey;
	private VirtualFileSystemService vfs;
	
	public EncryptionInitializer(VirtualFileSystemService vfs, Optional<String> providedMasterKey) {
		this.vfs=vfs;
		this.providedMasterKey=providedMasterKey;
	}

	public VirtualFileSystemService getVFS() {
		return this.vfs;
	}

	/**
	 * 
	 * 
	 */
	public void execute() {
		if (getProvidedMasterKey().isPresent())
			rekey();
		else
			initializeEnc();
	}

	private synchronized void initializeEnc() {
		
	 	startuplogger.info("Initializing Encryption Service");
	 	startuplogger.info("");
	 	
		IODriver driver = getVFS().createVFSIODriver();
		OdilonServerInfo info = driver.getServerInfo();
		
		if (info==null)
			info=getVFS().getServerSettings().getDefaultOdilonServerInfo();

		if (info.isEncryptionIntialized()) {

			startuplogger.info("Encryption Service has already been initialized on -> " + info.getEncryptionIntializedDate().toString());
			startuplogger.info("The server will shutdown now.");
			try {
				
				Thread.sleep(10000);
				
			} catch (InterruptedException e) {
			}
			((ConfigurableApplicationContext) this.getVFS().getApplicationContext()).close();
			System.exit(1);
		}
		
		SecureRandom secureRandom = new SecureRandom();
		 
		byte 	[] encKey 		= new byte[EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE]; // 16 bytes -> 2 ASCII chars per byte -> 32 ASCII chars
		byte 	[] iv			= new byte[EncryptionService.AES_IV_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];  // 12 bytes -> 2 ASCII chars per byte -> 24 ASCII chars
		byte 	[] masterKey	= new byte[EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
		byte	[] salt 		= new byte[EncryptionService.AES_KEY_SALT_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
		byte 	[] hmac;
		 
		secureRandom.nextBytes(encKey);
		secureRandom.nextBytes(iv);
		secureRandom.nextBytes(masterKey);
		secureRandom.nextBytes(salt);
		
		byte 	[] encKeyIV = new byte[ encKey.length + iv.length ];
		
		System.arraycopy(encKey, 0, encKeyIV, 			  0, encKey.length);
		System.arraycopy(iv,     0, encKeyIV, encKey.length, iv.length);
		
		
		try {
			
			// 
			// HMAC is   taken from -> enc key + IV (28 bytes)
			//
			hmac = getVFS().HMAC(encKeyIV, encKey);
			
		} catch (InvalidKeyException | NoSuchAlgorithmException e) {
			throw new InternalCriticalException(e);
		}

		getVFS().getMasterKeyEncryptorService().setKeyToEncryptMasterKey(encKey, iv);
				 
		driver.saveServerMasterKey(masterKey, hmac, iv, salt);
		
		info.setEncryptionIntialized(true);
		OffsetDateTime now = OffsetDateTime.now();
		
		info.setEncryptionIntializedDate(now);
		info.setEncryptionLastModifiedDate(now);
		
		driver.setServerInfo(info);
		
		startuplogger.info("ENCRYPTION KEY");
		startuplogger.info("--------------");

		
		// the encryption key encoded in ASCII 
		// is = AES KEY 32 ASCII characters + IV 24 ASCIII characteres = 56 ASCII chars
		
		startuplogger.info("encryption.key = " + ByteToString.byteToHexString(encKeyIV));
		startuplogger.info("The encrytion key must be added to the 'odilon.properties' file in variable 'encryption.key' as printed above.");
		startuplogger.info("");
		startuplogger.info("MASTER KEY");
		startuplogger.info("----------");										
		startuplogger.info("Master Key -> " + ByteToString.byteToHexString(masterKey));
		startuplogger.info("");
		startuplogger.info("The master key is used internally and it is secret, it is NOT required in 'odilon.properties' or anywhere else."); 
		startuplogger.info("However it may be required to restore the system in case some critical system files are");
		startuplogger.info("accidental or intentionally deleted in the future.");
		startuplogger.info("It is recommended that you store it securely.");
		startuplogger.info("");
		startuplogger.info("process completed.");
		
		
		
		
		
		/** try to copy kbee.enc -> /config  */

		try {
			File srcFile = driver.getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
			File destFile = new File(System.getProperty("user.dir") + File.separator + "config" + File.separator + srcFile.getName());
			Files.copy(srcFile, destFile);
			startuplogger.info("");
			startuplogger.info("Odilon made a backup of the encrypted key to -> " + System.getProperty("user.dir") + File.separator + "config" + File.separator + srcFile.getName());
			startuplogger.info("");
		} catch (Exception e) {
			logger.error(e, "Backup encrypted key to -> " + System.getProperty("user.dir") + File.separator + "config");
		}
		
		shutDown(0);
	}

	
	/**
	 * 
	 * 
	 */
	private synchronized void rekey() {
		
		startuplogger.info("NEW ENCRYPTION KEY");
		startuplogger.info("------------------");
		
		IODriver driver = getVFS().createVFSIODriver();
		OdilonServerInfo info = driver.getServerInfo();
		
		if (info==null)
			info=getVFS().getServerSettings().getDefaultOdilonServerInfo();

		if (!info.isEncryptionIntialized()) {
			rekeyNotIntializedError();
			return;
		}
		
		// ---
		
		// check if the provided master key is correct
		boolean isCorrectMasterKey = false;
		
		byte[] key = driver.getServerMasterKey();
		
		if (key==null) {
			startuplogger.info("Server's master key is null");
			shutDown(1);
		}
			
		String validk = ByteToString.byteToHexString(key);
		String provk  = providedMasterKey.get();
		
		isCorrectMasterKey=validk.equals(provk);
		
		if (!isCorrectMasterKey) {
			rekeyMasterKeyNotCorrectError();
			return;
		}
				
		SecureRandom secureRandom = new SecureRandom();
		 
		byte 	[] encKey 		= new byte[ EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE]; // 16 bytes -> 2 ASCII chars per byte -> 32 ASCII chars
		byte 	[] iv			= new byte[ EncryptionService.AES_IV_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];  // 12 bytes -> 2 ASCII chars per byte -> 24 ASCII chars

		byte	[] salt 		= new byte[ EncryptionService.AES_KEY_SALT_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
		byte 	[] hmac;

		secureRandom.nextBytes(encKey);
		secureRandom.nextBytes(iv);
		
		secureRandom.nextBytes(salt);
		
		byte 	[] encKeyIV = new byte[ encKey.length + iv.length ];
		
		System.arraycopy(encKey, 0, encKeyIV, 			  0, encKey.length);
		System.arraycopy(iv,     0, encKeyIV, encKey.length, iv.length);
		
		
		try {
			// 
			// HMAC is   taken from -> enc key + IV (28 bytes)
			//
			hmac = getVFS().HMAC(encKeyIV, encKey);
			
		} catch (InvalidKeyException | NoSuchAlgorithmException e) {
			throw new InternalCriticalException(e);
		}

		getVFS().getMasterKeyEncryptorService().setKeyToEncryptMasterKey(encKey, iv);
				 
		driver.saveServerMasterKey(key, hmac, iv, salt);
		
		info.setEncryptionIntialized(true);
		info.setEncryptionLastModifiedDate(OffsetDateTime.now());
		
		driver.setServerInfo(info);
		
		startuplogger.info("encryption.key = " + ByteToString.byteToHexString(encKeyIV));
		startuplogger.info("The encrytion key must replice the previous value of encryption.key in 'odilon.properties'.");
		startuplogger.info("");
		startuplogger.info("");
		startuplogger.info("process completed.");
		
		
		/** try to copy kbee.enc -> /config  */
		
		try {
			File srcFile = driver.getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
			File destFile = new File(System.getProperty("user.dir") + File.separator + "config" + File.separator + srcFile.getName());
			Files.copy(srcFile, destFile);
			startuplogger.info("");
			startuplogger.info("Odilon made a backup of the encrypted key to -> " + System.getProperty("user.dir") + File.separator + "config" + File.separator + srcFile.getName());
			startuplogger.info("");
		} catch (Exception e) {
			logger.error(e, "Backup encrypted key to -> " + System.getProperty("user.dir") + File.separator + "config");
		}
	
		shutDown(0);
	}
	

	/**
	 * 
	 * 
	 * 
	 * 
	 */
	private void rekeyMasterKeyNotCorrectError() {
		startuplogger.info("");
		startuplogger.info("The Master key provided -> " + this.providedMasterKey.get());
		startuplogger.info("is incorrect");
		startuplogger.info("The server will shutdown now.");
		startuplogger.info(ServerConstant.SEPARATOR);
		try {
			Thread.sleep(2000);
			
		} catch (InterruptedException e) {
		}
		((ConfigurableApplicationContext) getVFS().getApplicationContext()).close();
		System.exit(1);
	}
	
	/**
	 * 
	 * 
	 * 
	 */
	private void rekeyNotIntializedError() {
		startuplogger.info("The Encryption Service has not been initialized.");
		startuplogger.info("You have to initialize the Encryption Service by executing '"+getEnableEncryptionScriptName()+"', Odilon will generate the encryption keys");
		startuplogger.info("");
		startuplogger.info("The server will shutdown now.");
		
		startuplogger.info(ServerConstant.SEPARATOR);
		try {
			Thread.sleep(3000);
			
		} catch (InterruptedException e) {
		}
		((ConfigurableApplicationContext) getVFS().getApplicationContext()).close();
		System.exit(1);
		
	}
	
	/**
	 * 
	 * 
	 * 
	 */
	public void notInitializedError() {
		startuplogger.info("");
		startuplogger.info("The server is configured to use encryption (ie. 'encryption.enabled=true' in file 'odilon.properties')");
		startuplogger.info("but the encryption service has not been initialized yet.");
		startuplogger.info("");
		startuplogger.info("You have to either:"); 
		startuplogger.info("a. Disable encryption in 'odilon.properties' by changing the variable to 'encryption.enabled=false' or");
		startuplogger.info("");
		startuplogger.info("b. Initialize the encryption service by executing '"+getEnableEncryptionScriptName()+"'");
		startuplogger.info("");
		startuplogger.info("If you execute '"+getEnableEncryptionScriptName()+"' Odilon will generate the encryption keys");
		startuplogger.info("The server will shutdown now.");
		startuplogger.info("");
		startuplogger.info(ServerConstant.SEPARATOR);
	try {
					Thread.sleep(6000);
			
		} catch (InterruptedException e) {
		}
		((ConfigurableApplicationContext) getVFS().getApplicationContext()).close();
		System.exit(1);
	}
	

	private String getEnableEncryptionScriptName() {
		return isLinux() ? (ServerConstant.ENABLE_ENCRYPTION_SCRIPT_LINUX) : (ServerConstant.ENABLE_ENCRYPTION_SCRIPT_WINDOWS);
	}
	
	/**
	 * @return
	 */
	private boolean isLinux() {
		if  (System.getenv("OS")!=null && System.getenv("OS").toLowerCase().contains("windows")) 
			return false;
		return true;
	}
	
	private Optional<String> getProvidedMasterKey() {
		return this.providedMasterKey;
	}
	
	

	private void shutDown(int code) {
		startuplogger.info("The server will shutdown now.");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
		}
		((ConfigurableApplicationContext) getVFS().getApplicationContext()).close();
		System.exit(code);
	}

}
